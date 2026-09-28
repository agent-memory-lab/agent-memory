"""Immutable compression identities, offline reports and host-approved rollout."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path

import sqlite3

from .recovery import _text
from .recovery_store import RecoveryConflict


@dataclass(frozen=True, slots=True)
class CompressionStrategy:
    strategy_id: str
    version: str
    compressor_id: str
    validator_id: str
    counter_id: str

    def __post_init__(self):
        for value in asdict(self).values():
            _text(value, limit=256)


@dataclass(frozen=True, slots=True)
class StrategyApproval:
    approval_id: str
    actor: str
    reason: str

    def __post_init__(self):
        _text(self.approval_id, limit=256)
        _text(self.actor, limit=256)
        _text(self.reason, limit=2048)


@dataclass(frozen=True, slots=True)
class StrategyBinding:
    strategy: CompressionStrategy
    compressor: object
    validator: object
    counter: object

    def validate(self):
        from .context_compression import ExtractiveContextCompressor
        validator_id = ("builtin-extractive-v1" if self.validator is None and
                        type(self.compressor) is ExtractiveContextCompressor else
                        getattr(self.validator, "validator_id", None))
        actual = (getattr(self.compressor, "compressor_id", None), validator_id,
                  getattr(self.counter, "counter_id", None))
        expected = (self.strategy.compressor_id, self.strategy.validator_id, self.strategy.counter_id)
        if actual != expected:
            raise ValueError("runtime binding does not match the pinned strategy identities")


class CompressionStrategyRegistry:
    """Exact-scope registry; host owns bindings and async authorizer.

    authorizer.authorize(scope, action, strategy, expected_generation, approval)
    must return True. Approval text alone cannot grant activation rights.
    This registry never trains a model, changes a prompt, or invents thresholds.
    """
    def __init__(self, path, scope, *, bindings, authorizer, max_records=1024):
        if str(path) == ":memory:":
            raise ValueError("strategy registry must survive restart")
        if type(max_records) is not int or not 1 <= max_records <= 100000:
            raise ValueError("invalid registry capacity")
        if not callable(getattr(authorizer, "authorize", None)):
            raise TypeError("strategy registry requires a host authorizer")
        self.path, self.scope = Path(path), scope
        self.bindings = {}
        for binding in bindings:
            binding.validate()
            key = (binding.strategy.strategy_id, binding.strategy.version)
            if key in self.bindings or len(self.bindings) >= max_records:
                raise ValueError("duplicate or excessive strategy bindings")
            self.bindings[key] = binding
        self.authorizer, self.max_records = authorizer, max_records

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    async def initialize(self):
        def create():
            with self._db() as db:
                db.execute("""CREATE TABLE IF NOT EXISTS compression_registry_v1 (
                    scope TEXT, kind TEXT, identity TEXT, payload TEXT NOT NULL,
                    PRIMARY KEY(scope,kind,identity))""")
        await asyncio.to_thread(create)
        for binding in self.bindings.values():
            await self._immutable("strategy", self._identity(binding.strategy), asdict(binding.strategy))

    @staticmethod
    def _identity(strategy):
        return json.dumps([strategy.strategy_id, strategy.version], separators=(",", ":"))

    async def _immutable(self, kind, identity, payload):
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > 32768:
            raise ValueError("registry payload exceeds budget")
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                key = (self.scope.partition_key(), kind, identity)
                old = db.execute("SELECT payload FROM compression_registry_v1 "
                                 "WHERE scope=? AND kind=? AND identity=?", key).fetchone()
                if old:
                    if old[0] != encoded:
                        raise RecoveryConflict("immutable strategy/report identity changed")
                    return
                if db.execute("SELECT count(*) FROM compression_registry_v1").fetchone()[0] >= self.max_records:
                    raise ValueError("strategy registry capacity exceeded")
                db.execute("INSERT INTO compression_registry_v1 VALUES (?,?,?,?)", (*key, encoded))
        await asyncio.to_thread(write)

    async def _read(self, kind, identity):
        def read():
            with self._db() as db:
                row = db.execute("SELECT payload FROM compression_registry_v1 "
                    "WHERE scope=? AND kind=? AND identity=?",
                    (self.scope.partition_key(), kind, identity)).fetchone()
                return json.loads(row[0]) if row else None
        return await asyncio.to_thread(read)

    async def resolve(self):
        snapshot = await self._read("active", "current")
        if snapshot is None:
            raise ValueError("no compression strategy has host approval")
        binding = self.bindings.get((snapshot["strategy_id"], snapshot["version"]))
        if binding is None:
            raise ValueError("approved strategy has no runtime binding")
        binding.validate()
        return snapshot, binding

    async def is_current(self, snapshot):
        return await self._read("active", "current") == snapshot

    async def switch(self, strategy_id, version, *, expected_generation, approval, rollback=False):
        if type(expected_generation) is not int or expected_generation < 0 or type(rollback) is not bool:
            raise ValueError("invalid strategy generation/action")
        if not isinstance(approval, StrategyApproval):
            raise TypeError("host approval metadata is required")
        binding = self.bindings.get((strategy_id, version))
        if binding is None:
            raise ValueError("strategy binding is not registered")
        binding.validate()
        action = "rollback" if rollback else "activate"
        if await self.authorizer.authorize(self.scope, action, binding.strategy,
                                           expected_generation, approval) is not True:
            raise PermissionError("host did not approve the strategy switch")
        snapshot = dict(strategy_id=strategy_id, version=version, generation=expected_generation + 1)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                scope = self.scope.partition_key()
                old = db.execute("SELECT payload FROM compression_registry_v1 "
                    "WHERE scope=? AND kind='active' AND identity='current'", (scope,)).fetchone()
                previous = json.loads(old[0]) if old else None
                if (previous["generation"] if previous else 0) != expected_generation:
                    raise RecoveryConflict("active strategy generation changed")
                if rollback and not db.execute("""SELECT 1 FROM compression_registry_v1
                    WHERE scope=? AND kind='approval'
                    AND json_extract(payload,'$.target.strategy_id')=?
                    AND json_extract(payload,'$.target.version')=? LIMIT 1""",
                    (scope, strategy_id, version)).fetchone():
                    raise ValueError("rollback target has never been activated")
                needed = 1 if old else 2
                if db.execute("SELECT count(*) FROM compression_registry_v1").fetchone()[0] + needed > self.max_records:
                    raise ValueError("strategy registry capacity exceeded")
                audit = dict(action=action, approval=asdict(approval), previous=previous, target=snapshot)
                db.execute("INSERT INTO compression_registry_v1 VALUES (?,'approval',?,?)",
                           (scope, approval.approval_id, json.dumps(audit, sort_keys=True)))
                db.execute("""INSERT INTO compression_registry_v1 VALUES (?,'active','current',?)
                    ON CONFLICT(scope,kind,identity) DO UPDATE SET payload=excluded.payload""",
                    (scope, json.dumps(snapshot, sort_keys=True)))
        await asyncio.to_thread(write)
        return snapshot

    async def record_evaluation(self, report_id, strategy_id, version, *, dataset_digest,
                                model_id, evaluator_id, sample_count, metrics):
        """Host reports must use the same held-out dataset/model/evaluator to compare."""
        for value in (report_id, dataset_digest, model_id, evaluator_id):
            _text(value, limit=256)
        if (strategy_id, version) not in self.bindings:
            raise ValueError("unknown evaluated strategy")
        if type(sample_count) is not int or not 1 <= sample_count <= 100000000:
            raise ValueError("invalid evaluation sample count")
        if not isinstance(metrics, dict) or not 1 <= len(metrics) <= 32:
            raise ValueError("provide one to 32 host-defined metrics")
        for key, value in metrics.items():
            _text(key, limit=128)
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("metrics must be finite numbers")
        await self._immutable("evaluation", report_id, dict(strategy_id=strategy_id, version=version,
            dataset_digest=dataset_digest, model_id=model_id, evaluator_id=evaluator_id,
            sample_count=sample_count, metrics=metrics, source="host_reported"))

    async def compare(self, baseline_id, candidate_id):
        baseline = await self._read("evaluation", baseline_id)
        candidate = await self._read("evaluation", candidate_id)
        if baseline is None or candidate is None:
            raise ValueError("evaluation report is unavailable")
        for key in ("dataset_digest", "model_id", "evaluator_id", "sample_count"):
            if baseline[key] != candidate[key]:
                raise ValueError("evaluation protocols differ")
        if baseline["metrics"].keys() != candidate["metrics"].keys():
            raise ValueError("evaluation metric definitions differ")
        return dict(baseline=baseline, candidate=candidate,
            candidate_minus_baseline={key: candidate["metrics"][key] - value
                for key, value in baseline["metrics"].items()}, automatic_promotion=False)
