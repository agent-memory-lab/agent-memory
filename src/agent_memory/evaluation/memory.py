"""Raw-input, host-scored comparison runner with isolated backend factories.

No provider credentials or gold annotations are sent to ingestion adapters.
This runner does not claim that a protocol double is a real backend experiment.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from time import perf_counter
from typing import Protocol
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class EvaluationTurn:
    event_id: str
    role: str
    text: str
    occurred_at: str

    def __post_init__(self):
        from datetime import datetime
        if self.role not in ("user", "assistant", "tool"):
            raise ValueError("unsupported turn role")
        if not isinstance(self.event_id, str) or not 1 <= len(self.event_id) <= 256:
            raise ValueError("invalid turn identity")
        if not isinstance(self.text, str) or len(self.text.encode()) > 16384:
            raise ValueError("turn text exceeds budget")
        if datetime.fromisoformat(self.occurred_at).utcoffset() is None:
            raise ValueError("turn timestamp requires timezone")


@dataclass(frozen=True, slots=True)
class EvaluationQuery:
    text: str
    expected_fragments: tuple[str, ...]

    def __post_init__(self):
        if not isinstance(self.text, str) or not 1 <= len(self.text) <= 4096:
            raise ValueError("query exceeds budget")
        if not isinstance(self.expected_fragments, tuple) or not 1 <= len(self.expected_fragments) <= 32:
            raise ValueError("provide one to 32 evaluation fragments")
        if any(not isinstance(s, str) or not 1 <= len(s) <= 1024 for s in self.expected_fragments):
            raise ValueError("invalid evaluation fragment")


@dataclass(frozen=True, slots=True)
class EvaluationCase:
    case_id: str
    initial: tuple[EvaluationTurn, ...]
    updates: tuple[EvaluationTurn, ...]
    initial_queries: tuple[EvaluationQuery, ...]
    update_queries: tuple[EvaluationQuery, ...]

    def __post_init__(self):
        if not isinstance(self.case_id, str) or not 1 <= len(self.case_id) <= 128:
            raise ValueError("invalid case identity")
        for turns in (self.initial, self.updates):
            if not isinstance(turns, tuple) or len(turns) > 128 or any(not isinstance(t, EvaluationTurn) for t in turns):
                raise ValueError("invalid or excessive turns")
        for queries in (self.initial_queries, self.update_queries):
            if not isinstance(queries, tuple) or not 1 <= len(queries) <= 32 or any(not isinstance(q, EvaluationQuery) for q in queries):
                raise ValueError("invalid or excessive queries")
        turns = (*self.initial, *self.updates)
        if not self.initial or len({t.event_id for t in turns}) != len(turns):
            raise ValueError("cases require initial input and unique event IDs")


class EvaluationBackend(Protocol):
    """Factory returns a fresh isolated instance; close releases its resources.

    add consumes raw text only. search returns text, not labels or judge output.
    Implementations own real provider version/model settings and may record
    billed usage externally; absent accounting is reported as unavailable.
    """
    async def add(self, turn: EvaluationTurn) -> None: ...
    async def search(self, text: str, *, limit: int) -> tuple[str, ...]: ...
    async def delete_all(self) -> None: ...
    async def close(self) -> None: ...


def _answers(value):
    if not isinstance(value, (tuple, list)) or len(value) > 8:
        raise ValueError("backend search must return at most eight text results")
    if any(not isinstance(s, str) or len(s.encode()) > 16384 for s in value):
        raise ValueError("backend result exceeds byte budget")
    return tuple(value)


async def compare_memory_backends(cases, factories, *, timeout_seconds=30):
    """Factories are async callables taking only an opaque isolated run ID.

    Returned records omit source/results text. Fragment recall is not semantic
    answer quality. Timed-out backend operations may outlive their coroutine;
    never reuse the generated isolation ID, even if cleanup was attempted.
    """
    if not isinstance(cases, tuple) or not 1 <= len(cases) <= 100 or any(not isinstance(c, EvaluationCase) for c in cases):
        raise ValueError("provide one to 100 cases")
    if not isinstance(factories, dict) or not 1 <= len(factories) <= 8:
        raise ValueError("provide one to eight backend factories")
    if any(not isinstance(k, str) or not 1 <= len(k) <= 128 or not callable(v) for k, v in factories.items()):
        raise ValueError("invalid backend factories")
    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 120:
        raise ValueError("invalid per-operation deadline")
    records = []
    dataset_digest = sha256(json.dumps([asdict(c) for c in cases], sort_keys=True).encode()).hexdigest()
    for case in cases:
        raw_digest = sha256(json.dumps([asdict(t) for t in (*case.initial, *case.updates)], sort_keys=True).encode()).hexdigest()
        for name, factory in factories.items():
            backend = None
            record = dict(case_id=case.case_id, backend=name, raw_input_digest=raw_digest,
                status="failed", operations=[], billed_usage=None, cleanup="not_started")
            async def measured(operation, fn, *args, **kwargs):
                started = perf_counter()
                try:
                    return await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout_seconds)
                finally:
                    record["operations"].append(dict(operation=operation, elapsed_seconds=perf_counter()-started))
            try:
                backend = await measured("initialize", factory, "eval-" + uuid4().hex)
                stages = []
                for stage, turns, queries in (("initial", case.initial, case.initial_queries),
                                               ("updated", case.updates, case.update_queries)):
                    for turn in turns:
                        await measured("add", backend.add, turn)
                    scores = []
                    for query in queries:
                        answers = _answers(await measured("search", backend.search, query.text, limit=8))
                        text = "\n".join(answers).casefold()
                        scores.append(sum(fragment.casefold() in text for fragment in query.expected_fragments)
                                      / len(query.expected_fragments))
                    stages.append(dict(stage=stage, fragment_recall=sum(scores)/len(scores), queries=len(scores)))
                await measured("delete", backend.delete_all)
                residual = 0
                for query in (*case.initial_queries, *case.update_queries):
                    residual += len(_answers(await measured("search_after_delete", backend.search, query.text, limit=8)))
                record.update(status="completed", stages=stages, residual_results=residual)
            except Exception as error:
                record["error_type"] = type(error).__name__
            finally:
                if backend is not None:
                    try:
                        await measured("cleanup", backend.delete_all)
                        record["cleanup"] = "completed"
                    except Exception:
                        record["cleanup"] = "requires_host_cleanup"
                    try:
                        await measured("close", backend.close)
                    except Exception:
                        record["cleanup"] = "requires_host_cleanup"
            records.append(record)
    return dict(dataset_digest=dataset_digest, records=records,
                metric="casefolded_fragment_recall", automatic_winner=False,
                real_backend_execution="determined_by_host_factories")
