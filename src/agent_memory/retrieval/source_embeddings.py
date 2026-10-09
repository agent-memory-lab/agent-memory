"""Opt-in, host-only retained-source embeddings with exact transactional proofs.

This module is deliberately separate from the request-local EmbeddingReranker.
It neither reads MemoryItem metadata nor treats MemoryQuery as authentication.
The injected port must perform no unconfigured model/recipient substitution.
"""

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from math import hypot, isfinite
from time import monotonic
from uuid import uuid4

from ..operations.source_revisions import document_head, source_is_current
from .model_contracts import ModelCoordinates, ModelError, canonical, digest, hash_value, text
from .source_processing import SourceProcessingAuthority

EMBEDDING_KINDS = ("source_embedding_header", "source_embedding_body", "source_embedding_grant")
HEADER, BODY, GRANT = EMBEDDING_KINDS
SCHEMA = "source-embedding/1"


@dataclass(frozen=True, slots=True)
class EmbeddingConfiguration:
    """Pinned identity of the injected embedding port, supplied by the trusted host.

    recipient identifies the actual processing destination (including account,
    endpoint/region/policy as applicable); a model tag alone is insufficient.
    No transport, model download, or model enablement is provided by the core.
    """

    provider: str
    recipient: str
    model: str
    model_revision: str
    runtime_sha256: str
    dimensions: int
    normalization: str = "none"
    input_revision: str = "raw-source-utf8/1"
    max_input_bytes: int = 65536
    timeout_seconds: int = 30

    def __post_init__(self):
        for key in ("provider", "recipient", "model"):
            text(getattr(self, key))
        for key in ("model_revision", "runtime_sha256"):
            hash_value(getattr(self, key))
        for key, maximum in (
            ("dimensions", 4096),
            ("max_input_bytes", 1048576),
            ("timeout_seconds", 600),
        ):
            value = getattr(self, key)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ModelError("invalid_embedding_configuration")
        if self.normalization not in {"none", "l2"} or self.input_revision != "raw-source-utf8/1":
            raise ModelError("invalid_embedding_configuration")

    @property
    def fingerprint(self):
        return digest(asdict(self))


@dataclass(frozen=True, slots=True)
class SourceEmbeddingInput:
    """Request-local source body; created and revalidated by the host authority."""

    configuration: EmbeddingConfiguration
    coordinates: ModelCoordinates
    source_id: str
    content: str
    manifest_json: str

    @property
    def key(self):
        return digest(json.loads(self.manifest_json))


@dataclass(frozen=True, slots=True)
class EmbeddingResponse:
    """An injected port must bind its finite vector to the exact dispatched input."""

    configuration_sha256: str
    input_sha256: str
    values: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class SourceEmbedding:
    source_id: str
    configuration_sha256: str
    values: tuple[float, ...]
    cache_hit: bool


def _vector(values, configuration):
    if type(values) not in (tuple, list) or len(values) != configuration.dimensions:
        raise ModelError("invalid_embedding_vector")
    if any(type(value) not in (int, float) for value in values):
        raise ModelError("invalid_embedding_vector")
    try:
        result = tuple(float(value) for value in values)
        norm = hypot(*result)
    except (ValueError, OverflowError):
        raise ModelError("invalid_embedding_vector") from None
    if (
        not all(isfinite(value) for value in result)
        or not isfinite(norm)
        or not 1e-150 <= norm <= 1e150
    ):
        raise ModelError("invalid_embedding_vector")
    if configuration.normalization == "l2" and abs(norm - 1.0) > 1e-6:
        raise ModelError("invalid_embedding_normalization")
    return result


class SourceEmbeddingAuthority(SourceProcessingAuthority):
    """Raw-source-only authority. The validator authenticates the whole coordinate proof.

    Reuses the existing read/processing grants, current host authority, observed
    clock and source-head controls. Embedding grants use a distinct ledger kind;
    a generation-model processing grant cannot authorize an embedding call.
    Atom, derived, question and free-form candidate text are intentionally absent.
    """

    processing_grant_kind = GRANT

    def __init__(self, service, *, configuration, verify_coordinates):
        if type(configuration) is not EmbeddingConfiguration:
            raise ModelError("invalid_embedding_configuration")
        super().__init__(
            service, configuration=configuration, verify_coordinates=verify_coordinates
        )

    async def _assemble(self, uow, coordinates, source_id):
        if type(coordinates) is not ModelCoordinates:
            raise ModelError("invalid_embedding_coordinates")
        text(source_id)
        epoch, authority, grants = await self._metadata(uow, coordinates, (source_id,))
        source = await uow.get_source_event(self.scope, source_id)
        if source is None or not await source_is_current(uow, source):
            raise ModelError("model_source_unavailable")
        if (
            type(source.content) is not str
            or len(source.content.encode()) > self.configuration.max_input_bytes
        ):
            raise ModelError("embedding_input_budget_exceeded")
        document_id, head = await document_head(uow, source)
        manifest = dict(
            schema=SCHEMA,
            configuration_sha256=self.configuration.fingerprint,
            coordinates_sha256=digest(coordinates.payload()),
            scope_sha256=digest(self.scope.partition_key()),
            sources=[source_id],
            source_content_sha256=sha256(source.content.encode()).hexdigest(),
            source_record_sha256=source.content_hash,
            document_sha256=digest(document_id),
            document_head_sha256=digest(head),
            epoch=epoch,
            authority_sha256=digest(authority),
            grants=grants,
        )
        if await self._metadata(uow, coordinates, (source_id,)) != (epoch, authority, grants):
            raise ModelError("model_input_changed")
        return SourceEmbeddingInput(
            self.configuration, coordinates, source_id, source.content, canonical(manifest)
        )

    async def prepare(self, coordinates, source_id):
        observed = await self.clock_barrier()
        try:
            async with self.repository.unit_of_work() as uow:
                return await self._assemble(uow, coordinates, source_id)
        except BaseException:
            await self.failure_barrier(observed)
            raise

    async def validate(self, uow, sealed):
        if type(sealed) is not SourceEmbeddingInput or sealed.configuration != self.configuration:
            raise ModelError("embedding_configuration_changed")
        fresh = await self._assemble(uow, sealed.coordinates, sealed.source_id)
        if fresh != sealed:
            raise ModelError("model_input_changed")


class GovernedSourceEmbeddings:
    """Real, bounded persistent reuse for exactly proven source revisions.

    A durable reservation limits concurrent port calls across runtime instances.
    Reusable opaque slots bound physical row count, including erased tombstones.
    max_cache_bytes bounds serialized payloads, not database/index file size.
    No retained in-process task, text, query vector or candidate-vector dictionary.
    """

    def __init__(
        self,
        authority,
        port,
        *,
        cache_seconds=300,
        max_cache_entries=128,
        max_cache_bytes=8 * 1024 * 1024,
        max_inflight=4,
    ):
        if type(authority) is not SourceEmbeddingAuthority:
            raise ModelError("embedding_authority_required")
        if getattr(port, "configuration", None) != authority.configuration or not callable(
            getattr(port, "embed", None)
        ):
            raise ModelError("embedding_configuration_mismatch")
        for value, lower, upper in (
            (cache_seconds, 1, 86400),
            (max_cache_entries, 1, 4096),
            (max_cache_bytes, 16384, 64 * 1024 * 1024),
            (max_inflight, 1, 128),
        ):
            if type(value) is not int or not lower <= value <= upper:
                raise ModelError("invalid_embedding_cache_limits")
        self.authority, self.port = authority, port
        self.repository, self.scope, self.clock = (
            authority.repository,
            authority.scope,
            authority.clock,
        )
        self.cache_seconds, self.max_entries = cache_seconds, max_cache_entries
        self.max_bytes, self.max_inflight = max_cache_bytes, max_inflight

    async def _scrub(self, uow, slot, *, state="expired"):
        for kind in (HEADER, BODY):
            await uow.derived_put(self.scope, kind, slot, {"state": state})
        await uow.derived_edges(self.scope, "source-embedding:" + slot, ())

    async def _headers(self, uow):
        rows = await uow.derived_records(self.scope, HEADER)
        result = {}
        for item in rows:
            slot, row = item["identity"], item["payload"]
            if row.get("state") not in {"expired", "erased"}:
                if (
                    row.get("schema") != SCHEMA
                    or row.get("state") not in {"reserved", "ready"}
                    or type(row.get("charge_bytes")) is not int
                    or row["charge_bytes"] < 1
                ):
                    raise ModelError("embedding_cache_proof_invalid")
                try:
                    expiry = datetime.fromisoformat(row["expires_at"])
                    if expiry.utcoffset() is None:
                        raise ValueError
                except (KeyError, TypeError, ValueError):
                    raise ModelError("embedding_cache_proof_invalid") from None
                if expiry <= self.clock():
                    await self._scrub(uow, slot)
                    row = {"state": "expired"}
            result[slot] = row
        return result

    async def sweep_expired(self):
        """Host maintenance physically scrubs expired vectors without reading bodies."""
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            before = await uow.derived_records(self.scope, HEADER)
            rows = await self._headers(uow)
            return sum(item["payload"] != rows[item["identity"]] for item in before)

    async def _claim(self, sealed):
        async with self.repository.unit_of_work() as uow:
            await self.authority.validate(uow, sealed)
            rows = await self._headers(uow)
            if (
                len(rows) > self.max_entries
                or sum(row.get("charge_bytes", 64) for row in rows.values()) > self.max_bytes
            ):
                raise ModelError("embedding_cache_capacity")
            for slot, row in rows.items():
                if row.get("key") == sealed.key:
                    return ("hit" if row["state"] == "ready" else "wait", slot, None)
            active = {s: r for s, r in rows.items() if r.get("state") == "reserved"}
            if len(active) >= self.max_inflight:
                raise ModelError("embedding_concurrency_capacity")
            # Space is reserved before dispatch, including manifest + finite vector.
            charge = (
                8192
                + len(sealed.manifest_json.encode())
                + 2 * len(sealed.source_id.encode())
                + sealed.configuration.dimensions * 32
            )
            if charge + 64 * self.max_entries > self.max_bytes:
                raise ModelError("embedding_cache_capacity")
            reusable = sorted(s for s, r in rows.items() if r.get("state") in {"expired", "erased"})
            ready = sorted(
                (r["expires_at"], s) for s, r in rows.items() if r.get("state") == "ready"
            )
            if reusable:
                slot = reusable[0]
            elif len(rows) < self.max_entries:
                slot = next(str(i) for i in range(self.max_entries) if str(i) not in rows)
            elif ready:
                slot = ready.pop(0)[1]
                await self._scrub(uow, slot)
                rows[slot] = {"state": "expired"}
            else:
                raise ModelError("embedding_cache_capacity")

            def usage():
                return sum(r.get("charge_bytes", 64) for s, r in rows.items() if s != slot)

            while usage() + charge > self.max_bytes and ready:
                other = ready.pop(0)[1]
                if other != slot:
                    await self._scrub(uow, other)
                    rows[other] = {"state": "expired"}
            if usage() + charge > self.max_bytes:
                raise ModelError("embedding_cache_capacity")
            token = uuid4().hex
            row = dict(
                schema=SCHEMA,
                state="reserved",
                token=token,
                key=sealed.key,
                sources=[sealed.source_id],
                generation_manifest=json.loads(sealed.manifest_json),
                charge_bytes=charge,
                expires_at=(
                    self.clock() + timedelta(seconds=sealed.configuration.timeout_seconds + 1)
                ).isoformat(),
            )
            await uow.derived_put(self.scope, HEADER, slot, row)
            await uow.derived_put(
                self.scope, BODY, slot, {"state": "reserved", "sources": [sealed.source_id]}
            )
            await uow.derived_edges(
                self.scope,
                "source-embedding:" + slot,
                (("processing", "source:" + sealed.source_id),),
            )
            await self.authority.validate(uow, sealed)
            return "claimed", slot, token

    async def _release(self, slot, token):
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            row = await uow.derived_get(self.scope, HEADER, slot)
            if row and row.get("state") == "reserved" and row.get("token") == token:
                await self._scrub(uow, slot)

    def _check_port(self, sealed):
        if self.port.configuration != sealed.configuration:
            raise ModelError("embedding_configuration_changed")

    async def _publish(self, sealed, slot, token, values):
        async with self.repository.unit_of_work() as uow:
            await self.authority.validate(uow, sealed)
            self._check_port(sealed)
            row = await uow.derived_get(self.scope, HEADER, slot)
            if (
                not row
                or row.get("state") != "reserved"
                or row.get("token") != token
                or row.get("key") != sealed.key
                or datetime.fromisoformat(row["expires_at"]) <= self.clock()
            ):
                raise ModelError("embedding_execution_fenced")
            body = dict(
                schema=SCHEMA,
                state="ready",
                sources=[sealed.source_id],
                key=sealed.key,
                values=list(values),
            )
            manifest = json.loads(sealed.manifest_json)
            expiry = min(
                self.clock() + timedelta(seconds=self.cache_seconds),
                datetime.fromisoformat(manifest["grants"][sealed.source_id]["valid_until"]),
            )
            header = {
                **row,
                "state": "ready",
                "body_sha256": digest(body),
                "expires_at": expiry.isoformat(),
            }
            header.pop("token")
            actual = len(canonical(header).encode()) + len(canonical(body).encode())
            if actual > row["charge_bytes"]:
                raise ModelError("embedding_cache_capacity")
            await uow.derived_put(self.scope, BODY, slot, body)
            await uow.derived_put(self.scope, HEADER, slot, header)
            await self.authority.validate(uow, sealed)
            if expiry <= self.clock() or datetime.fromisoformat(row["expires_at"]) <= self.clock():
                raise ModelError("embedding_execution_fenced")

    async def _deliver(self, sealed, slot, *, cache_hit):
        async with self.repository.unit_of_work() as uow:
            await self.authority.validate(uow, sealed)
            self._check_port(sealed)
            row = await uow.derived_get(self.scope, HEADER, slot)
            if not row or row.get("state") != "ready" or row.get("key") != sealed.key:
                raise ModelError("embedding_execution_fenced")
            if (
                row.get("schema") != SCHEMA
                or row.get("generation_manifest") != json.loads(sealed.manifest_json)
                or datetime.fromisoformat(row["expires_at"]) <= self.clock()
            ):
                raise ModelError("embedding_cache_proof_invalid")
            body = await uow.derived_get(self.scope, BODY, slot)
            if (
                not body
                or digest(body) != row.get("body_sha256")
                or body.get("key") != sealed.key
                or body.get("schema") != SCHEMA
                or body.get("state") != "ready"
                or body.get("sources") != [sealed.source_id]
            ):
                raise ModelError("embedding_cache_proof_invalid")
            values = _vector(body.get("values"), sealed.configuration)
            await self.authority.validate(uow, sealed)
            if datetime.fromisoformat(row["expires_at"]) <= self.clock():
                raise ModelError("embedding_execution_fenced")
            return SourceEmbedding(
                sealed.source_id, sealed.configuration.fingerprint, values, cache_hit
            )

    async def embed_source(self, coordinates, source_id):
        """Embed one authoritative document revision, or reuse its exact proven vector.

        Candidate selection/ranking remains the host's responsibility. Coordinates
        must be authenticated by the registered guard; caller booleans never suffice.
        Every waiter independently revalidates before reading and returning a vector.
        """
        await self.sweep_expired()
        sealed = await self.authority.prepare(coordinates, source_id)
        observed = self.clock()
        slot = token = None
        try:
            stop = monotonic() + sealed.configuration.timeout_seconds * 2 + 2
            while True:
                self._check_port(sealed)
                state, slot, token = await self._claim(sealed)
                if state == "hit":
                    return await self._deliver(sealed, slot, cache_hit=True)
                if state == "claimed":
                    break
                if monotonic() >= stop:
                    raise ModelError("embedding_wait_timeout")
                await asyncio.sleep(0.02)
            # Last full authority check immediately before provider dispatch.
            async with self.repository.unit_of_work() as uow:
                await self.authority.validate(uow, sealed)
                self._check_port(sealed)
            async with asyncio.timeout(sealed.configuration.timeout_seconds):
                response = await self.port.embed(sealed)
            if (
                type(response) is not EmbeddingResponse
                or response.configuration_sha256 != sealed.configuration.fingerprint
                or response.input_sha256 != sealed.key
            ):
                raise ModelError("embedding_response_mismatch")
            values = _vector(response.values, sealed.configuration)
            await self._publish(sealed, slot, token, values)
            return await self._deliver(sealed, slot, cache_hit=False)
        except BaseException as error:
            await self.authority.failure_barrier(observed)
            from ..derived.model import DerivedError

            if isinstance(error, (ModelError, DerivedError, asyncio.CancelledError)):
                raise
            raise ModelError("embedding_execution_failed") from None
        finally:
            if token is not None:
                await asyncio.shield(self._release(slot, token))
