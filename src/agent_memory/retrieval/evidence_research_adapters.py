"""Working SQLite, registered-view and independent-vector research adapters.

The census proves completeness only for a host-registered event-type collection
of retained rows. It makes no claim about the world or natural-language truth.
"""

from __future__ import annotations

import asyncio
import hmac
import math
import secrets
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from hashlib import sha256

from ..composition import build_local_kernel
from ..domain import (
    Citation,
    MemoryBundle,
    MemoryCapabilities,
    MemoryChannel,
    MemoryItem,
    MemoryKind,
)
from ..providers import ReciprocalRankFusionReranker
from ..sqlite import SQLiteMemoryRepository
from .evidence_research import EvidenceResearchLoop, evidence_records
from .evidence_research_contracts import (
    CoverageReceipt,
    FinalizedEvidence,
    RetrievalBatch,
    bounded_text,
    digest,
)
from .packing import token_estimate
from .semantic import SemanticVectorSpec
from .sqlite_delivery_guard import guard_delivery_snapshot


def _bundle(items=(), *, request_id=None, metadata=None):
    return MemoryBundle(
        (),
        tuple(items),
        (),
        (),
        tuple(Citation(i.id, (i.id,)) for i in items),
        sum(token_estimate(i.text) for i in items),
        metadata or {"coverage": "partial"},
        MemoryCapabilities(),
        request_id=request_id,
    )


def _item(row, *, score=1.0):
    return MemoryItem(
        row["id"],
        MemoryKind.EVENT,
        row["content"],
        score,
        datetime.fromisoformat(row["occurred_at"]),
        {
            "source_event_ids": (row["id"],),
            "channel": MemoryChannel.SEMANTIC,
            "event_type": row["event_type"],
        },
    )


@dataclass(frozen=True, slots=True)
class SQLiteCensusPolicy:
    domain: str
    event_type: str
    max_rows: int = 100

    def __post_init__(self):
        bounded_text(self.domain, "census_domain", 128)
        bounded_text(self.event_type, "census_event_type", 128)
        if type(self.max_rows) is not int or not 1 <= self.max_rows <= 100:
            raise ValueError("invalid_census_max_rows")


class SQLiteResearchBackend:
    supports_history = False  # Raw-event research cannot reconstruct both axes.

    def __init__(
        self, kernel, repository: SQLiteMemoryRepository, *, census_policies=(), semantic_route=None
    ):
        if (
            type(repository) is not SQLiteMemoryRepository
            or getattr(kernel, "_repository", None) is not repository
        ):
            raise ValueError("kernel_and_sqlite_repository_must_share_authority")
        if (
            type(getattr(kernel, "_reranker", None)) is not ReciprocalRankFusionReranker
            or getattr(kernel, "_passage_reranker", None) is not None
        ):
            raise ValueError(
                "research_requires_model_free_native_ranking_use_bounded_semantic_route"
            )
        if (
            type(census_policies) is not tuple
            or len(census_policies) > 16
            or any(type(p) is not SQLiteCensusPolicy for p in census_policies)
            or len({p.domain for p in census_policies}) != len(census_policies)
        ):
            raise ValueError("invalid_census_policies")
        self.kernel, self.repository = kernel, repository
        self.policies = {p.domain: p for p in census_policies}
        self.semantic_route = semantic_route
        self.semantic_model_calls = getattr(semantic_route, "model_calls_per_search", 0)
        if self.semantic_model_calls not in (0, 1):
            raise ValueError("semantic_route_must_be_single_call")
        self._secret = secrets.token_bytes(32)
        self._authority_id = secrets.token_hex(16)

    @classmethod
    def from_memory(cls, memory, **options):
        """Use an initialized AgentMemory's actual kernel; no parallel store."""
        provider = memory.provider
        repository = getattr(provider, "_repository", None)
        if type(repository) is not SQLiteMemoryRepository:
            raise ValueError("sqlite_agent_memory_required")
        result = cls(provider, repository, **options)
        result.bound_scope = memory.scope
        return result

    async def search(self, contract, requirement, route, allowance):
        if hasattr(self, "bound_scope") and contract.query.scope != self.bound_scope:
            raise ValueError("research_scope_outside_memory")
        query = replace(
            contract.query,
            text=requirement.search_text if requirement else contract.query.text,
            request_id=allowance.call_id,
            run_id=contract.query.run_id or contract.query.request_id,
        )
        if query.valid_at is not None or query.known_at is not None:
            return RetrievalBatch(_bundle(), partial_reason="historical_research_unsupported")
        if route in ("semantic", "census") and MemoryChannel.SEMANTIC not in query.channels:
            return RetrievalBatch(_bundle(), partial_reason="semantic_channel_disabled")
        if route == "native":
            return RetrievalBatch(await self.kernel.retrieve(query))
        if route == "semantic":
            if self.semantic_route is None:
                return RetrievalBatch(_bundle(), partial_reason="semantic_backend_unavailable")
            bundle = await self.semantic_route.retrieve(query, allowance)
            # Unlike the generic external-candidate fallback, this adapter only
            # accepts evidence that the local source authority can validate.
            return RetrievalBatch(
                await self.repository.guard_retrieval_delivery(query, bundle),
                model_calls=self.semantic_model_calls,
            )
        if route == "census" and requirement is not None:
            return await asyncio.to_thread(self._read_census, contract, requirement)
        return RetrievalBatch(_bundle(), partial_reason="unsupported_research_route")

    def _census(self, connection, contract, requirement):
        policy = self.policies.get(requirement.coverage_domain)
        if policy is None:
            return None, "unregistered_coverage_domain"
        where, params = self.repository._visible_scope_clause(contract.query.scope)
        # The filter is a fixed host registration. No reader SQL/filter is used.
        headers = connection.execute(
            f"SELECT id,length(CAST(content AS BLOB)) AS byte_count FROM events WHERE {where} "
            "AND archived_at IS NULL AND event_type=? ORDER BY id LIMIT ?",
            (*params, policy.event_type, policy.max_rows + 1),
        ).fetchall()
        if len(headers) > policy.max_rows:
            return None, "census_row_budget_exceeded"
        # Bound bytes before hydrating content, including pathological large rows.
        if (
            len(headers) > contract.limits.max_evidence_items
            or sum(r["byte_count"] for r in headers) > contract.query.token_budget * 4
        ):
            return None, "census_context_budget_exceeded"
        rows = connection.execute(
            f"SELECT id,event_type,content,occurred_at FROM events WHERE {where} "
            "AND archived_at IS NULL AND event_type=? ORDER BY id LIMIT ?",
            (*params, policy.event_type, policy.max_rows + 1),
        ).fetchall()
        if (
            len(rows) > contract.limits.max_evidence_items
            or sum(token_estimate(r["content"]) for r in rows) > contract.query.token_budget
        ):
            return None, "census_context_budget_exceeded"
        return tuple(dict(r) for r in rows), None

    def _receipt(self, contract, requirement, rows, bundle):
        versions = tuple((r["id"], digest(r)) for r in rows)
        receipt = CoverageReceipt(
            requirement.id,
            contract.fingerprint,
            requirement.coverage_domain,
            digest((requirement.coverage_domain, versions)),
            versions,
            tuple(r.key for r in evidence_records((bundle,))),
            self._authority_id,
            "",
        )
        signature = hmac.new(self._secret, digest(asdict(receipt)).encode(), sha256).hexdigest()
        return replace(receipt, signature=signature)

    def _read_census(self, contract, requirement):
        with self.repository._connection() as connection:
            connection.execute("BEGIN")
            rows, reason = self._census(connection, contract, requirement)
            if rows is None:
                return RetrievalBatch(_bundle(), partial_reason=reason)
            bundle = _bundle(tuple(_item(r) for r in rows), request_id=contract.query.request_id)
            # Census raw rows pass the same source/admission/delete guard as recall.
            bundle = guard_delivery_snapshot(self.repository, connection, contract.query, bundle)
            if len(bundle.relevant_memories) != len(rows):
                return RetrievalBatch(bundle, partial_reason="census_rows_not_deliverable")
            return RetrievalBatch(bundle, (self._receipt(contract, requirement, rows, bundle),))

    async def finalize(self, contract, bundles, receipts, allowance):
        return await asyncio.to_thread(self._finalize, contract, bundles, receipts)

    def _finalize(self, contract, bundles, receipts):
        with self.repository._connection() as connection:
            connection.execute("BEGIN")

            # The base guard verifies body/version/authority. Research also
            # binds source occurrence coordinates used by host temporal policy.
            def current_time(item):
                if item.kind != MemoryKind.EVENT:
                    return True
                row = connection.execute(
                    "SELECT occurred_at FROM events WHERE id=?", (item.id,)
                ).fetchone()
                return row is not None and row["occurred_at"] == item.occurred_at.isoformat()

            guarded = []
            for bundle in bundles:
                current = replace(
                    bundle,
                    relevant_memories=tuple(i for i in bundle.relevant_memories if current_time(i)),
                    episodes=tuple(i for i in bundle.episodes if current_time(i)),
                    procedures=tuple(i for i in bundle.procedures if current_time(i)),
                )
                guarded.append(
                    guard_delivery_snapshot(self.repository, connection, contract.query, current)
                )
            final_keys = {r.key for r in evidence_records(guarded)}
            valid = []
            requirements = {r.id: r for r in contract.requirements}
            for receipt in receipts:
                req = requirements.get(receipt.requirement_id)
                if req is None or req.coverage_domain != receipt.domain:
                    continue
                rows, reason = self._census(connection, contract, req)
                if reason is not None:
                    continue
                bundle = _bundle(tuple(_item(r) for r in rows))
                expected = self._receipt(contract, req, rows, bundle)
                if (
                    hmac.compare_digest(expected.signature, receipt.signature)
                    and expected == receipt
                    and set(receipt.evidence_keys) <= final_keys
                ):
                    valid.append(receipt)
            return FinalizedEvidence(tuple(guarded), tuple(valid))


@dataclass(frozen=True, slots=True)
class VectorSource:
    event_id: str
    text_sha256: str
    vector: tuple[float, ...]


class FrozenQueryVectors:
    """Offline query-vector adapter; production may supply its own bounded embedder.

    This fixture utility makes no claim that supplied vectors were learned or
    achieve semantic quality. The same route accepts externally built vectors.
    """

    model_calls_per_search = 0

    def __init__(self, spec: SemanticVectorSpec, vectors):
        if type(spec) is not SemanticVectorSpec or type(vectors) is not dict or len(vectors) > 256:
            raise ValueError("invalid_frozen_vectors")
        self.spec = spec
        self.vectors = {text: _vector(value, spec.dimension) for text, value in vectors.items()}
        for text in self.vectors:
            bounded_text(text, "vector_query", 4096)

    async def embed_query(self, text, allowance):
        return self.vectors.get(text, (0.0,) * self.spec.dimension)


def _vector(values, dimension):
    if (
        not isinstance(values, (tuple, list))
        or len(values) != dimension
        or any(type(v) not in (int, float) or not math.isfinite(v) for v in values)
    ):
        raise ValueError("invalid_semantic_vector")
    norm = math.hypot(*values)
    return tuple(float(v) / norm if norm else 0.0 for v in values)


class SQLiteVectorResearchRoute:
    """Independent cosine candidates, hydrated and authorized by SQLite.

    There is no lexical gate. Source text hashes invalidate stale embeddings.
    Storage is bounded in memory; this does not require or install a vector DB.
    """

    def __init__(self, repository, embedder, sources: tuple[VectorSource, ...]):
        if (
            type(repository) is not SQLiteMemoryRepository
            or type(embedder.spec) is not SemanticVectorSpec
        ):
            raise ValueError("typed_vector_adapter_required")
        if (
            type(sources) is not tuple
            or len(sources) > 4096
            or any(type(s) is not VectorSource for s in sources)
            or len({s.event_id for s in sources}) != len(sources)
            or len(sources) * embedder.spec.dimension > 1048576
        ):
            raise ValueError("bounded_unique_vector_sources_required")
        self.repository, self.embedder = repository, embedder
        self.sources = tuple(
            replace(s, vector=_vector(s.vector, embedder.spec.dimension)) for s in sources
        )
        self.model_calls_per_search = getattr(embedder, "model_calls_per_search", None)
        if self.model_calls_per_search not in (0, 1):
            raise ValueError("bounded_embedder_call_declaration_required")
        for source in sources:
            bounded_text(source.event_id, "vector_source", 128)
            if len(source.text_sha256) != 64:
                raise ValueError("source_text_hash_required")

    async def retrieve(self, query, allowance):
        if query.valid_at is not None or query.known_at is not None:
            raise ValueError("semantic_history_unsupported")
        if MemoryChannel.SEMANTIC not in query.channels:
            return _bundle(request_id=query.request_id)
        vector = _vector(
            await self.embedder.embed_query(query.text, allowance), self.embedder.spec.dimension
        )
        return await asyncio.to_thread(self._retrieve, query, vector)

    def _retrieve(self, query, vector):
        # Rank vectors before loading bodies; unrelated sources need no hydration.
        sources = {}
        for source in self.sources:
            score = sum(a * b for a, b in zip(vector, source.vector, strict=True))
            if score > 0:
                sources[source.event_id] = (source, score)
        scored = []
        where, params = self.repository._visible_scope_clause(query.scope)
        with self.repository._connection() as connection:
            connection.execute("BEGIN")
            # Governed raw sources must not consume the result/context capacity
            # before the final guard removes them. Reuse the admission authority.
            protected = set(self.repository._admission_protected_sources(connection, query.scope))
            ids = tuple(identity for identity in sources if identity not in protected)
            for start in range(0, len(ids), 256):
                batch = ids[start : start + 256]
                marks = ",".join("?" for _ in batch)
                rows = connection.execute(
                    f"SELECT id,event_type,content,occurred_at FROM events WHERE {where} "
                    f"AND archived_at IS NULL AND id IN ({marks}) AND length(content)<=4096",
                    (*params, *batch),
                ).fetchall()
                for row in rows:
                    source, score = sources[row["id"]]
                    if sha256(row["content"].encode()).hexdigest() != source.text_sha256:
                        continue
                    scored.append(_item(row, score=score))
            scored.sort(key=lambda i: (-i.score, i.id))
            selected, used = [], 0
            for item in scored:
                cost = token_estimate(item.text)
                if used + cost > query.token_budget:
                    continue
                selected.append(item)
                used += cost
                if len(selected) >= query.limit:
                    break
            bundle = _bundle(
                tuple(selected),
                request_id=query.request_id,
                metadata={
                    "coverage": "partial",
                    "semantic_candidates": len(scored),
                    "lexical_gate": False,
                    "index_revision": digest(self.sources),
                },
            )
            return guard_delivery_snapshot(self.repository, connection, query, bundle)


class RegisteredQuestionResearchView:
    """Read an already registered QuestionView; never starts refresh/model work."""

    def __init__(self, router):
        self.router = router

    async def read(self, contract, allowance):
        from ..derived.model import DerivedError

        query = contract.query
        if query.scope != self.router.service.scope:
            raise ValueError("registered_view_scope_mismatch")
        if query.valid_at is not None or query.known_at is not None:
            return {"availability_status": "partial", "reason": "view_time_mapping_unsupported"}
        route = await self.router.route(contract.registered_question, actor=contract.actor)
        if route["route"] != "question":
            return {"availability_status": "partial", "reason": route["reason"]}
        try:
            return await self.router.service.read(route["question_id"], actor=contract.actor)
        except DerivedError as error:
            return {"availability_status": "partial", "reason": error.code}


@asynccontextmanager
async def sqlite_research(
    database_path,
    reader,
    *,
    census_policies=(),
    semantic_factory=None,
    planner=None,
    temporal_resolver=None,
    view=None,
    decision_sink=None,
):
    """Ready-to-run local host factory; existing packing defaults stay unchanged."""
    kernel = build_local_kernel(database_path)
    try:
        await kernel.initialize()
        repository = kernel._repository
        semantic_route = semantic_factory(repository) if semantic_factory is not None else None
        backend = SQLiteResearchBackend(
            kernel, repository, census_policies=census_policies, semantic_route=semantic_route
        )
        yield EvidenceResearchLoop(
            backend,
            reader,
            planner=planner,
            temporal_resolver=temporal_resolver,
            view=view,
            decision_sink=decision_sink,
        )
    finally:
        close = getattr(kernel, "close", None)
        if close is not None:
            await close()
