"""Opt-in, bounded fusion of lexical and host-provided retrieval candidates."""

from dataclasses import dataclass
from typing import Protocol, Sequence

from .candidate_fusion import CandidateFusionResult, fuse_candidates
from .domain import MemoryScope
from .lexical_plugin import LexicalCandidatePlugin
from .lexical_retrieval import LexicalSearchTrace
from .plugin_protocol import RetrievalCandidate
from .scoped_lexical_retrieval import ScopeIsolationError


@dataclass(frozen=True, slots=True)
class ScopedRetrievalCandidate:
    """A candidate labelled with the scope of its authoritative source record."""

    scope: MemoryScope
    candidate: RetrievalCandidate


class AdditionalCandidateSource(Protocol):
    """A trusted host adapter; it must never copy a requested scope onto data."""

    async def candidates(
        self, query_text: str, scope: MemoryScope, *, limit: int
    ) -> Sequence[ScopedRetrievalCandidate]: ...


@dataclass(frozen=True, slots=True)
class HybridCandidateResult:
    fusion: CandidateFusionResult
    lexical_trace: LexicalSearchTrace


class HybridCandidatePlugin:
    """A separate candidate pipeline; the default memory recall stays unchanged."""

    def __init__(
        self,
        lexical: LexicalCandidatePlugin,
        additional_sources: Sequence[AdditionalCandidateSource] = (),
        *,
        max_candidates: int = 256,
        max_results: int = 8,
    ) -> None:
        if len(additional_sources) > 4:
            raise ValueError("at most four additional candidate sources are supported")
        if isinstance(max_candidates, bool) or not isinstance(max_candidates, int) or not 1 <= max_candidates <= 256:
            raise ValueError("max_candidates must be between 1 and 256")
        if isinstance(max_results, bool) or not isinstance(max_results, int) or not 1 <= max_results <= max_candidates:
            raise ValueError("max_results must be between 1 and max_candidates")
        self._lexical = lexical
        self._sources = tuple(additional_sources)
        self._max_candidates = max_candidates
        self._max_results = max_results

    async def candidates(
        self, query_text: str, scope: MemoryScope
    ) -> HybridCandidateResult:
        lexical_result = await self._lexical.candidates(query_text, scope)
        combined = list(lexical_result.candidates)
        if len(combined) > self._max_candidates:
            raise ValueError("lexical source exceeded the candidate budget")

        for source in self._sources:
            remaining = self._max_candidates - len(combined)
            if remaining == 0:
                break
            batch = await source.candidates(query_text, scope, limit=remaining)
            if not isinstance(batch, Sequence) or isinstance(batch, (str, bytes)):
                raise ScopeIsolationError("candidate source must return a bounded sequence")
            if len(batch) > remaining:
                raise ScopeIsolationError("candidate source exceeded the candidate budget")
            for record in batch:
                if not isinstance(record, ScopedRetrievalCandidate):
                    raise ScopeIsolationError("candidate source returned an unlabelled item")
                if not isinstance(record.scope, MemoryScope) or record.scope != scope:
                    raise ScopeIsolationError("candidate source returned another scope")
                if not isinstance(record.candidate, RetrievalCandidate):
                    raise ScopeIsolationError("candidate source returned an invalid candidate")
                combined.append(record.candidate)

        return HybridCandidateResult(
            fusion=fuse_candidates(
                combined,
                max_candidates=self._max_candidates,
                max_results=self._max_results,
            ),
            lexical_trace=lexical_result.trace,
        )
