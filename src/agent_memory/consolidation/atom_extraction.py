"""Bounded automatic candidate generation, source review and typed admission.

Generators propose data. Reviewers judge source fidelity and reuse, while the
host's AdmissionPolicy and SourceAuthority retain publication authority.
External calls run outside database transactions; the final L0, diagnostics and
admission result commit together, including empty and failed extraction batches.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite
from time import perf_counter
from typing import Any

from ..domain import (
    AdmissionReceipt,
    AtomDraft,
    AtomExtractionDecision,
    AtomExtractionReceipt,
    AtomReview,
    ExtractedAtom,
    MemoryEvent,
    ScopeLevel,
    SourceAuthority,
    canonical_json,
)
from ..lifecycle import capture_annotation, is_memory_context
from ..ports import AdmissionRepository, AtomGenerator, AtomReviewer
from .admission import AdmissionPolicy, authority_to_payload, draft_to_payload
from .admission_runtime import AdmissionEngine


class _InvalidCandidate(ValueError):
    pass


def _key(draft: AtomDraft) -> str:
    return canonical_json(draft_to_payload(draft))


def _parse(raw: Any, event: MemoryEvent, scope_level: ScopeLevel) -> ExtractedAtom:
    required = {"subject_id", "predicate", "value", "kind", "modality", "source_quote"}
    if not isinstance(raw, Mapping) or not required.issubset(raw):
        raise _InvalidCandidate("missing_candidate_fields")
    if any(raw[name] != scope_level.value for name in ("scope", "scope_level") if name in raw):
        raise _InvalidCandidate("generated_scope_override")
    if raw.get("change_kind", "replace") != "replace" or raw.get("corrects_id") is not None:
        raise _InvalidCandidate("automatic_correction_requires_host_review")
    allowed = required | {
        "scope",
        "scope_level",
        "change_kind",
        "corrects_id",
        "text",
        "confidence",
        "authority",
        "valid_from",
        "valid_to",
        "source_start",
        "source_end",
        "conditions",
        "exceptions",
        "negated",
        "field_evidence",
    }
    if set(raw) - allowed:
        raise _InvalidCandidate("unsupported_candidate_fields")
    try:
        times = {
            name: datetime.fromisoformat(raw[name]) if raw.get(name) is not None else None
            for name in ("valid_from", "valid_to")
        }
        draft = AtomDraft(
            raw["subject_id"],
            raw["predicate"],
            raw["value"],
            # Never render unreviewed model prose as the published fact.
            f"{raw['subject_id']}: {raw['predicate']} = {canonical_json(raw['value'])}",
            raw["source_quote"],
            scope_level=scope_level,
            kind=raw["kind"],
            modality=raw["modality"],
            conditions=raw.get("conditions", ()),
            exceptions=raw.get("exceptions", ()),
            negated=raw.get("negated", False),
            field_evidence=raw.get("field_evidence", ()),
            **times,
        )
        start, end = raw.get("source_start"), raw.get("source_end")
        if start is None and end is None and draft.source_quote:
            first = event.content.find(draft.source_quote)
            if first >= 0 and event.content.find(draft.source_quote, first + 1) == -1:
                start, end = first, first + len(draft.source_quote)
        return ExtractedAtom(draft, start, end)
    except (TypeError, ValueError, OverflowError) as error:
        raise _InvalidCandidate("invalid_candidate_fields") from error


def _gate(
    event: MemoryEvent, candidate: ExtractedAtom, review: AtomReview
) -> tuple[str, tuple[str, ...]]:
    if (
        candidate.source_start is None
        or candidate.source_end > len(event.content)
        or event.content[candidate.source_start : candidate.source_end]
        != candidate.draft.source_quote
    ):
        return "PENDING_VERIFICATION", ("source_span_missing_ambiguous_or_mismatched",)
    if review.faithfulness == "unsupported":
        return "REJECT", ("source_does_not_support_candidate", *review.reasons)
    if review.faithfulness != "supported":
        return "PENDING_VERIFICATION", ("source_semantics_uncertain", *review.reasons)
    if review.retention == "transient":
        return "L0_ONLY", ("transient_information", *review.reasons)
    if review.retention == "uncertain":
        return "PENDING_VERIFICATION", ("reuse_value_uncertain", *review.reasons)
    if review.retention == "session" and candidate.draft.scope_level != ScopeLevel.SESSION:
        return "L0_ONLY", ("retention_does_not_allow_scope_promotion", *review.reasons)
    return "ACCEPT", ("source_review_passed", *review.reasons)


class _ReviewedPolicy(AdmissionPolicy):
    def __init__(self, base: AdmissionPolicy, gates, versions):
        self.base, self.gates, self.versions = base, gates, versions
        self.version = "atom-extraction-v1"

    def config_payload(self):
        return {"version": self.version, "admission": self.base.config_payload(), **self.versions}

    def evaluate(self, event, draft, authority):
        action, reasons = self.base.evaluate(event, draft, authority)
        gate, gate_reasons = self.gates[_key(draft)]
        if gate != "ACCEPT":
            return gate, tuple(dict.fromkeys((*gate_reasons, *reasons)))
        return action, tuple(dict.fromkeys((*reasons, *gate_reasons)))


def extraction_receipt(
    admission: AdmissionReceipt, audit: Mapping[str, Any]
) -> AtomExtractionReceipt:
    decisions = []
    admitted = {item.candidate_id: item for item in admission.decisions}
    for report in audit["reports"]:
        position = report["draft_index"]
        identity = audit["candidates"][position]["id"] if position is not None else None
        decision = admitted.get(identity)
        reasons = tuple(
            dict.fromkeys((*(decision.reasons if decision else ()), *report["reasons"]))
        )
        decisions.append(
            AtomExtractionDecision(
                report["candidate_index"],
                identity,
                decision.action if decision else report["action"],
                reasons,
                report["faithfulness"],
                report["retention"],
                report["source_start"],
                report["source_end"],
            )
        )
    return AtomExtractionReceipt(
        admission,
        tuple(decisions),
        audit["processing_state"],
        tuple(audit["failure_codes"]),
        audit["generator_version"],
        audit["reviewer_version"],
        audit["generation_calls"],
        audit["review_calls"],
        audit["elapsed_ms"],
    )


class AtomExtractionPipeline:
    """Synchronous opt-in pipeline with cached first-result idempotency.

    Adapter versions must change whenever prompts, models or rule bindings do.
    A supplied reviewer judges all semantic fields against the full source;
    its output is not a business-domain fact verification or authentication.
    """

    version = "atom-extraction-v1"

    def __init__(
        self,
        generator: AtomGenerator,
        reviewer: AtomReviewer,
        *,
        scope_level: ScopeLevel = ScopeLevel.SESSION,
        max_candidates: int = 32,
        timeout_seconds: float = 20.0,
    ) -> None:
        for adapter in (generator, reviewer):
            if not isinstance(adapter.version, str) or not 1 <= len(adapter.version) <= 128:
                raise ValueError("each extraction adapter requires a bounded version identifier")
        if type(max_candidates) is not int or not 1 <= max_candidates <= 64:
            raise ValueError("max_candidates must be between 1 and 64")
        if (
            type(timeout_seconds) not in (int, float)
            or not isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 30
        ):
            raise ValueError("timeout_seconds must be finite and within (0, 30]")
        self.generator, self.reviewer = generator, reviewer
        self.scope_level = ScopeLevel(scope_level)
        self.max_candidates, self.timeout_seconds = max_candidates, timeout_seconds

    def config_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generator_version": self.generator.version,
            "reviewer_version": self.reviewer.version,
            "scope_level": self.scope_level.value,
            "max_candidates": self.max_candidates,
            "timeout_seconds": self.timeout_seconds,
        }

    def input_fingerprint(
        self, event: MemoryEvent, *, authority: SourceAuthority, policy: AdmissionPolicy
    ) -> str:
        if is_memory_context(event):
            raise ValueError("memory context requires its original evidence before extraction")
        if len(event.content) > 32_000:
            raise ValueError("event exceeds extraction content limit")
        if not isinstance(event.occurred_at, datetime) or event.occurred_at.utcoffset() is None:
            raise ValueError("event occurred_at must include a timezone")
        event.scope.project(self.scope_level)
        config = self.config_payload()
        capture = capture_annotation(event)
        if capture is not None and len(canonical_json(capture).encode()) > 32_768:
            raise ValueError("capture annotation exceeds metadata budget")
        fingerprint = sha256(
            canonical_json(
                {
                    "scope": asdict(event.scope),
                    "content": event.content,
                    "source_uri": event.source_uri,
                    "actor": event.actor,
                    **({"capture_annotation": capture} if capture is not None else {}),
                    "authority": authority_to_payload(authority),
                    "admission": policy.config_payload(),
                    "extraction": config,
                    "occurred_at": (
                        None
                        if event.metadata.get("atom_implicit_observation") is True
                        else event.occurred_at.astimezone(UTC).isoformat()
                    ),
                }
            ).encode()
        ).hexdigest()
        return fingerprint

    async def prepare(
        self, event: MemoryEvent, *, authority: SourceAuthority, policy: AdmissionPolicy
    ) -> dict[str, Any]:
        """Run generation/review outside storage transactions; return serializable data."""
        config = self.config_payload()
        fingerprint = self.input_fingerprint(event, authority=authority, policy=policy)
        started = perf_counter()
        failures: list[str] = []
        reports: list[dict[str, Any]] = []
        candidates: list[ExtractedAtom] = []
        review_calls = 0
        try:
            async with asyncio.timeout(self.timeout_seconds):
                raw = await self.generator.generate_atoms(event)
            if (
                not isinstance(raw, Sequence)
                or isinstance(raw, (str, bytes))
                or len(raw) > self.max_candidates
            ):
                raise _InvalidCandidate("invalid_or_oversized_generation_batch")
        except Exception as error:
            code = (
                str(error)
                if isinstance(error, _InvalidCandidate)
                else (
                    "generation_timeout" if isinstance(error, TimeoutError) else "generation_failed"
                )
            )
            failures.append(code)
            raw = ()
        for index, item in enumerate(raw):
            report = dict(
                candidate_index=index,
                draft_index=None,
                action="REJECT",
                reasons=[],
                faithfulness="uncertain",
                retention="uncertain",
                source_start=None,
                source_end=None,
            )
            try:
                candidate = _parse(item, event, self.scope_level)
                report.update(
                    draft_index=len(candidates),
                    source_start=candidate.source_start,
                    source_end=candidate.source_end,
                )
                candidates.append(candidate)
            except _InvalidCandidate as error:
                report["reasons"] = [str(error)]
            reports.append(report)
        reviews: Sequence[AtomReview] = ()
        if candidates:
            review_calls = 1
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    reviews = await self.reviewer.review_atoms(event, tuple(candidates))
                if (
                    not isinstance(reviews, Sequence)
                    or len(reviews) != len(candidates)
                    or any(not isinstance(r, AtomReview) for r in reviews)
                    or {r.candidate_index for r in reviews} != set(range(len(candidates)))
                ):
                    raise ValueError("invalid review batch")
            except Exception as error:
                code = "review_timeout" if isinstance(error, TimeoutError) else "review_failed"
                failures.append(code)
                reviews = tuple(
                    AtomReview(i, "uncertain", "uncertain", (code,)) for i in range(len(candidates))
                )
        reviews_by_index = {review.candidate_index: review for review in reviews}
        gates = {}
        for report in reports:
            index = report["draft_index"]
            if index is None:
                continue
            candidate, review = candidates[index], reviews_by_index[index]
            action, reasons = _gate(event, candidate, review)
            report.update(
                action=action,
                reasons=list(reasons),
                faithfulness=review.faithfulness,
                retention=review.retention,
            )
            identity = _key(candidate.draft)
            previous = gates.get(identity)
            if previous is not None and previous != (action, reasons):
                gates[identity] = (
                    "PENDING_VERIFICATION",
                    ("duplicate_candidate_reviews_disagree",),
                )
            else:
                gates[identity] = (action, reasons)
        audit = {
            "input_fingerprint": fingerprint,
            "reports": reports,
            "processing_state": "failed" if failures else "completed",
            "failure_codes": failures,
            "generator_version": config["generator_version"],
            "reviewer_version": config["reviewer_version"],
            "generation_calls": 1,
            "review_calls": review_calls,
            "elapsed_ms": round((perf_counter() - started) * 1000, 3),
        }
        return {
            "drafts": [draft_to_payload(c.draft) for c in candidates],
            "gates": gates,
            "audit": audit,
        }

    async def publish_prepared(
        self, repository, event, prepared, *, authority, policy, unit_of_work=None, retained=False
    ):
        """Publish saved stage data; caller may provide the transaction-B UoW."""
        from .admission import draft_from_payload

        drafts = tuple(draft_from_payload(value) for value in prepared["drafts"])
        audit = prepared["audit"]
        if audit["input_fingerprint"] != self.input_fingerprint(
            event, authority=authority, policy=policy
        ):
            raise ValueError("prepared extraction input changed")
        reviewed = _ReviewedPolicy(
            policy,
            prepared["gates"],
            {
                "generator_version": self.generator.version,
                "reviewer_version": self.reviewer.version,
            },
        )
        return await AdmissionEngine(repository).admit(
            event,
            drafts,
            authority=authority,
            policy=reviewed,
            _extraction_audit=audit,
            _unit_of_work=unit_of_work,
            _retained=retained,
        )

    async def process(
        self,
        repository: AdmissionRepository,
        event: MemoryEvent,
        *,
        authority: SourceAuthority,
        policy: AdmissionPolicy,
    ) -> AtomExtractionReceipt:
        fingerprint = self.input_fingerprint(event, authority=authority, policy=policy)
        key = event.idempotency_key or event.id
        engine = AdmissionEngine(repository)
        cached = await engine.extraction_status(
            event.scope,
            key,
            input_fingerprint=fingerprint,
            duplicate=True,
        )
        if cached is not None:
            return extraction_receipt(*cached)
        prepared = await self.prepare(event, authority=authority, policy=policy)
        admitted = await self.publish_prepared(
            repository,
            event,
            prepared,
            authority=authority,
            policy=policy,
        )
        stored = await engine.extraction_status(
            event.scope,
            key,
            input_fingerprint=fingerprint,
            duplicate=admitted.duplicate,
        )
        if stored is None:
            raise ValueError("extraction source was deleted before receipt delivery")
        return extraction_receipt(*stored)
