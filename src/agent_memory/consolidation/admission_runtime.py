"""Transactional L0 -> L1 admission and its bitemporal state projection.

Candidate snapshots are authoritative for typed atoms. The ordinary Claim table
is a compatibility/index projection; it must never bypass admission on reads.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from ..domain import (
    AdmissionReceipt,
    AtomDecision,
    AtomDraft,
    Claim,
    ClaimStatus,
    EvidenceSupport,
    MemoryEvent,
    MemoryScope,
    Provenance,
    SourceAuthority,
    canonical_json,
    utc_now,
)
from ..lifecycle import CAPTURE_METADATA_KEY, capture_annotation, is_memory_context
from ..ports import AdmissionRepository, AdmissionUnitOfWork
from ..retrieval.atom_state import project_records
from ..serialization import to_jsonable
from .admission import (
    AdmissionPolicy,
    authority_to_payload,
    candidate_id,
    draft_from_payload,
    draft_to_payload,
    slot_key,
)

ATOM_PREFIX = "atom:"
ATOM_EVENT_TYPES = frozenset({"memory.atom", "memory.atom.verification"})
PENDING_ACTIONS = frozenset({"PENDING_VERIFICATION", "CONTESTED"})


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    lf, lt = _time(left["valid_from"]), _time(left.get("valid_to"))
    rf, rt = _time(right["valid_from"]), _time(right.get("valid_to"))
    return (rt is None or lf < rt) and (lt is None or rf < lt)


def _value(payload: dict[str, Any]) -> str:
    return canonical_json(payload["draft"]["value"])


class AdmissionEngine:
    """Explicit host API; no LLM confidence or metadata becomes source authority."""

    def __init__(self, repository: AdmissionRepository) -> None:
        self.repository = repository

    def _require_support(self) -> None:
        if not all(
            callable(getattr(self.repository, name, None))
            for name in (
                "admission_records",
                "admission_snapshot",
                "admission_protected_sources",
                "admission_record",
                "admission_record_versions",
            )
        ):
            raise NotImplementedError("provider does not support typed atom admission")

    async def records_at(
        self,
        scope: MemoryScope,
        known_at: datetime,
    ) -> tuple[dict[str, Any], ...]:
        reader = getattr(self.repository, "admission_records", None)
        if not callable(reader):
            return ()
        self._require_support()
        rows = await self.repository.admission_snapshot(scope)
        selected = []
        for row in rows:
            if "contribution_barrier" in row["payload"]:
                selected.append(row)
                continue
            versions = row["versions"]
            visible = [v for v in versions if _time(v["recorded_at"]) <= known_at]
            future = [v["recorded_at"] for v in versions if _time(v["recorded_at"]) > known_at]
            next_time = min(future, key=_time) if future else None
            if visible:
                version = max(visible, key=lambda v: v["version"])
                selected.append({
                    **row, **version, "_next_recorded_at": next_time,
                    "_snapshot_version": row["version"],
                })
            elif future:
                selected.append(
                    {
                        **row,
                        "payload": {"action": "UNOBSERVED"},
                        "recorded_at": None,
                        "_next_recorded_at": next_time,
                    }
                )
        return tuple(selected)

    async def state(
        self,
        scope: MemoryScope,
        *,
        valid_at: datetime,
        known_at: datetime,
    ) -> tuple[tuple[Claim, ...], dict[str, Any]]:
        for value in (valid_at, known_at):
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError("temporal coordinates must include a timezone")
        return project_records(await self.records_at(scope, known_at), valid_at)

    @staticmethod
    def receipt(
        event_id: str,
        rows: Sequence[dict[str, Any]],
        *,
        duplicate: bool = False,
        initial: bool = True,
    ) -> AdmissionReceipt:
        decisions, claim_ids, pending = [], [], []
        for row in rows:
            value = row["payload"]["initial"] if initial else row["payload"]
            decisions.append(AtomDecision(row["id"], value["action"], tuple(value["reasons"])))
            if value.get("claim_id"):
                claim_ids.append(value["claim_id"])
            if value["action"] in PENDING_ACTIONS:
                pending.append(row["id"])
        return AdmissionReceipt(
            event_id,
            tuple(r["id"] for r in rows),
            tuple(claim_ids),
            tuple(decisions),
            tuple(pending),
            duplicate,
        )

    async def admit(
        self,
        event: MemoryEvent,
        drafts: Sequence[AtomDraft],
        *,
        authority: SourceAuthority,
        policy: AdmissionPolicy,
        _extraction_audit: dict[str, Any] | None = None,
        _unit_of_work=None,
        _retained: bool = False,
        _publication_id: str | None = None,
        _contribution_write: bool = False,
    ) -> AdmissionReceipt:
        # Keep fingerprinting and transactional admission on the same owned
        # inputs, even if callers mutate their objects while we await a lock.
        event, drafts, authority, policy, _extraction_audit = deepcopy(
            (event, tuple(drafts), authority, policy, _extraction_audit)
        )
        if _publication_id is not None and (not _retained or not isinstance(_publication_id, str)):
            raise ValueError("publication identity requires retained source admission")

        def identity_for(draft):
            identity = candidate_id(event, draft)
            if _publication_id is None:
                return identity
            return (
                "candidate:"
                + sha256(canonical_json([identity, _publication_id]).encode()).hexdigest()
            )

        source_family = event.metadata.get("_retention", {}).get("document_id", event.id)
        self._require_support()
        if is_memory_context(event):
            raise ValueError("memory context cannot be admitted as independent evidence")
        if not (0 if _extraction_audit is not None else 1) <= len(drafts) <= 64:
            raise ValueError("admission requires between 1 and 64 atoms")
        if len(event.content) > 32_000:
            raise ValueError("event exceeds admission content limit")
        if not isinstance(event.occurred_at, datetime) or event.occurred_at.utcoffset() is None:
            raise ValueError("event occurred_at must include a timezone")
        event = replace(event, occurred_at=event.occurred_at.astimezone(UTC))
        # An unavailable scope cannot provide a stable identity; fail atomically.
        scopes = [event.scope.project(d.scope_level) for d in drafts]
        capture = capture_annotation(event)
        if capture is not None and len(canonical_json(capture).encode()) > 32_768:
            raise ValueError("capture annotation exceeds metadata budget")
        fingerprint = sha256(
            canonical_json(
                {
                    "content": event.content,
                    "drafts": [draft_to_payload(d) for d in drafts],
                    "authority": authority_to_payload(authority),
                    "policy": policy.config_payload(),
                    "source_uri": event.source_uri,
                    "actor": event.actor,
                    **({"capture_annotation": capture} if capture is not None else {}),
                    "occurred_at": (
                        None
                        if event.metadata.get("atom_implicit_observation") is True
                        else event.occurred_at.isoformat()
                    ),
                }
            ).encode()
        ).hexdigest()
        metadata = {"atom_fingerprint": fingerprint}
        if capture is not None:
            metadata["lifecycle"] = {"payload": {CAPTURE_METADATA_KEY: capture}}
        if _extraction_audit is not None:
            metadata["atom_extraction"] = {
                **_extraction_audit,
                "candidates": [
                    {"id": identity_for(draft), "scope": to_jsonable(scope)}
                    for draft, scope in zip(drafts, scopes, strict=True)
                ],
            }
            if len(canonical_json(metadata).encode()) > 128_000:
                raise ValueError("extraction audit exceeds metadata budget")
        event = replace(
            event,
            event_type="memory.atom",
            idempotency_key=event.idempotency_key or event.id,
            metadata=metadata,
            content_hash="",
        )
        if _retained and (_unit_of_work is None or _extraction_audit is None):
            raise ValueError("retained publication requires an enclosing transaction and stage")
        context = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with context as uow:
            for scope in sorted(
                {s.partition_key(): s for s in [event.scope, *scopes]}.values(),
                key=lambda s: s.partition_key(),
            ):
                await uow.lock_admission_scope(scope)
            stored = await uow.find_event_by_idempotency(event.scope, event.idempotency_key)
            if _retained:
                if (
                    stored is None
                    or stored.id != event.id
                    or "_retention" not in stored.metadata
                    or stored.content != event.content
                    or not await uow.events_exist(event.scope, (event.id,))
                ):
                    raise ValueError("retained source is unavailable or changed")
            elif stored:
                if _extraction_audit is not None:
                    receipt, _ = await self._extraction_receipt(
                        uow,
                        stored,
                        _extraction_audit["input_fingerprint"],
                        duplicate=True,
                    )
                    return receipt
                if stored.metadata.get("atom_fingerprint") != fingerprint:
                    raise ValueError("idempotency key reused with different atom input")
                rows = []
                for draft, scope in zip(drafts, scopes, strict=True):
                    row = await uow.get_admission_record(scope, candidate_id(stored, draft))
                    if row is None:
                        raise ValueError("admission input was deleted or is incomplete")
                    if row["id"] not in {r["id"] for r in rows}:
                        rows.append(row)
                return self.receipt(stored.id, rows, duplicate=True)
            if not _retained:
                await uow.append_event(event)
            rows: list[dict[str, Any]] = []
            existing: dict[str, list[dict[str, Any]]] = {}
            for draft, scope in zip(drafts, scopes, strict=True):
                identity = identity_for(draft)
                if identity in {r["id"] for r in rows}:
                    continue
                key = slot_key(event.scope, draft)
                if key not in existing:
                    existing[key] = list(await uow.list_admission_records(scope, key))
                barrier_reader = getattr(uow, "list_admission_barriers", None)
                barriers = await barrier_reader(scope, key) if callable(barrier_reader) else ()
                if not _contribution_write and (barriers or any(
                    r["payload"].get("contribution") for r in existing[key]
                )):
                    raise ValueError("managed slot requires versioned contribution operations")
                action, reasons = policy.evaluate(event, draft, authority)
                payload: dict[str, Any] = {
                    "draft": draft_to_payload(draft),
                    "authority": authority_to_payload(authority),
                    "action": action,
                    "reasons": list(reasons),
                    "claim_id": None,
                    "claim": None,
                    "valid_from": (draft.valid_from or event.occurred_at).isoformat(),
                    "valid_to": draft.valid_to.isoformat() if draft.valid_to else None,
                    "source_event_ids": [event.id],
                    "source_family": source_family,
                    "interpretation_request": _publication_id,
                    "evidence": [],
                    "evidence_qualified": action == "ACCEPT",
                    "decisions": [],
                    "corrects": draft.corrects_id,
                    "base_candidate_id": None,
                    "valid_time_basis": "explicit" if draft.valid_from else "observation",
                }
                if _extraction_audit is not None:
                    matching_indexes = {
                        i for i, item in enumerate(drafts) if identity_for(item) == identity
                    }
                    payload["extraction"] = {
                        "generator_version": _extraction_audit["generator_version"],
                        "reviewer_version": _extraction_audit["reviewer_version"],
                        "reports": [
                            report
                            for report in _extraction_audit["reports"]
                            if report["draft_index"] in matching_indexes
                        ],
                    }
                rows.append(
                    {
                        "id": identity,
                        "event_id": event.id,
                        "slot_key": key,
                        "scope": to_jsonable(scope),
                        "payload": payload,
                        "version": 0,
                        "recorded_at": utc_now().isoformat(),
                    }
                )
            # Evaluate the entire batch before any Claim is published.
            for row in rows:
                if row["payload"]["action"] == "ACCEPT":
                    self._reconcile(row, existing[row["slot_key"]], rows)
            for row in rows:
                payload = row["payload"]
                draft = draft_from_payload(payload["draft"])
                evidence = EvidenceSupport(
                    event.id,
                    authority.kind,
                    support_kind="interval" if draft.valid_from else "point",
                    support_at=None if draft.valid_from else event.occurred_at,
                    support_from=draft.valid_from,
                    support_to=draft.valid_to if draft.valid_from else None,
                    recorded_at=utc_now(),
                )
                if payload["evidence_qualified"]:
                    payload["evidence"] = [
                        {
                            **to_jsonable(evidence),
                            "source_quote": draft.source_quote,
                            "authority": authority_to_payload(authority),
                        }
                    ]
                if payload["action"] == "ACCEPT":
                    await self._publish(uow, row, event)
                self._decision(payload, policy)
                payload["initial"] = {
                    name: payload[name] for name in ("action", "reasons", "claim_id")
                }
                await uow.save_admission_record(
                    MemoryScope(**row["scope"]), row["id"], event.id, row["slot_key"], payload, 0
                )
            return self.receipt(event.id, rows)

    async def extraction_status(
        self, scope: MemoryScope, idempotency_key: str, *,
        input_fingerprint: str | None = None, duplicate: bool = False,
    ) -> tuple[AdmissionReceipt, dict[str, Any]] | None:
        """Read the saved first result without rerunning an external generator."""
        self._require_support()
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(scope)
            stored = await uow.find_event_by_idempotency(scope, idempotency_key)
            if stored is None:
                return None
            return await self._extraction_receipt(
                uow, stored, input_fingerprint, duplicate=duplicate,
            )

    async def _extraction_receipt(
        self, uow: AdmissionUnitOfWork, stored: MemoryEvent,
        input_fingerprint: str | None, *, duplicate: bool,
    ) -> tuple[AdmissionReceipt, dict[str, Any]]:
        audit = stored.metadata.get("atom_extraction")
        if not isinstance(audit, dict) or (
            input_fingerprint is not None and audit["input_fingerprint"] != input_fingerprint
        ):
            raise ValueError("idempotency key reused with different extraction input")
        rows = []
        for reference in audit["candidates"]:
            row = await uow.get_admission_record(MemoryScope(**reference["scope"]), reference["id"])
            if row is None:
                raise ValueError("extraction candidate was deleted or is incomplete")
            if row["id"] not in {r["id"] for r in rows}:
                rows.append(row)
        return self.receipt(stored.id, rows, duplicate=duplicate), audit

    @staticmethod
    def _decision(payload: dict[str, Any], policy: AdmissionPolicy) -> None:
        payload["decisions"].append(
            {
                "action": payload["action"],
                "reasons": payload["reasons"],
                "recorded_at": utc_now().isoformat(),
                "policy_version": policy.version,
                "policy": policy.config_payload(),
            }
        )

    @staticmethod
    def _reconcile(
        row: dict[str, Any], existing: list[dict[str, Any]], batch: Sequence[dict[str, Any]]
    ) -> None:
        payload = row["payload"]
        others = [
            r
            for r in [*existing, *batch]
            if r["id"] != row["id"] and r["slot_key"] == row["slot_key"]
        ]
        accepted = [r for r in existing if r["payload"]["action"] == "ACCEPT"]
        corrected = {r["payload"].get("corrects") for r in accepted}
        accepted = [r for r in accepted if r["id"] not in corrected]
        draft = payload["draft"]
        if draft["change_kind"] == "correct":
            target = next((r for r in accepted if r["id"] == draft["corrects_id"]), None)
            if target is None or target["payload"]["draft"]["change_kind"] == "temporary_override":
                payload.update(action="PENDING_VERIFICATION", reasons=["invalid_correction_target"])
                return
            competing = [
                r
                for r in batch
                if r["id"] != row["id"] and r["payload"].get("corrects") == draft["corrects_id"]
            ]
            if competing:
                payload.update(
                    action="PENDING_VERIFICATION", reasons=["multiple_corrections_in_batch"]
                )
                return
        elif draft["change_kind"] == "temporary_override":
            claims, info = project_records(existing, _time(payload["valid_from"]))
            base = next(
                (r for r in accepted if any(c.id == r["payload"]["claim_id"] for c in claims)), None
            )
            if (
                base is None
                or info["conflicts"]
                or base["payload"]["draft"]["change_kind"] == "temporary_override"
                or any(
                    r["payload"]["draft"]["change_kind"] == "temporary_override"
                    and (r in batch or r["payload"]["action"] in {"ACCEPT", "CONTESTED"})
                    and _overlap(payload, r["payload"])
                    for r in others
                )
            ):
                payload.update(
                    action="PENDING_VERIFICATION", reasons=["override_requires_unique_base"]
                )
                return
            payload["base_candidate_id"] = base["id"]
            return
        conflicts = [
            r
            for r in others
            if r["payload"]["action"] in {"ACCEPT", "CONTESTED"}
            and r["id"] != payload.get("corrects")
            and r["id"] not in corrected
            and r["payload"]["draft"]["change_kind"] != "temporary_override"
            and _overlap(payload, r["payload"])
            and _value(payload) != _value(r["payload"])
            and (draft["valid_from"] is None or payload["valid_from"] == r["payload"]["valid_from"])
        ]
        if conflicts:
            payload.update(
                action="CONTESTED",
                reasons=["overlapping_single_state_values"],
                conflict_with=[r["id"] for r in conflicts],
            )

    @staticmethod
    async def _publish(
        uow: AdmissionUnitOfWork,
        row: dict[str, Any],
        event: MemoryEvent,
    ) -> None:
        payload = row["payload"]
        scope = MemoryScope(**row["scope"])
        previous = await uow.find_current_claim(scope, row["slot_key"])
        target_id = None
        if payload.get("corrects"):
            target = await uow.get_admission_record(scope, payload["corrects"])
            target_id = target["payload"]["claim_id"] if target else None
        claim = Claim(
            id="atom-claim:" + sha256(row["id"].encode()).hexdigest(),
            scope=scope,
            key=row["slot_key"],
            value=payload["draft"]["value"],
            text=payload["draft"]["text"],
            confidence=1.0,
            importance=0.5,
            status=ClaimStatus.ACTIVE,
            provenance=Provenance(
                tuple(payload["source_event_ids"]),
                extractor="host-typed-atom",
                provider="atom-admission-v1",
                source_uri=event.source_uri,
                created_at=utc_now(),
            ),
            valid_from=_time(payload["valid_from"]),
            valid_to=_time(payload["valid_to"]),
            created_at=utc_now(),
            corrects_id=target_id,
            version=previous.version + 1 if previous else 1,
            supersedes=previous.id if previous else None,
        )
        if previous:
            await uow.replace_current_claim(previous, claim)
        else:
            await uow.save_claim(claim)
        payload.update(claim_id=claim.id, claim=to_jsonable(claim))

    async def retract(self, scope, identity, **options):
        from .retraction import retract

        return await retract(self, scope, identity, **options)

    async def resolve(
        self,
        scope: MemoryScope,
        identity: str,
        *,
        event: MemoryEvent,
        authority: SourceAuthority,
        policy: AdmissionPolicy,
        expected_version: int,
        accept: bool,
        source_quote: str,
        support_from: datetime | None = None,
        support_to: datetime | None = None,
    ) -> AdmissionReceipt:
        """CAS review of a pending candidate. Conflicts require explicit interval proof.

        The host authenticates the reviewer and source. No quote matching is
        presented as automated semantic verification.
        """
        self._require_support()
        if type(accept) is not bool or type(expected_version) is not int or expected_version < 1:
            raise ValueError("review requires a boolean decision and positive expected_version")
        visible = await self.repository.admission_record(scope, identity)
        if visible is None:
            raise ValueError("candidate is missing or outside scope")
        target_scope = MemoryScope(**visible["scope"])
        if event.scope != scope:
            raise ValueError("verification event must belong to the caller scope")
        if len(event.content) > 32_000:
            raise ValueError("verification event exceeds content limit")
        # Validate range and times even on a rejection.
        evidence = EvidenceSupport(
            event.id,
            authority.kind,
            relation="supports" if accept else "refutes",
            support_kind="interval" if support_from is not None else "point",
            support_at=event.occurred_at if support_from is None else None,
            support_from=support_from,
            support_to=support_to,
            recorded_at=utc_now(),
        )
        support_from, support_to = evidence.support_from, evidence.support_to
        event = replace(
            event,
            event_type="memory.atom.verification",
            metadata={},
            content_hash="",
            occurred_at=event.occurred_at.astimezone(UTC),
        )
        async with self.repository.unit_of_work() as uow:
            for item in sorted([scope, target_scope], key=lambda s: s.partition_key()):
                await uow.lock_admission_scope(item)
            row = await uow.get_admission_record(target_scope, identity)
            if row is None or row["version"] != expected_version:
                raise ValueError("candidate version changed or was deleted")
            payload = row["payload"]
            previous_action = payload["action"]
            if payload["action"] not in PENDING_ACTIONS:
                raise ValueError("only pending or contested candidates can be resolved")
            if payload.get("contribution"):
                raise ValueError("managed slot requires versioned contribution operations")
            if payload.get("qualification"):
                raise ValueError("contextual candidates require versioned field qualification")
            original = draft_from_payload(payload["draft"])
            checked = replace(
                original,
                source_quote=source_quote,
                valid_from=support_from or event.occurred_at,
                valid_to=support_to,
            )
            action, reasons = policy.evaluate(event, checked, authority)
            if action != "ACCEPT":
                raise ValueError("verification source failed admission: " + ", ".join(reasons))
            if original.change_kind != "replace":
                raise ValueError(
                    "submit a corrected atom for correction/override structural errors"
                )
            peers = list(await uow.list_admission_records(target_scope, row["slot_key"]))
            if accept and payload["action"] == "CONTESTED":
                start, end = _time(payload["valid_from"]), _time(payload["valid_to"])
                if support_from != start or support_to != end:
                    raise ValueError("conflict resolution requires evidence for its exact interval")
            await uow.append_event(event)
            payload["source_event_ids"].append(event.id)
            qualified_evidence = {
                **to_jsonable(evidence),
                "source_quote": source_quote,
                "authority": authority_to_payload(authority),
            }
            payload["evidence"].append(qualified_evidence)
            payload["authority"] = authority_to_payload(authority)
            payload["action"] = "ACCEPT" if accept else "REJECT"
            payload["reasons"] = ["host_verified" if accept else "host_rejected"]
            if accept:
                payload["valid_from"] = (support_from or event.occurred_at).isoformat()
                payload["valid_to"] = support_to.isoformat() if support_to else None
                if previous_action == "PENDING_VERIFICATION":
                    # Evidence qualification does not resolve a conflict by itself.
                    # A point observation has no explicit change boundary.
                    checked_draft = replace(checked, valid_from=support_from)
                    saved_draft = payload["draft"]
                    payload["draft"] = draft_to_payload(checked_draft)
                    self._reconcile(row, peers, ())
                    payload["draft"] = saved_draft
                if payload["action"] == "ACCEPT":
                    payload["resolved_at"] = utc_now().isoformat()
                # A chosen value can resolve only disputes with identical temporal
                # coverage. Other overlaps remain explicitly contested.
                for peer in peers:
                    other = peer["payload"]
                    if (
                        peer["id"] != identity
                        and previous_action == "CONTESTED"
                        and other["action"] == "CONTESTED"
                        and other["valid_from"] == payload["valid_from"]
                        and other["valid_to"] == payload["valid_to"]
                    ):
                        agrees = _value(other) == _value(payload)
                        other.update(
                            action="L0_ONLY" if agrees else "REJECT",
                            reasons=[
                                "duplicate_of_resolved_candidate"
                                if agrees
                                else "conflict_resolved_by_host"
                            ],
                        )
                        other["source_event_ids"].append(event.id)
                        other["evidence"].append(
                            {
                                **qualified_evidence,
                                "relation": "supports" if agrees else "refutes",
                            }
                        )
                        self._decision(other, policy)
                        await uow.save_admission_record(
                            target_scope,
                            peer["id"],
                            peer["event_id"],
                            peer["slot_key"],
                            other,
                            peer["version"],
                        )
                if payload["action"] == "ACCEPT":
                    await self._publish(uow, row, event)
            self._decision(payload, policy)
            await uow.save_admission_record(
                target_scope, identity, row["event_id"], row["slot_key"], payload, expected_version
            )
            return self.receipt(event.id, [row], initial=False)
