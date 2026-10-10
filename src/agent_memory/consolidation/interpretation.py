"""Whole-source interpretation review and atomic contribution reconciliation.

The old/new union is reviewed explicitly. Missing generation output is never
negative evidence. Publication shares the caller's fenced transaction.
"""

import asyncio
from copy import deepcopy

from .. import domain
from ..domain import AtomReview, EvidenceSupport, ExtractedAtom, canonical_json
from ..operations.reprocessing import checked_records
from ..operations.retention import RetentionError
from ..serialization import to_jsonable
from .admission import authority_to_payload, draft_from_payload, draft_to_payload, slot_key
from .admission_runtime import AdmissionEngine
from .atom_extraction import _gate
from .source_audit import audit_review_required


def signature(draft):
    return canonical_json(draft_to_payload(draft))


async def prepare_reconciliation(pipeline, source, prepared, records, contract, policy, authority):
    if any(report["draft_index"] is None for report in prepared["audit"]["reports"]):
        raise RetentionError("reprocessing_incomplete")
    dispositions = []
    review_audit = []
    if contract["mode"] == "additive":
        dispositions = [
            {
                "candidate_id": r["id"],
                "action": "carry_forward",
                "reasons": ["additive_preserves_previous_contribution"],
            }
            for r in records
        ]
    elif records:
        candidates = []
        for row in records:
            draft = draft_from_payload(row["payload"]["draft"])
            start = source.content.find(draft.source_quote) if draft.source_quote else -1
            unique = start >= 0 and source.content.find(draft.source_quote, start + 1) == -1
            candidates.append(
                ExtractedAtom(
                    draft,
                    start if unique else None,
                    start + len(draft.source_quote) if unique else None,
                )
            )
        try:
            async with asyncio.timeout(pipeline.timeout_seconds):
                reviews = await pipeline.reviewer.review_atoms(source, tuple(candidates))
        except Exception:
            raise RetentionError("reprocessing_review_failed") from None
        if (
            not isinstance(reviews, (tuple, list))
            or len(reviews) != len(records)
            or any(not isinstance(r, AtomReview) for r in reviews)
            or {r.candidate_index for r in reviews} != set(range(len(records)))
        ):
            raise RetentionError("reprocessing_review_incomplete")
        by_index = {review.candidate_index: review for review in reviews}
        review_audit = [
            {"candidate_id": row["id"], **to_jsonable(by_index[index])}
            for index, row in enumerate(records)
        ]
        for index, (row, candidate) in enumerate(zip(records, candidates, strict=True)):
            review = by_index[index]
            action, reasons = policy.evaluate(source, candidate.draft, authority)
            gate, gate_reasons = _gate(source, candidate, review)
            if review.faithfulness == "unsupported":
                disposition, why = (
                    "withdraw_source_support",
                    ("source_semantics_unsupported", *review.reasons),
                )
            elif gate == "L0_ONLY" or action in {"L0_ONLY", "REJECT"}:
                disposition, why = (
                    "withdraw_source_support",
                    ("target_policy_disallows", *reasons, *gate_reasons),
                )
            elif gate == "ACCEPT" and action == "ACCEPT":
                if audit_review_required(row["payload"]):
                    if not contract["allow_pending"]:
                        raise RetentionError("reprocessing_needs_resolution")
                    disposition, why = (
                        "qualification_pending",
                        ("source_audit_recovery_requires_host_verification",),
                    )
                else:
                    disposition, why = (
                        "retain",
                        ("source_review_and_target_policy_passed", *review.reasons),
                    )
            elif contract["allow_pending"]:
                disposition, why = "qualification_pending", (*reasons, *gate_reasons)
            else:
                raise RetentionError("reprocessing_needs_resolution")
            dispositions.append(
                {
                    "candidate_id": row["id"],
                    "action": disposition,
                    "reasons": list(dict.fromkeys(why)),
                }
            )
    held = {row["id"] for row in records if audit_review_required(row["payload"])}
    kept = {
        d["candidate_id"] for d in dispositions
        if d["action"] in {"retain", "carry_forward"}
        or (d["action"] == "qualification_pending" and d["candidate_id"] in held)
    }
    keep_signatures = {
        signature(draft_from_payload(r["payload"]["draft"])) for r in records if r["id"] in kept
    }
    withdrawn = {
        d["candidate_id"] for d in dispositions if d["action"] == "withdraw_source_support"
    }
    withdrawn_signatures = {
        signature(draft_from_payload(r["payload"]["draft"]))
        for r in records
        if r["id"] in withdrawn
    }
    new_drafts = [draft_from_payload(d) for d in prepared["drafts"]]
    if any(d.change_kind != "replace" for d in new_drafts):
        raise RetentionError("interpretation_capability_unsupported")
    if withdrawn_signatures & {signature(d) for d in new_drafts}:
        raise RetentionError("reprocessing_review_inconsistent")
    if withdrawn and any(
        slot_key(source.scope, d) not in {r["slot_key"] for r in records} for d in new_drafts
    ):
        raise RetentionError("interpretation_capability_unsupported")
    indexes = {
        i: n
        for n, i in enumerate(
            i for i, draft in enumerate(new_drafts) if signature(draft) not in keep_signatures
        )
    }
    filtered = deepcopy(prepared)
    filtered["drafts"] = [prepared["drafts"][i] for i in indexes]
    filtered["audit"]["reports"] = [
        {**r, "draft_index": indexes[r["draft_index"]]}
        for r in prepared["audit"]["reports"]
        if r["draft_index"] in indexes
    ]
    # Preserve the complete generation audit as well as the filtered publication input.
    filtered["reconciliation"] = {
        "dispositions": dispositions,
        "complete": True,
        "generation_audit": prepared["audit"],
        "old_review": review_audit,
        "old_review_calls": int(bool(records) and contract["mode"] == "replace_interpretation"),
    }
    return filtered


def record_decision(payload, request_id, action, reasons):
    payload.update(action=action, reasons=reasons)
    payload["decisions"].append(
        {
            "action": action,
            "reasons": reasons,
            "recorded_at": domain.utc_now().isoformat(),
            "request_id": request_id,
        }
    )


async def activate(uow, repository, source, request, prepared, pipeline, policy, authority):
    contract = request["reprocessing"]
    records = await checked_records(
        uow, source.scope, source.id, contract["expected_head_generation"], request["base_versions"]
    )
    plan = prepared["reconciliation"]
    dispositions = plan["dispositions"]
    if (
        not plan["complete"]
        or len(dispositions) != len(records)
        or {d["candidate_id"] for d in dispositions} != {r["id"] for r in records}
    ):
        raise RetentionError("reprocessing_incomplete")
    by_id = {r["id"]: r for r in records}
    survivors = []
    for item in dispositions:
        row = by_id[item["candidate_id"]]
        if item["action"] in {"retain", "carry_forward"}:
            survivors.append(row)
            continue
        if item["action"] not in {"withdraw_source_support", "qualification_pending"}:
            raise RetentionError("reprocessing_incomplete")
        action = (
            "WITHDRAWN" if item["action"] == "withdraw_source_support" else "PENDING_VERIFICATION"
        )
        record_decision(row["payload"], request["request_id"], action, item["reasons"])
        row["version"] = await uow.save_admission_record(
            source.scope,
            row["id"],
            row["event_id"],
            row["slot_key"],
            row["payload"],
            row["version"],
        )
        if action == "PENDING_VERIFICATION":
            survivors.append(row)
    new = await pipeline.publish_prepared(
        repository,
        source,
        prepared,
        authority=authority,
        policy=policy,
        unit_of_work=uow,
        retained=True,
        publication_id=request["request_id"],
    )
    new_records = [
        await uow.get_admission_record(source.scope, identity) for identity in new.candidate_ids
    ]
    # Requalified old contributions use their old identities and evidence family.
    for item in dispositions:
        if item["action"] != "retain":
            continue
        row = by_id[item["candidate_id"]]
        payload = row["payload"]
        payload.update(action="ACCEPT", reasons=item["reasons"])
        peers = list(await uow.list_admission_records(source.scope, row["slot_key"]))
        AdmissionEngine._reconcile(row, peers, ())
        record_decision(payload, request["request_id"], payload["action"], payload["reasons"])
        payload["authority"] = authority_to_payload(authority)
        if not payload["evidence_qualified"]:
            draft = draft_from_payload(payload["draft"])
            evidence = EvidenceSupport(
                source.id,
                authority.kind,
                support_kind="interval" if draft.valid_from else "point",
                support_at=None if draft.valid_from else source.occurred_at,
                support_from=draft.valid_from,
                support_to=draft.valid_to if draft.valid_from else None,
                recorded_at=domain.utc_now(),
            )
            payload["evidence"] = [
                {
                    **to_jsonable(evidence),
                    "source_quote": draft.source_quote,
                    "authority": authority_to_payload(authority),
                }
            ]
            payload["evidence_qualified"] = True
        if payload["action"] == "ACCEPT" and payload["claim_id"] is None:
            await AdmissionEngine._publish(uow, row, source)
        row["version"] = await uow.save_admission_record(
            source.scope, row["id"], row["event_id"], row["slot_key"], payload, row["version"]
        )
    active = [
        r
        for r in [*survivors, *new_records]
        if r["payload"]["action"] not in {"REJECT", "L0_ONLY", "WITHDRAWN"}
    ]
    if len(active) > 64:
        raise RetentionError("interpretation_capacity")
    pending = any(r["payload"]["action"] in {"PENDING_VERIFICATION", "CONTESTED"} for r in active)
    if pending and not contract["allow_pending"] and contract["mode"] == "replace_interpretation":
        raise RetentionError("reprocessing_needs_resolution")
    generation = await uow.retention_head_put(
        source.scope,
        "interpretation",
        source.id,
        {
            "active_ids": [r["id"] for r in active],
            "request_id": request["request_id"],
            "stream": "primary",
        },
        contract["expected_head_generation"],
    )
    visible = [
        {
            **r,
            "payload": {
                **r["payload"],
                "claim_id": r["payload"]["claim_id"]
                if r["payload"]["action"] == "ACCEPT"
                else None,
            },
        }
        for r in active
    ]
    receipt = AdmissionEngine.receipt(source.id, visible, initial=False)
    return receipt, {
        "generation": generation,
        "activation_state": "activated_with_pending" if pending else "activated",
        "generation_audit": plan["generation_audit"],
        "old_review": plan["old_review"],
        "dispositions": dispositions,
        "new_candidate_ids": list(new.candidate_ids),
    }
