"""Condition-first exact-scope projection, with current source guards on historical reads."""

from datetime import datetime
from hashlib import sha256

from ..conditions import Condition, ProjectionPolicy, QueryContext, applicability
from ..domain import canonical_json
from ..evidence_support import SupportRange, evaluate_support, intersect, union
from ..operations.source_revisions import source_available_at
from ..serialization import to_jsonable
from .atom_state import project_records


def compose(items, policy):
    active = [item for item in items if item["applicable"] is not False]
    if policy.mode == "ordered_override":
        graph = {}
        for low, high in policy.precedence:
            graph.setdefault(low, set()).add(high)

        def above(low, high):
            pending, seen = list(graph.get(low, ())), set()
            while pending:
                node = pending.pop()
                if node == high:
                    return True
                if node not in seen:
                    seen.add(node)
                    pending.extend(graph.get(node, ()))
            return False

        # Only a definitely applicable higher domain can exclude a lower one.
        active = [
            i
            for i in active
            if not any(
                j["applicable"] is True and above(i["applicability_id"], j["applicability_id"])
                for j in active
            )
        ]
    if not active:
        return "unknown", [], ["no_applicable_interpretation"]
    if any(i["applicable"] is None for i in active):
        return "unknown", [], ["context_incomplete"]
    if any(i["status"] == "contested" for i in active):
        return "contested", [], ["applicable_domain_contested"]
    if any(i["status"] != "resolved" for i in active):
        return "unknown", [], ["applicable_support_incomplete"]
    if len({canonical_json(i["value"]) for i in active}) != 1:
        return "ambiguous", [], ["incomparable_applicable_values"]
    return "resolved", active, []


async def query(service, context, *, predicate, policy):
    if (
        not isinstance(context, QueryContext)
        or context.scope != service.scope
        or context.principal != service.principal
    ):
        raise ValueError("query context is outside authenticated host routing")
    if not isinstance(policy, ProjectionPolicy) or context.purpose != policy.purpose:
        raise ValueError("query purpose does not match host policy")
    rows = await service.engine.records_at(service.scope, context.known_at)
    rows = [
        r
        for r in rows
        if r["scope"] == to_jsonable(service.scope)
        and r["payload"].get("draft", {}).get("subject_id") == context.subject_id
        and r["payload"]["draft"]["predicate"] == predicate
    ]
    items = []
    inputs = [
        [
            r["id"],
            r["version"],
            r["recorded_at"],
            sha256(canonical_json(r["payload"]).encode()).hexdigest(),
        ]
        for r in rows
    ]
    async with service.engine.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(service.scope)
        # Validate the selected versions against current deletion state. This is
        # also the linearization point for all source guards in this response.
        for row in rows:
            current = await uow.get_admission_record(service.scope, row["id"])
            if current is None:
                raise ValueError("query snapshot invalidated by deletion; retry with a new context")
        ordinary = [r for r in rows if not r["payload"].get("qualification")]
        claims, information = project_records(ordinary, context.valid_at)
        for claim in claims:
            items.append(
                {
                    "applicability_id": "global",
                    "applicable": True,
                    "status": "resolved",
                    "value": claim.value,
                    "candidate_ids": [information["atom_support"][claim.id]["candidate_id"]],
                    "support": information["atom_support"][claim.id],
                    "ranges": [],
                }
            )
        for conflict in information["conflicts"]:
            items.append(
                {
                    "applicability_id": "global",
                    "applicable": True,
                    "status": "contested",
                    "candidate_ids": conflict["candidate_ids"],
                }
            )
        qualified = [
            r
            for r in rows
            if r["payload"].get("qualification")
            and r["payload"]["action"] == "PENDING_VERIFICATION"
        ]
        groups = {}
        for row in qualified:
            q = row["payload"]["qualification"]
            if q["policy_sha256"] != policy.fingerprint:
                return {
                    "status": "history_unavailable",
                    "reasons": ["projection_policy_version_mismatch"],
                    "context_hash": context.context_hash,
                    "interpretations": [],
                }
            # Same domain must have the same condition meaning. Reject instead
            # of using registration/write order as an applicability policy.
            binding = canonical_json([q["conditions"], q["exceptions"]])
            domain = q["applicability_id"]
            if domain in groups and groups[domain][0] != binding:
                return {
                    "status": "unsupported",
                    "reasons": ["applicability_definition_changed"],
                    "context_hash": context.context_hash,
                    "interpretations": [],
                }
            groups.setdefault(domain, (binding, []))[1].append(row)
        for domain, (_, members) in groups.items():
            eligible = [
                r
                for r in members
                if datetime.fromisoformat(r["payload"]["valid_from"]) <= context.valid_at
            ]
            if not eligible:
                continue
            boundary = max(datetime.fromisoformat(r["payload"]["valid_from"]) for r in eligible)
            latest = [
                r
                for r in eligible
                if datetime.fromisoformat(r["payload"]["valid_from"]) == boundary
            ]
            domain_items = []
            for row in latest:
                payload, q = row["payload"], row["payload"]["qualification"]
                applies = applicability(
                    [Condition(**c) for c in q["conditions"]],
                    [Condition(**c) for c in q["exceptions"]],
                    context,
                )
                available = set()
                for link in q["links"]:
                    source = await uow.get_source_event(
                        service.scope, link["span"]["source_event_id"]
                    )
                    if source is None or source.content_hash != link["source_sha256"]:
                        continue
                    if "_retention" in source.metadata and not await source_available_at(
                        uow, source, context.known_at
                    ):
                        continue
                    available.add(source.id)
                ranges, proofs = evaluate_support(q, available)
                future_starts = [
                    datetime.fromisoformat(r["payload"]["valid_from"])
                    for r in members
                    if datetime.fromisoformat(r["payload"]["valid_from"]) > context.valid_at
                ]
                ends = [
                    *future_starts,
                    *([datetime.fromisoformat(payload["valid_to"])] if payload["valid_to"] else []),
                ]
                candidate_range = SupportRange(
                    datetime.fromisoformat(payload["valid_from"]), min(ends) if ends else None
                )
                ranges = union(
                    [r for value in ranges if (r := intersect(value, candidate_range)) is not None]
                )
                supported = any(r.contains(context.valid_at) for r in ranges)
                item = {
                    "applicability_id": domain,
                    "applicable": applies,
                    "status": "resolved" if supported else "unknown",
                    "value": payload["draft"]["value"],
                    "candidate_ids": [row["id"]],
                    "ranges": to_jsonable(ranges),
                    "evidence_link_ids": sorted(
                        {i for r, ids in proofs if r.contains(context.valid_at) for i in ids}
                    ),
                }
                domain_items.append(item)
            status, selected, _ = compose(
                domain_items, ProjectionPolicy(policy.revision, policy.purpose)
            )
            if status == "ambiguous":
                status = "contested"
            items.append(
                {
                    "applicability_id": domain,
                    "applicable": domain_items[0]["applicable"],
                    "status": status,
                    "value": selected[0]["value"] if selected else None,
                    "candidate_ids": [i for v in domain_items for i in v["candidate_ids"]],
                    "segments": domain_items,
                }
            )
    status, selected, reasons = compose(items, policy)
    return {
        "status": status,
        "value": selected[0]["value"] if selected else None,
        "interpretations": selected,
        "diagnostics": [
            {k: item[k] for k in ("applicability_id", "applicable", "status", "candidate_ids")}
            for item in items
        ],
        "reasons": reasons,
        "context_hash": context.context_hash,
        "policy_sha256": policy.fingerprint,
        "snapshot_token": context.snapshot_token,
        "input_snapshot_sha256": sha256(canonical_json([inputs, items]).encode()).hexdigest(),
    }
