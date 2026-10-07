"""Pure language-facet composition preserving reviewed qualifiers and field proofs."""

from datetime import UTC, datetime, time, timedelta
from hashlib import sha256
from zoneinfo import ZoneInfo

from ..conditions import Condition, ProjectionPolicy, applicability
from ..domain import canonical_json
from ..evidence_support import SupportRange, evaluate_support, intersect, union
from ..retrieval.atom_state import project_records
from ..retrieval.contextual_state import compose as compose_domains
from .model import DerivedError, FacetContext, digest


def reviewed(payload, sources, binding, admission_policy):
    q, draft = payload["qualification"], payload["draft"]
    required = {"subject_id", "predicate", "value"}
    spec = next(s for s in admission_policy["predicates"] if s["predicate"] == "locale")
    required.update(spec.get("required_evidence_fields", ()))
    required.update(
        k for k in ("valid_from", "valid_to", "conditions", "exceptions") if draft.get(k)
    )
    if (
        q.get("schema") != "contextual-qualification/1"
        or q.get("principal") != binding.query.principal
        or q.get("admission_policy") != admission_policy
        or q.get("target_sha256") != sha256(canonical_json(draft).encode()).hexdigest()
        or q.get("policy_sha256") != binding.policy.fingerprint
        or ProjectionPolicy(**q["policy"]).fingerprint != binding.policy.fingerprint
        or len(q["conditions"]) != len(draft.get("conditions", []))
        or len(q["exceptions"]) != len(draft.get("exceptions", []))
        or not required <= {f["field"] for f in q["field_support"]}
    ):
        raise DerivedError("derived_qualification_invalid")
    links = {link["id"]: link for link in q["links"]}
    if len(links) != len(q["links"]):
        raise DerivedError("derived_qualification_invalid")
    for field in q["field_support"]:
        for group in field["alternatives"]:
            if any(key not in links or field["field"] not in links[key]["fields"] for key in group):
                raise DerivedError("derived_qualification_invalid")
    for link in links.values():
        span = link["span"]
        source = sources.get(span["source_event_id"])
        if (
            source is None
            or source.content_hash != link["source_sha256"]
            or link["target_sha256"] != q["target_sha256"]
            or type(span["start"]) is not int
            or type(span["end"]) is not int
            or not 0 <= span["start"] < span["end"] <= len(source.content)
            or source.content[span["start"] : span["end"]] != span["quote"]
        ):
            raise DerivedError("derived_support_quote_missing")
    return q


def compose_contextual(definition, records, sources, at, admission_policy):
    binding = FacetContext.from_payload(definition["context"])
    context = binding.current(at)
    items, ordinary, groups = [], [], {}
    transitions = [binding.expires_at]
    for row in sorted(records, key=lambda r: r["id"]):
        p = row["payload"]
        draft = p.get("draft", {})
        if (
            draft.get("subject_id") != definition["subject_id"]
            or draft.get("predicate") != "locale"
        ):
            raise DerivedError("derived_input_outside_facet")
        if not isinstance(draft.get("value"), str):
            raise DerivedError("derived_input_invalid")
        for field in ("valid_from", "valid_to"):
            if p.get(field) and datetime.fromisoformat(p[field]) > at:
                transitions.append(datetime.fromisoformat(p[field]))
        if p["action"] in {"WITHDRAWN", "REJECT", "L0_ONLY"}:
            ordinary.append(row)  # Preserve ordinary predecessor/withdrawal barriers.
            continue
        if draft.get("negated") or draft.get("modality") != "asserted":
            raise DerivedError("derived_qualification_unsupported")
        if not p.get("qualification"):
            if draft.get("conditions") or draft.get("exceptions"):
                raise DerivedError("derived_qualification_incomplete")
            if p["action"] == "ACCEPT" and (
                not draft.get("source_quote")
                or draft["source_quote"] not in sources[row["event_id"]].content
            ):
                raise DerivedError("derived_support_quote_missing")
            ordinary.append(row)
            continue
        if (
            p["action"] != "PENDING_VERIFICATION"
            or draft.get("change_kind") != "replace"
            or draft.get("kind") not in {"fact", "preference"}
        ):
            raise DerivedError("derived_qualification_unsupported")
        if (
            not draft.get("source_quote")
            or draft["source_quote"] not in sources[row["event_id"]].content
        ):
            raise DerivedError("derived_support_quote_missing")
        q = reviewed(p, sources, binding, admission_policy)
        signature = canonical_json([q["conditions"], q["exceptions"]])
        domain = q["applicability_id"]
        if domain == "global" or (domain in groups and groups[domain][0] != signature):
            raise DerivedError("derived_applicability_changed")
        groups.setdefault(domain, (signature, []))[1].append(row)
        ranges, _ = evaluate_support(q, sources)
        for value in ranges:
            transitions.extend(v for v in (value.start, value.end) if v and v > at)
        # Weekday expressions can change without any write to a source.
        if '"weekday"' in signature and context.timezone:
            zone = ZoneInfo(context.timezone)
            tomorrow = context.valid_at.astimezone(zone).date() + timedelta(days=1)
            transitions.append(datetime.combine(tomorrow, time(), zone).astimezone(UTC))
    claims, info = project_records(ordinary, at)
    for claim in claims:
        evidence = info["atom_support"][claim.id]
        block = dict(
            kind="source_fact",
            subject_id=definition["subject_id"],
            predicate="locale",
            value=claim.value,
            candidate_id=evidence["candidate_id"],
            valid_from=claim.valid_from.isoformat(),
            valid_to=claim.valid_to.isoformat() if claim.valid_to else None,
            support_basis=evidence["basis"],
            evidence=evidence["evidence"],
        )
        items.append(
            dict(
                applicability_id="global",
                applicable=True,
                status="resolved",
                value=claim.value,
                candidate_ids=[evidence["candidate_id"]],
                blocks=[block],
            )
        )
    for conflict in info["conflicts"]:
        items.append(
            dict(
                applicability_id="global",
                applicable=True,
                status="contested",
                candidate_ids=conflict["candidate_ids"],
            )
        )
    for domain, (_, members) in sorted(groups.items()):
        eligible = [r for r in members if datetime.fromisoformat(r["payload"]["valid_from"]) <= at]
        if not eligible:
            continue
        boundary = max(datetime.fromisoformat(r["payload"]["valid_from"]) for r in eligible)
        domain_items = []
        for row in [
            r for r in eligible if datetime.fromisoformat(r["payload"]["valid_from"]) == boundary
        ]:
            p, q = row["payload"], row["payload"]["qualification"]
            applies = applicability(
                [Condition(**c) for c in q["conditions"]],
                [Condition(**c) for c in q["exceptions"]],
                context,
            )
            ranges, proofs = evaluate_support(q, sources)
            ends = [
                datetime.fromisoformat(r["payload"]["valid_from"])
                for r in members
                if datetime.fromisoformat(r["payload"]["valid_from"]) > at
            ]
            if p.get("valid_to"):
                ends.append(datetime.fromisoformat(p["valid_to"]))
            candidate_range = SupportRange(
                datetime.fromisoformat(p["valid_from"]), min(ends) if ends else None
            )
            ranges = union(
                [i for value in ranges if (i := intersect(value, candidate_range)) is not None]
            )
            active = [r for r in ranges if r.contains(at)]
            blocks = []
            if active:
                supported = active[0]
                if supported.kind == "point":
                    transitions.append(at + timedelta(microseconds=1))
                blocks.append(
                    dict(
                        kind="source_fact",
                        subject_id=definition["subject_id"],
                        predicate="locale",
                        value=p["draft"]["value"],
                        candidate_id=row["id"],
                        qualified=True,
                        applicability_id=domain,
                        conditions=p["draft"].get("conditions", []),
                        exceptions=p["draft"].get("exceptions", []),
                        valid_from=supported.start.isoformat(),
                        valid_to=supported.end.isoformat() if supported.end else None,
                        support_kind=supported.kind,
                        support_basis="field_supported",
                        field_support=q["field_support"],
                        condition_bindings=q["conditions"],
                        exception_bindings=q["exceptions"],
                        evidence=[
                            link
                            for link in q["links"]
                            if any(link["id"] in ids for r, ids in proofs if r.contains(at))
                        ],
                        evidence_link_ids=sorted(
                            {i for r, ids in proofs if r.contains(at) for i in ids}
                        ),
                    )
                )
            domain_items.append(
                dict(
                    applicability_id=domain,
                    applicable=applies,
                    status="resolved" if active else "unknown",
                    value=p["draft"]["value"],
                    candidate_ids=[row["id"]],
                    blocks=blocks,
                )
            )
        status, selected, _ = compose_domains(
            domain_items, ProjectionPolicy(binding.policy.revision, binding.policy.purpose)
        )
        items.append(
            dict(
                applicability_id=domain,
                applicable=domain_items[0]["applicable"],
                status="contested" if status == "ambiguous" else status,
                value=selected[0]["value"] if selected else None,
                candidate_ids=[i for item in domain_items for i in item["candidate_ids"]],
                blocks=[b for item in selected for b in item["blocks"]],
            )
        )
    status, selected, reasons = compose_domains(items, binding.policy)
    blocks, support = [], []
    if status == "resolved":
        blocks = [block for item in selected for block in item["blocks"]]
        support = [("support", "atom:" + b["candidate_id"]) for b in blocks]
    elif any(i["applicable"] is not False for i in items):
        ids = sorted(
            {
                key
                for item in items
                if item["applicable"] is not False
                for key in item["candidate_ids"]
            }
        )
        blocks = [
            dict(
                kind="context_unknown" if status == "unknown" else "conflict",
                predicate="locale",
                candidate_ids=ids,
                reasons=reasons,
            )
        ]
        if status != "unknown":
            support = [("support", "atom:" + key) for key in ids]
    if len(blocks) > 16 or len(canonical_json(blocks)) > 32768:
        raise DerivedError("derived_output_capacity")
    body = dict(
        subject_id=definition["subject_id"],
        facet=definition["facet"],
        blocks=blocks,
        context=binding.payload(),
        context_sha256=digest(binding.payload()),
        projection_status=status,
        policy_sha256=binding.policy.fingerprint,
    )
    return dict(
        body=body,
        body_sha256=digest(body),
        support=sorted(set(support)),
        next_transition_at=min(transitions).isoformat(),
        no_outputs=not blocks,
    )
