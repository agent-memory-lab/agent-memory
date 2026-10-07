"""Deterministic full-snapshot composer; never mutates authoritative L1."""

from datetime import datetime

from ..domain import canonical_json
from ..retrieval.atom_state import project_records
from .model import DerivedError, digest


def compose(definition, records, sources, at):
    for record in records:
        payload = record["payload"]
        if payload.get("qualification"):
            # A locale facet has no host condition context in this version.
            # Keep the old view inaccessible rather than dropping qualifiers.
            raise DerivedError("derived_qualification_unsupported")
        draft = payload.get("draft", {})
        if (
            draft.get("subject_id") != definition["subject_id"]
            or draft.get("predicate") not in definition["predicates"]
        ):
            raise DerivedError("derived_input_outside_facet")
        if not isinstance(draft.get("value"), str):
            raise DerivedError("derived_input_invalid")
        if draft.get("conditions") or draft.get("exceptions") or draft.get("negated"):
            raise DerivedError("derived_qualification_unsupported")
        if payload.get("action") == "ACCEPT":
            quote = draft.get("source_quote")
            if (
                not isinstance(quote, str)
                or not quote
                or quote not in sources[record["event_id"]].content
            ):
                raise DerivedError("derived_support_quote_missing")
    claims, diagnostics = project_records(records, at)
    if diagnostics.get("context_required"):
        raise DerivedError("derived_qualification_unsupported")
    blocks, support = [], []
    for claim in claims:
        evidence = diagnostics["atom_support"][claim.id]
        candidate = evidence["candidate_id"]
        blocks.append(
            dict(
                kind="source_fact",
                subject_id=definition["subject_id"],
                predicate="locale",
                value=claim.value,
                candidate_id=candidate,
                valid_from=claim.valid_from.isoformat(),
                valid_to=claim.valid_to.isoformat() if claim.valid_to else None,
                support_basis=evidence["basis"],
                evidence=evidence["evidence"],
            )
        )
        support.append(("support", "atom:" + candidate))
    for conflict in diagnostics["conflicts"]:
        blocks.append(
            dict(kind="conflict", candidate_ids=conflict["candidate_ids"], predicate="locale")
        )
        support.extend(("support", "atom:" + key) for key in conflict["candidate_ids"])
    if len(blocks) > 16 or len(canonical_json(blocks)) > 32768:
        raise DerivedError("derived_output_capacity")
    transitions = []
    for row in records:
        for field in ("valid_from", "valid_to"):
            value = row["payload"].get(field)
            if value and datetime.fromisoformat(value) > at:
                transitions.append(datetime.fromisoformat(value))
    body = dict(subject_id=definition["subject_id"], facet=definition["facet"], blocks=blocks)
    return dict(
        body=body,
        body_sha256=digest(body),
        support=sorted(set(support)),
        next_transition_at=min(transitions).isoformat() if transitions else None,
        no_outputs=not blocks,
    )
