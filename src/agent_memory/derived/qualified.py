"""Explicit current-route contracts for qualification-preserving parent/page views.

These proofs are part of the immutable generation manifest. They never authorize
body access on their own: the caller must still check current authority, source
grants, all fixed parent revisions, and the publication unit in its transaction.
"""

from copy import deepcopy
from datetime import datetime

from ..domain import canonical_json
from .model import DerivedError, FacetContext, digest

QUALIFIED_PARENT_TEMPLATE = "locale-qualified-parents/1"
QUALIFIED_PAGE_TEMPLATE = "language-qualified-scenario/1"
QUALIFIED_TEMPLATES = {QUALIFIED_PARENT_TEMPLATE, QUALIFIED_PAGE_TEMPLATE}
ROUTE_CONTRACT = "qualified-current-route/1"


def qualified_supported(uow):
    # Current qualification expiry must survive a restarted reader and wall
    # rollback, so route support includes the existing durable clock contract.
    from ..operations.refresh_demand import supported as scheduler_supported

    return (
        getattr(uow, "derived_qualified_contract", None) == ROUTE_CONTRACT
        and scheduler_supported(uow)
    )


def is_qualified_graph(spec):
    return spec.get("template_version") in QUALIFIED_TEMPLATES


def route_proof(spec):
    """Bind the actual host context, qualification policy and graph template."""
    context = spec.get("context")
    if context is None:
        if is_qualified_graph(spec):
            raise DerivedError("derived_parent_route_missing")
        return None
    binding = FacetContext.from_payload(context)
    if binding.query.subject_id != spec["subject_id"] or (
        binding.query.purpose != spec["purpose"]
    ):
        raise DerivedError("derived_context_mismatch")
    if is_qualified_graph(spec) and spec.get("history_mode"):
        raise DerivedError("derived_parent_template_unsupported")
    return dict(
        schema=ROUTE_CONTRACT,
        template=spec["template_version"],
        context=deepcopy(binding.payload()),
        context_sha256=digest(binding.payload()),
        qualification_policy_sha256=binding.policy.fingerprint,
        authority_id=spec.get("authority_id"),
    )


def manifest_fields(spec):
    proof = route_proof(spec)
    return {"qualification": proof} if proof is not None else {}


def manifest_schema(spec, has_parents):
    return "derived-input-manifest/3" if spec.get("context") is not None else (
        "derived-input-manifest/2" if has_parents else "derived-input-manifest/1"
    )


def validate_manifest_route(spec, manifest, at):
    """Run from metadata before loading any parent or page revision body."""
    proof = route_proof(spec)
    if manifest.get("qualification") != proof:
        raise DerivedError("derived_parent_route_proof_invalid")
    if proof is not None:
        FacetContext.from_payload(proof["context"]).current(at)
    return proof


def validate_header_route(spec, header, at):
    """Old revisions cannot gain a route certificate without a real rebuild."""
    if not header or header.get("manifest", {}).get("schema") != "derived-input-manifest/3":
        raise DerivedError("derived_parent_route_proof_invalid")
    proof = validate_manifest_route(spec, header["manifest"], at)
    if proof is None or header.get("qualification") != proof:
        raise DerivedError("derived_parent_route_proof_invalid")
    return proof


def validate_route_edge(child, parent):
    """No implicit widening: compatibility is exact except contained lifetimes."""
    if child.get("history_mode") or parent.get("history_mode"):
        raise DerivedError("derived_parent_template_unsupported")
    if not is_qualified_graph(child):
        if child.get("context") or parent.get("context") or is_qualified_graph(parent):
            raise DerivedError("derived_parent_template_unsupported")
        return
    if parent.get("template_version") not in {"locale-context/1", QUALIFIED_PARENT_TEMPLATE}:
        raise DerivedError("derived_parent_template_unsupported")
    if child.get("context") is None or parent.get("context") is None:
        raise DerivedError("derived_parent_route_missing")
    c, p = (FacetContext.from_payload(row["context"]) for row in (child, parent))
    for row, binding in ((child, c), (parent, p)):
        if row.get("subject_id") != binding.query.subject_id or (
            row.get("purpose") != binding.query.purpose
        ):
            raise DerivedError("derived_context_mismatch")
    for field in ("subject_id", "purpose", "authority_id"):
        if child.get(field) != parent.get(field):
            raise DerivedError("derived_parent_scope_mismatch")
    for field in ("principal", "scope", "subject_id", "purpose", "snapshot_token",
                  "attributes", "timezone"):
        if getattr(c.query, field) != getattr(p.query, field):
            raise DerivedError("derived_parent_route_mismatch")
    if c.policy != p.policy:
        raise DerivedError("derived_parent_qualification_mismatch")
    if c.query.known_at < p.query.known_at or c.expires_at > p.expires_at:
        raise DerivedError("derived_parent_route_lifetime")


def preserved_observation(parent):
    """Copy the entire actual observation, never reconstruct from its summary."""
    body = parent.get("body")
    if not isinstance(body, dict) or not isinstance(body.get("blocks"), list):
        raise DerivedError("derived_parent_integrity_failed")
    return deepcopy(body)


def compose_qualified_parents(definition, parents):
    """Full projection preserves entire observations; it does not combine claims."""
    if not is_qualified_graph(definition) or set(parents) != set(definition["parent_facets"]):
        raise DerivedError("derived_input_dependency_unknown")
    proof = route_proof(definition)
    blocks, support, transitions = [], [], [proof["context"]["expires_at"]]
    for key in definition["parent_facets"]:
        parent = parents[key]
        if parent["state"] == "ready":
            blocks.append(dict(
                kind="qualified_derived_view", facet_id=key, revision_id=parent["id"],
                observation=preserved_observation(parent),
            ))
            support.append(("support", "derived:" + parent["id"]))
        elif parent["state"] != "empty":
            raise DerivedError("derived_parent_unavailable")
        if parent.get("next_transition_at"):
            transitions.append(parent["next_transition_at"])
    body = dict(subject_id=definition["subject_id"], facet=definition["facet"], blocks=blocks,
                qualification=proof)
    if len(canonical_json(body).encode()) > 32768:
        raise DerivedError("derived_output_capacity")
    return dict(
        body=body, body_sha256=digest(body), support=support,
        next_transition_at=min(transitions, key=datetime.fromisoformat), no_outputs=not blocks,
    )
