"""Bounded fixed-revision lineage, authorization before bodies, and dirty propagation.

The manifest records processing inputs independently from output support. Metadata
headers let us authorize the entire transitive census before fetching any body.
"""

from copy import deepcopy
from datetime import datetime

from ..domain import canonical_json
from .model import DerivedError, digest

MAX_DEPTH = 4
MAX_NODES = 32
PARENT_CONTRACT = "processing-graph/1"


def supported(uow):
    return getattr(uow, "derived_parent_contract", None) == PARENT_CONTRACT


def revision_header(revision):
    fields = (
        "id", "facet_id", "state", "manifest", "manifest_sha256", "body_sha256",
        "unit", "parents", "built_at", "next_transition_at",
    )
    header = deepcopy({key: revision[key] for key in fields if key in revision})
    header["body_bytes"] = len(canonical_json(revision.get("body")).encode())
    return header


def validate_graph(definitions, spec, *, check_readers=True):
    definitions = {**definitions, spec["id"]: spec}
    visited = set()

    def visit(key, path):
        if key in path:
            raise DerivedError("derived_parent_cycle")
        if len(path) >= MAX_DEPTH:
            raise DerivedError("derived_parent_depth")
        if key not in definitions:
            raise DerivedError("derived_parent_unknown")
        visited.add(key)
        if len(visited) > MAX_NODES:
            raise DerivedError("derived_parent_capacity")
        row = definitions[key]
        if key != spec["id"] and row.get("resource_kind") == "page":
            raise DerivedError("derived_parent_page_unsupported")
        if row.get("history_mode") or row.get("context"):
            raise DerivedError("derived_parent_template_unsupported")
        if any(row.get(field) != spec.get(field) for field in (
            "subject_id", "purpose", "authority_id"
        )):
            raise DerivedError("derived_parent_scope_mismatch")
        if check_readers and not set(spec["readers"]).issubset(row["readers"]):
            raise DerivedError("derived_parent_denied")
        for parent in row.get("parent_facets", ()):
            visit(parent, (*path, key))

    visit(spec["id"], ())


async def invalidate_descendants(uow, scope, facets, *, safety=False):
    """Same original writer UoW; definitions are the existing durable outbox."""
    rows = await uow.derived_records(scope, "definition")
    affected = set(facets)
    while True:
        new = {
            item["identity"] for item in rows if not item["payload"].get("disabled")
            and set(item["payload"]["spec"].get("parent_facets", ())).intersection(affected)
        } - affected
        if not new:
            break
        affected.update(new)
        for item in rows:
            if item["identity"] in new:
                row = item["payload"]
                row["dirty"] = True
                if safety:
                    row["safety_generation"] += 1
                await uow.derived_put(scope, "definition", item["identity"], row)


class ParentGraph:
    def __init__(self, service):
        self.service, self.scope = service, service.scope

    async def validate_registration(self, uow, spec):
        if spec.get("parent_facets") and not supported(uow):
            raise DerivedError("derived_parent_backend_unsupported")
        definitions = {
            item["identity"]: item["payload"]["spec"]
            for item in await uow.derived_records(self.scope, "definition")
            if not item["payload"].get("disabled")
        }
        # Check descendants too: an ancestor edit can increase their depth or ACL.
        definitions[spec["id"]] = spec
        for candidate in definitions.values():
            if candidate.get("parent_facets"):
                validate_graph(
                    definitions, candidate, check_readers=candidate["id"] == spec["id"]
                )

    async def bindings(self, uow, spec):
        result = {}
        for key in spec.get("parent_facets", ()):
            definition = await uow.derived_get(self.scope, "definition", key)
            head = await uow.derived_get(self.scope, "head", key)
            if not definition or definition.get("disabled") or not head or (
                head.get("state") not in {"ready", "empty"}
                or not head.get("audit_revision_id")
            ):
                raise DerivedError("derived_parent_unavailable")
            result[key] = dict(
                revision_id=head["audit_revision_id"], head_sha256=digest(head),
                definition_sha256=digest(definition["spec"]),
            )
        return result or None

    async def inputs(self, uow, spec, expected, at, *, readers=None, load_bodies=True):
        """Authorize every transitive metadata header, then validate and load bodies."""
        if not spec.get("parent_facets"):
            return {}, {}
        if not supported(uow):
            raise DerivedError("derived_parent_backend_unsupported")
        readers = tuple(readers or spec["readers"])
        definitions = {
            item["identity"]: item["payload"]["spec"]
            for item in await uow.derived_records(self.scope, "definition")
            if not item["payload"].get("disabled")
        }
        validate_graph(definitions, {**spec, "readers": readers})
        if await self.bindings(uow, spec) != expected:
            raise DerivedError("derived_snapshot_changed")
        headers, seen = {}, set()

        async def visit(key):
            if key in seen:
                return
            seen.add(key)
            if len(seen) >= MAX_NODES:
                raise DerivedError("derived_parent_capacity")
            definition = await self.service._definition(uow, key)
            if not set(readers).issubset(definition["spec"]["readers"]):
                raise DerivedError("derived_parent_denied")
            head = await uow.derived_get(self.scope, "head", key)
            if not head or head.get("state") not in {"ready", "empty"}:
                raise DerivedError("derived_parent_unavailable")
            if head.get("unit") != (await self.service._unit(uow, definition)).payload():
                raise DerivedError("derived_parent_stale")
            if head.get("next_transition_at") and (
                datetime.fromisoformat(head["next_transition_at"]) <= at
            ):
                raise DerivedError("derived_parent_stale")
            header = await uow.derived_get(
                self.scope, "revision_header", head["audit_revision_id"]
            )
            if not header or digest(header) != head.get("input_header_sha256"):
                # Older publications must be explicitly republished; never inspect
                # an old body to fabricate a processing proof after the fact.
                raise DerivedError("derived_parent_proof_missing")
            if header["unit"] != head["unit"] or header["facet_id"] != key or (
                header["id"] != head["audit_revision_id"]
                or digest(header["manifest"]) != header["manifest_sha256"]
                or datetime.fromisoformat(header["built_at"]) > at
            ):
                raise DerivedError("derived_parent_integrity_failed")
            if header["manifest"].get("parents") != (
                header["unit"].get("parents") or None
            ):
                raise DerivedError("derived_parent_integrity_failed")
            _, authority = await self.service.registry.bindings(uow, definition["spec"])
            for source_id, data in header["manifest"]["sources"].items():
                grant = await uow.derived_get(self.scope, "grant", source_id)
                self.service._permission(grant, readers, spec["purpose"], at, authority)
                if grant["version"] != data["grant_version"]:
                    raise DerivedError("derived_parent_stale")
            headers[key] = header
            for parent in definition["spec"].get("parent_facets", ()):
                await visit(parent)

        for key in spec["parent_facets"]:
            await visit(key)
        input_bytes = sum(
            len(canonical_json(h).encode()) + h["body_bytes"] for h in headers.values()
        )
        if input_bytes > 262144:
            raise DerivedError("derived_parent_input_capacity")
        if not load_bodies:
            return {}, headers
        # All permissions have passed. No revision/L0/L1 body was fetched above.
        revisions = {}
        for key, header in headers.items():
            for reader in readers:
                view = await self.service._read(
                    uow, key, reader, spec["purpose"], lineage_checked=True
                )
                if view["state"] not in {"ready", "empty"}:
                    raise DerivedError("derived_parent_stale")
            revision = await uow.derived_get(self.scope, "revision", header["id"])
            if not revision or revision_header(revision) != header or (
                revision.get("state") == "ready"
                and digest(revision["body"]) != header["body_sha256"]
            ):
                raise DerivedError("derived_parent_integrity_failed")
            revisions[key] = revision
        if len(canonical_json(revisions).encode()) > 262144:
            raise DerivedError("derived_parent_input_capacity")
        return {key: revisions[key] for key in spec["parent_facets"]}, headers


def waiting_for_parent(error):
    return error.code.startswith(("derived_parent_", "derived_processing_")) or (
        error.code == "derived_grant_authority_changed"
    )


def compose_parents(definition, parents):
    """A derived view preserves complete blocks, including disputes and provenance."""
    if set(parents) != set(definition["parent_facets"]):
        raise DerivedError("derived_input_dependency_unknown")
    blocks, support, transitions = [], [], []
    for key in definition["parent_facets"]:
        parent = parents[key]
        if parent["state"] == "ready":
            blocks.append(dict(
                kind="derived_view", facet_id=key, revision_id=parent["id"],
                blocks=deepcopy(parent["body"]["blocks"]),
            ))
            support.append(("support", "derived:" + parent["id"]))
        if parent.get("next_transition_at"):
            transitions.append(parent["next_transition_at"])
    body = dict(subject_id=definition["subject_id"], facet=definition["facet"], blocks=blocks)
    if len(canonical_json(body).encode()) > 32768:
        raise DerivedError("derived_output_capacity")
    return dict(
        body=body, body_sha256=digest(body), support=support,
        next_transition_at=min(transitions, key=datetime.fromisoformat) if transitions else None,
        no_outputs=not blocks,
    )
