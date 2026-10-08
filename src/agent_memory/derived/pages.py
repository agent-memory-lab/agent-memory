"""L2 full rebuild, versioned blocks, and guarded presentation on the shared lifecycle."""

from copy import deepcopy
from datetime import datetime

from ..domain import canonical_json
from .model import DerivedError, digest, identity
from .page_model import PAGE_CONTRACT, PAGE_TEMPLATE, PageBlockRevision, PageDefinition
from .parents import supported


def is_page(spec):
    return spec.get("resource_kind") == "page"


def page_supported(uow):
    return supported(uow) and getattr(uow, "derived_page_contract", None) == PAGE_CONTRACT


def materialized_body(body, block_bodies):
    """The same delivered representation defines the full rebuild output budget."""
    return {**body, "blocks": [
        {**ref, "body": deepcopy(content)}
        for ref, content in zip(body["blocks"], block_bodies, strict=True)
    ]}


def compose_page(snapshot, scope):
    spec, parents, manifest = (
        snapshot["definition"]["spec"], snapshot["parents"], snapshot["manifest"]
    )
    if set(parents) != set(spec["parent_facets"]):
        raise DerivedError("derived_input_dependency_unknown")
    manifest_hash = digest(manifest)
    blocks, references, support, transitions = [], [], [], []
    for key in spec["parent_facets"]:
        parent = parents[key]
        if parent["state"] == "ready":
            stable_id = "page-block:" + digest([scope.partition_key(), spec["id"], key])
            content = dict(
                kind="language_observation", parent_facet_id=key,
                parent_revision_id=parent["id"], content=deepcopy(parent["body"]["blocks"]),
            )
            body_hash = digest(content)
            revision_id = "page-block-version:" + digest([stable_id, body_hash, manifest_hash])
            block = PageBlockRevision(
                revision_id, stable_id, spec["id"], content, body_hash, manifest, manifest_hash
            ).payload()
            blocks.append(block)
            references.append(dict(block_id=stable_id, revision_id=revision_id, sha256=body_hash))
            support.append(("support", "derived:" + parent["id"]))
        if parent.get("next_transition_at"):
            transitions.append(parent["next_transition_at"])
    body = dict(
        schema="memory-page/1", page_id=spec["id"], subject_id=spec["subject_id"],
        scenario=deepcopy(spec["scenario"]), template=PAGE_TEMPLATE,
        rebuild="full", blocks=references,
    )
    # Include the materialized blocks in the output budget, not just tiny references.
    delivered = materialized_body(body, [block["body"] for block in blocks])
    if len(canonical_json(delivered).encode()) > 32768:
        raise DerivedError("page_output_capacity")
    return dict(
        body=body, body_sha256=digest(body), block_revisions=blocks,
        support=support, next_transition_at=(
            min(transitions, key=datetime.fromisoformat) if transitions else None
        ), no_outputs=not blocks,
    )


class KnowledgePages:
    """Page-specific adapter; the existing service owns UoW, input guards and queue."""

    def __init__(self, service):
        self.service, self.scope = service, service.scope

    async def check(self, uow, spec):
        if is_page(spec) and not page_supported(uow):
            raise DerivedError("page_backend_unsupported")

    async def register(self, definition, *, expected_generation=0):
        if not isinstance(definition, PageDefinition):
            raise TypeError("trusted PageDefinition required")
        if definition.scenario.scope != self.scope:
            raise DerivedError("page_scenario_scope_mismatch")
        if self.service.history_mode is not None:
            raise DerivedError("page_history_unsupported")
        if self.service.context_token is not None:
            raise DerivedError("page_context_unsupported")
        return await self.service._register_spec(
            definition.payload(), expected_generation=expected_generation, definition_slots=[]
        )

    async def persist(self, uow, snapshot, prepared, page_revision_id):
        if not is_page(snapshot["definition"]["spec"]):
            return
        await self.check(uow, snapshot["definition"]["spec"])
        blocks = prepared["block_revisions"]
        if len(await uow.derived_records(self.scope, "page_block")) + len(blocks) > 4096:
            raise DerivedError("page_block_capacity")
        for block in blocks:
            row = dict(**deepcopy(block), page_revision_id=page_revision_id)
            old = await uow.derived_get(self.scope, "page_block", row["id"])
            if old is not None and old != row:
                raise DerivedError("page_block_revision_conflict")
            await uow.derived_put(self.scope, "page_block", row["id"], row)

    async def read(self, page_id, *, actor, purpose="agent_context"):
        from .service import open_derived

        async with self.service.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.service.history_mode)
            return await self._read(uow, page_id, actor, purpose)

    async def _read(self, uow, page_id, actor, purpose):
        definition = await self.service._definition(uow, identity(page_id))
        if not is_page(definition["spec"]):
            raise DerivedError("derived_resource_kind_mismatch")
        view = await self.service._read(uow, page_id, actor, purpose)
        result = dict(page_id=page_id, state=view["state"], body=None)
        if view.get("reason"):
            result["reason"] = view["reason"]
        if view["state"] != "ready":
            # Distinguish unfinished/failed rebuild from a completed empty census.
            try:
                unit = await self.service._unit(uow, definition)
                job = await uow.derived_get(self.scope, "job", unit.id)
            except DerivedError:
                job = None
            result["refresh_state"] = job["status"] if job else "waiting_inputs"
            if view.get("reason") == "not_built" and job:
                result["state"] = "failed" if job["status"] == "dead" else "building"
            if view["state"] == "empty":
                head = await uow.derived_get(self.scope, "head", page_id)
                result.update(revision_id=head["audit_revision_id"], processed_unit=head["unit"])
            return result
        # Shared read already passed the entire current processing chain. Only
        # then load page blocks; references alone never prove a complete page.
        revision = await uow.derived_get(self.scope, "revision", view["revision_id"])
        materialized = []
        for ref in view["body"]["blocks"]:
            block = await uow.derived_get(self.scope, "page_block", ref["revision_id"])
            if not block or block.get("state") != "ready" or (
                block.get("page_id") != page_id or block.get("block_id") != ref["block_id"]
                or block.get("page_revision_id") != revision["id"]
                or digest(block.get("body")) != ref["sha256"]
                or block.get("body_sha256") != ref["sha256"]
                or block.get("manifest") != revision["manifest"]
                or block.get("manifest_sha256") != revision["manifest_sha256"]
                or block.get("id") != "page-block-version:" + digest([
                    ref["block_id"], ref["sha256"], revision["manifest_sha256"]
                ])
            ):
                return dict(page_id=page_id, state="invalid", body=None,
                            reason="page_block_integrity_failed")
            materialized.append(block["body"])
        result.update(
            revision_id=revision["id"], refresh_state="completed",
            body=materialized_body(view["body"], materialized),
            processed_unit=deepcopy(revision["unit"]),
        )
        return result

    async def status(self, target_id, *, actor, purpose="agent_context"):
        from ..operations.facet_refresh import FacetRefreshQueue
        from .service import open_derived

        fixed = await FacetRefreshQueue(self.service).status(target_id, actor=actor)
        async with self.service.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.service.history_mode)
            receipt = await uow.derived_get(self.scope, "request", target_id)
            if not receipt or receipt.get("invalidated"):
                raise DerivedError("derived_target_unavailable")
            view = await self._read(uow, receipt["facet_id"], actor, purpose)
            head = await uow.derived_get(self.scope, "head", receipt["facet_id"])
            current = bool(fixed["complete"] and head and head.get("unit") == receipt["unit"])
            return dict(
                **fixed, page_id=receipt["facet_id"], page_state=view["state"],
                page_complete=current and view["state"] in {"ready", "empty"},
                page_ready=current and view["state"] == "ready",
            )

    async def call(self, operation, payload, context):
        if not isinstance(payload, dict) or set(payload) - {"page_id", "purpose", "target_id"}:
            raise DerivedError("invalid_page_request")
        if operation == "page_capabilities":
            from .service import open_derived

            async with self.service.repository.unit_of_work() as uow:
                await open_derived(uow, self.scope, self.service.history_mode)
                enabled = (page_supported(uow) and self.service.history_mode is None
                           and self.service.context_token is None)
            return dict(
                schema="page-capabilities/1", readonly=True, enabled=enabled,
                templates=[PAGE_TEMPLATE] if enabled else [],
                rebuild_modes=["full"] if enabled else [],
                historical=False, page_parents=False, inferred_persona=False,
                contract=PAGE_CONTRACT if enabled else None,
                limits=dict(parents=4, graph_nodes=32, path_nodes=4, output_bytes=32768,
                            scope_block_revisions=4096),
            )
        if operation == "page_status":
            return await self.status(
                payload.get("target_id"), actor=context.actor,
                purpose=payload.get("purpose", "agent_context"),
            )
        if operation not in {"page_read", "page_context"}:
            raise DerivedError("unsupported_derived_operation")
        view = await self.read(
            payload.get("page_id"), actor=context.actor,
            purpose=payload.get("purpose", "agent_context"),
        )
        if operation == "page_context":
            view = await self.read(
                payload.get("page_id"), actor=context.actor,
                purpose=payload.get("purpose", "agent_context"),
            )
            return dict(schema="page-context/1", state=view["state"],
                        pages=[view] if view["state"] == "ready" else [])
        return view
