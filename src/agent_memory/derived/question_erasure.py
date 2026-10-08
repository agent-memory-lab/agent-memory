"""Pure physical QuestionView erasure plan, shared by live forget and backup replay.

The original generation manifest is authoritative even when later certificates
validate equal public values. Every revision for an affected instance is scrubbed.
Routing selectors, aliases, contexts, finite receipts and scheduler metadata follow
that same opaque instance identity; no raw question labels survive as storage keys.
"""

from copy import deepcopy

from .project_index import query_keys
from .question_contracts import REGISTRATION_SCHEMA
from .question_pages import PAGE_KINDS

QUESTION_KINDS = (
    "question_registration",
    "question_content",
    "question_certificate",
    "question_head",
    *PAGE_KINDS,
)
RELATED_KINDS = (
    "definition",
    "job",
    "request",
    "refresh_policy",
    "refresh_demand",
    "refresh_execution",
    "refresh_publication",
    "coverage_request",
)


def _instance(kind, key, row):
    if kind in PAGE_KINDS:
        return row.get("instance_id")
    if kind == "question_content":
        # Content wire contains a full instance but no redundant instance ID.
        from .question_model import QuestionInstance

        try:
            return QuestionInstance.from_payload(row["instance"]).id
        except (KeyError, TypeError, ValueError):
            return row.get("instance_id")
    if kind == "question_certificate":
        return row.get("instance_id")
    if kind == "question_head":
        return row.get("head", {}).get("instance_id", row.get("facet_id", key))
    if kind == "question_registration":
        return row.get("instance_id")
    if kind == "job":
        return row.get("unit", {}).get("facet_id", row.get("facet_id"))
    return row.get("facet_id", key if kind == "definition" else None)


def _dependencies(kind, row):
    result = set()
    manifests = []
    if kind in PAGE_KINDS:
        manifests.append(row.get("generation_manifest", {}))
        # Unpublished page registrations still inherit their parents' erasure.
        for parent in row.get("parents", ()):
            if isinstance(parent, dict) and parent.get("instance_id"):
                result.add("derived:" + parent["instance_id"])
        if kind == "question_page_head":
            result.update("derived:" + key for key in row.get("parents", {}))
    if kind == "question_content":
        manifests.append(row.get("generation_manifest", {}))
    elif kind == "question_certificate":
        manifests.append(row.get("validation_manifest", {}))
        # Explicit support can only add a dependency, never replace generation.
        manifests.append({"inputs": row.get("support", ())})
    for manifest in manifests:
        for ref in manifest.get("inputs", ()):
            if ref.get("kind") in {"source", "atom"}:
                result.add(ref["kind"] + ":" + ref["id"])
            elif ref.get("kind") in {"derived_content", "derived_certificate"}:
                result.add("derived:" + ref["id"])
    if kind == "question_head":
        proof = row.get("proof", {})
        result.update("source:" + s["source_event_id"] for s in proof.get("sources", ()))
        for candidate in proof.get("candidates", ()):
            result.add("atom:" + candidate["id"])
            result.update("source:" + key for key in candidate.get("source_ids", ()))
    return result


def erase_question_rows(rows, parents, all_in_scope, *, project_routes=()):
    """Return (replacement rows, affected instance IDs, dependency owners to clear).

    rows use the existing derived ledger {kind, identity, payload} shape. parents
    are canonical source:/atom:/derived: references from the provider's actual
    affected-scope deletion loop. project_routes are old header routes captured
    before deletion, protecting empty and not-yet-built subscribed questions.
    """
    rows, parents, routes = deepcopy(tuple(rows)), set(parents), set(project_routes)
    instances = {
        (r["kind"], r["identity"]): _instance(r["kind"], r["identity"], r["payload"]) for r in rows
    }
    question_instances = {
        i
        for r in rows
        if r["kind"] in QUESTION_KINDS
        if (i := instances[(r["kind"], r["identity"])])
    }
    question_instances.update(
        r["identity"]
        for r in rows
        if r["kind"] == "definition"
        and r["payload"].get("spec", {}).get("schema") == REGISTRATION_SCHEMA
    )
    affected = set(question_instances) if all_in_scope else set()
    for item in rows:
        kind, key, row = item["kind"], item["identity"], item["payload"]
        instance = instances[(kind, key)]
        if instance in question_instances and (
            "derived:" + key in parents
            or "derived:" + instance in parents
            or parents.intersection(_dependencies(kind, row))
        ):
            affected.add(instance)
        spec = row.get("spec", {})
        if (
            kind == "definition"
            and spec.get("schema") == REGISTRATION_SCHEMA
            and routes.intersection(query_keys(spec["contract_fingerprint"], spec["project_id"]))
        ):
            affected.add(instance)
    # Future typed question parents must inherit the complete original processing
    # lineage, including content and certificate links, not just cited support.
    while True:
        changed = False
        dead = {
            "derived:" + r["identity"]
            for r in rows
            if instances[(r["kind"], r["identity"])] in affected
        }
        for item in rows:
            instance = instances[(item["kind"], item["identity"])]
            if (
                instance
                and instance not in affected
                and dead.intersection(_dependencies(item["kind"], item["payload"]))
            ):
                affected.add(instance)
                changed = True
        if not changed:
            break
    changes, owners = [], set()
    for item in rows:
        kind, key, row = item["kind"], item["identity"], item["payload"]
        instance = instances[(kind, key)]
        if kind not in (*QUESTION_KINDS, *RELATED_KINDS) or instance not in affected:
            continue
        owners.add(key)
        if kind == "definition":
            # Legacy subscription rebuild needs only these inert coordinates.
            value = dict(
                facet_id=key,
                spec={"id": key, "schema": "question-erased/1"},
                slots=[],
                disabled=True,
                dirty=False,
                epoch=row.get("epoch", 0),
                generation=row.get("generation", 0),
                fingerprint=row.get("fingerprint"),
                safety_generation=row.get("safety_generation", 0) + 1,
                time_generation=row.get("time_generation", 0),
                state="erased",
            )
        elif kind == "job":
            value = {
                "id": key,
                "facet_id": instance,
                "status": "cancelled",
                "unit": {},
                "reason": "erased",
            }
        else:
            value = {"id": key, "instance_id": instance, "facet_id": instance, "state": "erased"}
        changes.append((kind, key, value))
    owners.update(affected)
    return changes, affected, owners
