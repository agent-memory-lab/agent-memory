"""Transactional reverse routing for the admitted-L1 derived runtime.

Routing edges are metadata, never published support/processing provenance. Installation
and every lookup run under the same admission scope lock as semantic writes. The
scope gate is a coordinated upgrade boundary: old writers must be stopped first.
"""

from copy import deepcopy
from datetime import datetime

from ..domain import utc_now
from .coverage import close_coverage
from .model import DerivedError, digest, source_ids

SCHEMA = "derived-subscription-index/1"
SUBSCRIPTION_SCHEMA = "derived-query-subscription/1"
HEADER_SCHEMA = "derived-candidate-header/3"
SCOPE_KEY = "route:scope"
SCOPE_BARRIER = "route:scope-generation"
FALLBACK_BARRIER = "route:fallback"
SUBSCRIPTION_BARRIER = "route:subscription-generation"
MAX_DEFINITIONS = 128
MAX_HEADERS = 4096


def candidate_owner(key):
    return "route:candidate:" + key


def subscription_owner(key):
    return "route:subscription:" + key


def source_key(key):
    return "route:source:" + key


def slot_key(key):
    return "route:slot:" + key


def control_key(field, key):
    if field not in {"query_id", "authority_id"}:
        raise DerivedError("derived_subscription_control_unsupported")
    return "route:" + field + ":" + key


def parent_key(key):
    return "route:parent:" + key


def header_data(record_id, event_id, slot, payload, version):
    from .project_index import routing

    return dict(
        project=routing(payload),
        schema=HEADER_SCHEMA,
        generation=version,
        id=record_id,
        event_id=event_id,
        source_ids=sorted({event_id, *source_ids(payload)}),
        version=version,
        claim_id=payload.get("claim_id"),
        slot_key=slot,
    )


def keys_for(definition):
    # Only canonical admission slots are admitted; reserved route: proof keys can
    # never alias a real slot (atom:<sha256>) or published provenance owner.
    if any(
        not key.startswith("atom:")
        or len(key) != 69
        or any(char not in "0123456789abcdef" for char in key[5:])
        for key in definition["slots"]
    ):
        raise DerivedError("derived_subscription_slot_unsupported")
    keys = {SCOPE_KEY, *(slot_key(key) for key in definition["slots"])}
    spec = definition["spec"]
    if spec.get("schema") == "question-instance-registration/1":
        from .project_index import query_keys

        keys.update(query_keys(spec["contract_fingerprint"], spec["project_id"]))
    for field in ("query_id", "authority_id"):
        if spec.get(field) is not None:
            keys.add(control_key(field, spec[field]))
    keys.update(parent_key(key) for key in spec.get("parent_facets", ()))
    return sorted(keys)


async def bump(uow, scope, key):
    row = await uow.derived_get(scope, "barrier", key) or {"generation": 0}
    generation = row["generation"] + 1
    await uow.derived_put(scope, "barrier", key, {"generation": generation})
    return generation


async def install(uow, scope, definition):
    """Install even an empty query before a refresh target or snapshot is captured."""
    facet_id = definition["facet_id"]
    old = await uow.derived_get(scope, "subscription", facet_id)
    keys = [] if definition.get("disabled") else keys_for(definition)
    row = dict(
        schema=SUBSCRIPTION_SCHEMA,
        facet_id=facet_id,
        definition_generation=definition["generation"],
        definition_sha256=definition["fingerprint"],
        keys=keys,
        generation=(old or {}).get("generation", 0) + 1,
    )
    row["sha256"] = digest(row)
    await uow.derived_put(scope, "subscription", facet_id, row)
    await uow.derived_edges(scope, subscription_owner(facet_id), [("query", key) for key in keys])
    return row


async def ensure_index(uow, scope):
    """Bounded one-time/backfill census; normal writes only perform point lookups.

    An oversized candidate census stays in explicit, metered scope fallback. Old
    binaries cannot concurrently write this schema: draining them is a prerequisite
    of deploying these hooks, not something this metadata gate can enforce for them.
    """
    await uow.lock_admission_scope(scope)
    project_index = getattr(uow, "derived_project_index", None)
    if callable(project_index):
        await project_index(scope)
    row = await uow.derived_get(scope, "subscription_index", "scope")
    if row is not None and row.get("schema") != SCHEMA:
        raise DerivedError("derived_subscription_schema_unsupported")
    if row is not None and row.get("state") == "ready":
        return row
    definitions = await uow.derived_records(scope, "definition")
    if len(definitions) > MAX_DEFINITIONS:
        raise DerivedError("derived_subscription_capacity")
    # Gate and conservative barrier are committed atomically with the full index.
    await bump(uow, scope, FALLBACK_BARRIER)
    for item in definitions:
        definition = item["payload"]
        await install(uow, scope, definition)
        if not definition.get("disabled"):
            coverage = await uow.derived_get(scope, "history_interval", item["identity"])
            if coverage and coverage.get("state") != "erased":
                # Migration cannot certify an unobserved interval. Close at the
                # last proven boundary, not a possibly different host wall clock.
                await close_coverage(
                    uow,
                    scope,
                    item["identity"],
                    at=datetime.fromisoformat(coverage["last_at"]),
                    reason="subscription_backfill",
                )
            definition["dirty"] = True
            await uow.derived_put(scope, "definition", item["identity"], definition)
            from ..operations.refresh_demand import record_dirty

            await record_dirty(uow, scope, definition, reason="subscription_backfill")
    headers = await uow.derived_headers(scope)
    mode = "indexed" if len(headers) <= MAX_HEADERS else "scope"
    if mode == "indexed":
        for header in headers:
            await uow.derived_edges(
                scope,
                candidate_owner(header["id"]),
                [("query", source_key(key)) for key in header["source_ids"]],
            )
    row = dict(
        schema=SCHEMA,
        state="ready",
        source_mode=mode,
        generation=(row or {}).get("generation", 0) + 1,
        fallback_count=(row or {}).get("fallback_count", 0),
    )
    await uow.derived_put(scope, "subscription_index", "scope", row)
    return row


async def subscription(uow, scope, definition):
    await ensure_index(uow, scope)
    row = await uow.derived_get(scope, "subscription", definition["facet_id"])
    if (
        row is None
        or row.get("schema") != SUBSCRIPTION_SCHEMA
        or row.get("definition_generation") != definition["generation"]
        or row.get("definition_sha256") != definition["fingerprint"]
        or row.get("keys") != keys_for(definition)
        or digest({key: value for key, value in row.items() if key != "sha256"})
        != row.get("sha256")
    ):
        raise DerivedError("derived_subscription_unavailable")
    return row


async def consumers(uow, scope, keys):
    """Only typed routing owners are accepted; published provenance is disjoint."""
    owners = set()
    for key in keys:
        owners.update(await uow.derived_reverse(scope, key))
    if len(owners) > MAX_DEFINITIONS:
        raise DerivedError("derived_subscription_capacity")
    rows = {}
    prefix = "route:subscription:"
    for owner in sorted(owners):
        if not owner.startswith(prefix):
            raise DerivedError("derived_subscription_integrity_failed")
        facet_id = owner[len(prefix) :]
        definition = await uow.derived_get(scope, "definition", facet_id)
        if not definition or definition.get("disabled"):
            continue
        proof = await subscription(uow, scope, definition)
        if not set(keys).intersection(proof["keys"]):
            raise DerivedError("derived_subscription_integrity_failed")
        rows[facet_id] = definition
    return rows


async def invalidate(uow, scope, keys, *, at=None, reason="candidate", safety=False):
    await ensure_index(uow, scope)
    rows = await consumers(uow, scope, keys)
    affected = set()
    while rows:
        next_keys = []
        for facet_id, definition in rows.items():
            if facet_id in affected:
                continue
            affected.add(facet_id)
            if not safety:
                await close_coverage(uow, scope, facet_id, at=at or utc_now(), reason=reason)
            definition["dirty"] = True
            if safety:
                definition["safety_generation"] += 1
            await uow.derived_put(scope, "definition", facet_id, definition)
            from ..operations.refresh_demand import record_dirty

            await record_dirty(uow, scope, definition, at=at, reason=reason)
            next_keys.append(parent_key(facet_id))
        rows = await consumers(uow, scope, next_keys) if next_keys else {}
    return affected


async def scope_fallback(uow, scope, *, at=None, reason, safety=False):
    row = deepcopy(await ensure_index(uow, scope))
    row["fallback_count"] += 1
    row["last_fallback_reason"] = reason
    await uow.derived_put(scope, "subscription_index", "scope", row)
    await bump(uow, scope, FALLBACK_BARRIER)
    return await invalidate(uow, scope, (SCOPE_KEY,), at=at, reason=reason, safety=safety)


async def source_slots(uow, scope, source_id):
    gate = await ensure_index(uow, scope)
    if gate["source_mode"] != "indexed":
        return None
    try:
        owners = await uow.derived_reverse(scope, source_key(source_id))
    except ValueError:
        return None  # indexed fan-out exceeded its bounded envelope
    slots = set()
    prefix = "route:candidate:"
    for owner in owners:
        if not owner.startswith(prefix):
            raise DerivedError("derived_subscription_integrity_failed")
        header = await uow.derived_header(scope, owner[len(prefix) :])
        if not header or source_id not in header["source_ids"]:
            raise DerivedError("derived_subscription_integrity_failed")
        slots.add(header["slot_key"])
    return slots
