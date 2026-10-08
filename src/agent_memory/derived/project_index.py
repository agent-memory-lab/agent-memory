"""Project census selectors and transaction-bound routing, separate from atom slots.

Deployment is a coordinated writer upgrade. Drain old writers before enabling
these hooks. The first new writer/read rebuilds each exact scope under its existing
namespace lock; oversized legacy scopes remain explicitly incomplete and fenced.
"""

from .model import DerivedError, digest

INDEX_SCHEMA = "derived-project-index/1"
MAX_BACKFILL = 4096
WILDCARD_KEY = "route:project-wildcard"
PROJECT_PREFIX = "route:project:"
UNBOUND_PREFIX = "route:project-unbound:"


def project_key(contract, project_id):
    if (
        not isinstance(contract, str)
        or not contract
        or not isinstance(project_id, str)
        or not project_id
    ):
        raise DerivedError("project_candidate_selector_invalid")
    return PROJECT_PREFIX + digest([contract, project_id])


def unbound_key(contract):
    if not isinstance(contract, str) or not contract:
        raise DerivedError("project_candidate_selector_invalid")
    return UNBOUND_PREFIX + digest(contract)


def query_keys(contract, project_id):
    return tuple(sorted((project_key(contract, project_id), unbound_key(contract), WILDCARD_KEY)))


def routing(payload):
    # Import lazily: admission/registry/subscriptions also use this metadata module.
    from ..consolidation.project_admission import project_header

    try:
        value = project_header(payload)
    except (KeyError, TypeError, AttributeError) as error:
        raise DerivedError("project_candidate_header_invalid") from error
    return checked_project(value)


def checked_project(value):
    from ..consolidation.project_admission import CANDIDATE_SCHEMA

    if value is None:
        return None
    if (
        not isinstance(value, dict)
        or value.get("schema") != CANDIDATE_SCHEMA
        or type(value.get("was_unbound")) is not bool
        or type(value.get("project_ids")) is not list
        or len(value["project_ids"]) > 64
        or any(type(key) is not str or not key for key in value["project_ids"])
        or value["project_ids"] != sorted(set(value["project_ids"]))
    ):
        raise DerivedError("project_candidate_header_invalid")
    contract, current = value.get("contract_fingerprint"), value.get("current_project_id")
    if contract is None:
        if value != {
            "schema": CANDIDATE_SCHEMA,
            "contract_fingerprint": None,
            "current_project_id": None,
            "project_ids": [],
            "was_unbound": True,
            "unreviewed": True,
        }:
            raise DerivedError("project_candidate_header_invalid")
    elif (
        type(contract) is not str
        or not contract
        or (current is not None and current not in value["project_ids"])
        or (current is None and not value["was_unbound"])
    ):
        raise DerivedError("project_candidate_header_invalid")
    return value


def header_keys(header):
    if not header or header.get("project") is None:
        return ()
    project = checked_project(header["project"])
    contract = project["contract_fingerprint"]
    if contract is None:
        return (WILDCARD_KEY,)
    keys = {project_key(contract, key) for key in project["project_ids"]}
    if project["was_unbound"]:
        keys.add(unbound_key(contract))
    return tuple(sorted(keys))


def erasure_header_keys(header):
    """Deletion cannot require trusting an obsolete or damaged routing proof.

    Ordinary reads/writes still reject unsupported headers. If an affected row's
    selectors cannot be verified, retire all subscribed project questions in the
    affected scope instead of retaining their sensitive, possibly empty routes.
    """
    try:
        return header_keys(header)
    except (DerivedError, KeyError, TypeError, AttributeError):
        return (WILDCARD_KEY,)


def checked_gate(row):
    if row is None:
        return None
    if not isinstance(row, dict) or row.get("schema") != INDEX_SCHEMA:
        raise DerivedError("project_candidate_index_schema_unsupported")
    if type(row.get("generation")) is not int or row["generation"] < 1:
        raise DerivedError("project_candidate_index_version_invalid")
    if row.get("state") not in {"ready", "needs_backfill", "scope"}:
        raise DerivedError("project_candidate_index_incomplete")
    return row


def checked_headers(values, contract, project_id):
    from .subscriptions import HEADER_SCHEMA

    keys = set(query_keys(contract, project_id))
    result = tuple(values)
    for row in result:
        if (
            not isinstance(row, dict)
            or row.get("schema") != HEADER_SCHEMA
            or type(row.get("version")) is not int
            or row["version"] < 1
            or type(row.get("generation")) is not int
            or row.get("generation") != row["version"]
            or not isinstance(row.get("id"), str)
            or not isinstance(row.get("source_ids"), list)
            or any(type(key) is not str or not key for key in row["source_ids"])
            or row["source_ids"] != sorted(set(row["source_ids"]))
            or not keys.intersection(header_keys(row))
        ):
            raise DerivedError("project_candidate_census_invalid")
    if len({row["id"] for row in result}) != len(result):
        raise DerivedError("project_candidate_census_invalid")
    return result


async def changed(uow, scope, old_header, new_header, *, at=None, reason="candidate"):
    """Bump query generations even when no project has been registered/read yet."""
    from . import subscriptions

    keys = set(header_keys(old_header)) | set(header_keys(new_header))
    for key in sorted(keys):
        await subscriptions.bump(uow, scope, key)
    gate = checked_gate(await uow.derived_get(scope, "project_index", "scope"))
    if gate and gate["state"] != "ready":
        await subscriptions.scope_fallback(uow, scope, at=at, reason="project_index_incomplete")
    elif keys:
        await subscriptions.invalidate(uow, scope, tuple(sorted(keys)), at=at, reason=reason)


async def source_changed(uow, scope, source_id, *, at=None, reason="source", safety=False):
    """Use existing source reverse edges; never enumerate candidate bodies."""
    from . import subscriptions

    gate = await subscriptions.ensure_index(uow, scope)
    project_gate = checked_gate(await uow.derived_get(scope, "project_index", "scope"))
    if gate["source_mode"] != "indexed" or (project_gate and project_gate["state"] != "ready"):
        await subscriptions.scope_fallback(uow, scope, at=at, reason=reason, safety=safety)
        return
    try:
        owners = await uow.derived_reverse(scope, subscriptions.source_key(source_id))
    except ValueError:
        await subscriptions.scope_fallback(uow, scope, at=at, reason=reason, safety=safety)
        return
    keys = set()
    prefix = "route:candidate:"
    for owner in owners:
        if not owner.startswith(prefix):
            raise DerivedError("derived_subscription_integrity_failed")
        header = await uow.derived_header(scope, owner[len(prefix) :])
        if not header or source_id not in header["source_ids"]:
            raise DerivedError("derived_subscription_integrity_failed")
        keys.update(header_keys(header))
    for key in sorted(keys):
        await subscriptions.bump(uow, scope, key)
    if keys:
        await subscriptions.invalidate(
            uow, scope, tuple(sorted(keys)), at=at, reason=reason, safety=safety
        )


def source_proof(source_id, content_hash, retained, head):
    if retained is not None and (
        not isinstance(retained, dict)
        or not isinstance(retained.get("document_id"), str)
        or type(retained.get("revision")) is not int
        or retained["revision"] < 1
        or head is None
    ):
        raise DerivedError("project_source_revision_changed")
    return dict(
        source_event_id=source_id,
        source_sha256=content_hash,
        document_id=retained["document_id"] if retained else None,
        revision=retained["revision"] if retained else None,
        document_head_generation=head["generation"] if head else None,
        document_head_event_id=head["payload"]["event_id"] if head else None,
        document_head_sha256=digest(head) if head else None,
    )
