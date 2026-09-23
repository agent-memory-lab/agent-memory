"""Host-authorized schema registration; projection migration remains explicit."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from .ontology_memory import OntologySchema
from .ontology_schema import (
    OntologyMigrationPlan,
    ontology_schema_digest,
    plan_ontology_migration,
)


class SchemaRegistrationStore(Protocol):
    async def register_schema(self, schema: OntologySchema) -> None: ...


@dataclass(frozen=True, slots=True)
class OntologyUpgradeRequest:
    """Exact document identities presented to an authenticated host policy."""

    previous_digest: str
    target_digest: str
    plan: OntologyMigrationPlan


class OntologyUpgradeAuthorizer(Protocol):
    """Host checks authenticated approval, scoped to the supplied request.

    Implementations must not accept an approver name supplied by an agent as
    proof of authorization. Persist approval evidence in the host audit store.
    """

    async def authorize(self, request: OntologyUpgradeRequest) -> bool: ...


@dataclass(frozen=True, slots=True)
class OntologyUpgradeReceipt:
    request: OntologyUpgradeRequest
    host_authorized: bool
    pending_actions: tuple[str, ...]
    status: str = "schema_registered"


async def register_ontology_upgrade(
    store: SchemaRegistrationStore,
    previous: OntologySchema,
    target: OntologySchema,
    *,
    authorizer: OntologyUpgradeAuthorizer | None = None,
    approval_timeout_ms: int = 2_000,
) -> OntologyUpgradeReceipt:
    """Register a version after policy checks, without activating or migrating it.

    The trusted host supplies the baseline and an initialized store. This API
    does not infer the active version, authenticate callers, or persist receipts.
    If provided, the host authorizer can veto compatible upgrades as well.
    Store failures propagate; a receipt is returned only after registration.
    """
    if type(approval_timeout_ms) is not int or not 1 <= approval_timeout_ms <= 60_000:
        raise ValueError("approval_timeout_ms must be between 1 and 60000")
    plan = plan_ontology_migration(previous, target)
    if not plan.version_policy_valid:
        raise ValueError(f"schema upgrade requires a {plan.required_version_bump} version bump")
    request = OntologyUpgradeRequest(
        previous_digest=ontology_schema_digest(previous),
        target_digest=ontology_schema_digest(target),
        plan=plan,
    )
    if plan.diff.requires_approval and authorizer is None:
        raise PermissionError("breaking schema upgrade requires host authorization")
    host_authorized = False
    if authorizer is not None:
        decision = await asyncio.wait_for(
            authorizer.authorize(request), timeout=approval_timeout_ms / 1_000
        )
        if decision is not True:
            raise PermissionError("host did not authorize this schema upgrade")
        host_authorized = True
    await store.register_schema(target)
    return OntologyUpgradeReceipt(
        request=request,
        host_authorized=host_authorized,
        pending_actions=tuple(
            action for action in plan.actions
            if action not in {"require_host_approval", "register_schema_version"}
        ),
    )
