"""Host-only, bounded reachability retention for immutable QuestionView objects.

Receipts have no expiry contract. They are roots forever (including their finite
proof), not an age-based deletion opportunity. This collector is deliberately
conservative: unknown kinds, incomplete censuses and unsupported candidate rows
cannot authorize deletion. A host must supply its legal/policy/history holds.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import datetime

from .model import DerivedError, digest, identity
from .question_model import QuestionCertificate, QuestionContent
from .question_pages import PAGE_KINDS, checked

CONTRACT = "question-reachability-gc/1"
REVISION_KINDS = (
    "question_content",
    "question_certificate",
    "question_page_content",
    "question_page_certificate",
    "question_page_block",
)
COLLECTIBLE_KINDS = (*REVISION_KINDS, "job", "refresh_execution", "refresh_publication")
# New persisted kinds must first declare their reference/lifecycle semantics.
KNOWN_KINDS = frozenset(
    (
        *COLLECTIBLE_KINDS,
        *PAGE_KINDS,
        "authority",
        "barrier",
        "coverage_request",
        "definition",
        "grant",
        "head",
        "history_interval",
        "history_point",
        "model_authorization",
        "model_cache_body",
        "model_cache_header",
        "model_flight",
        "model_processing_grant",
        "page_block",
        "project_index",
        "query",
        "question_change_log",
        "question_delta_state",
        "question_head",
        "question_registration",
        "refresh_demand",
        "refresh_policy",
        "request",
        "revision",
        "revision_header",
        "subscription",
        "subscription_index",
    )
)
V7_SCHEMAS = {
    **{
        kind: kind.replace("_", "-") + "/1"
        for kind in (
            *REVISION_KINDS,
            *PAGE_KINDS,
            "question_registration",
            "refresh_demand",
            "refresh_execution",
            "refresh_publication",
        )
    },
    "question_head": "question-head-proof/1",
    "question_delta_state": "project-keyed-delta/1",
    "question_change_log": "project-change-window/1",
    "refresh_policy": "refresh-policy-binding/1",
    "coverage_request": "coverage-receipt/1",
}


@dataclass(frozen=True, slots=True)
class QuestionRetentionPolicy:
    """Explicit host authorization for *unreferenced* objects in this exact scope.

    The host must resolve legal and historical-retention requirements before
    invoking GC, retaining opaque object/instance IDs where needed. No transport
    accepts this policy. No hold overrides erase; erased tombstones stay intact.
    Bounds limit the complete census and atomic deletion batch independently.
    """

    policy_id: str
    retain_ids: tuple[str, ...] = ()
    retain_instances: tuple[str, ...] = ()
    max_records: int = 32768
    max_edges: int = 131072
    max_delete: int = 256
    max_bytes: int = 67108864

    def __post_init__(self):
        identity(self.policy_id)
        for values in (self.retain_ids, self.retain_instances):
            if type(values) is not tuple or len(values) > 4096 or len(set(values)) != len(values):
                raise DerivedError("invalid_question_retention_policy")
            for value in values:
                identity(value)
        for key, upper in (
            ("max_records", 65536),
            ("max_edges", 262144),
            ("max_delete", 4096),
            ("max_bytes", 268435456),
        ):
            value = getattr(self, key)
            if type(value) is not int or not 1 <= value <= upper:
                raise DerivedError("invalid_question_retention_policy")


def census_limits(max_records, max_edges, max_bytes):
    """Validate optional storage extension limits before SQL."""
    QuestionRetentionPolicy(
        "census", max_records=max_records, max_edges=max_edges, max_bytes=max_bytes
    )


def _strings(value):
    """Inspect keys too: finite receipt publications and generation parents use them."""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            stack.extend(item)
            stack.extend(item.values())
        elif isinstance(item, (list, tuple)):
            stack.extend(item)


def _instance(kind, key, row, scope):
    if kind == "question_content":
        content = QuestionContent.from_payload(row)
        if content.id != key or content.instance.definition.scope != scope:
            raise ValueError("invalid content")
        return content.instance.id
    if kind == "question_certificate":
        certificate = QuestionCertificate.from_payload(row)
        if certificate.id != key or certificate.scope != scope:
            raise ValueError("invalid certificate")
        return certificate.instance_id
    if kind in REVISION_KINDS:
        checked(row, kind.replace("_", "-") + "/1")
        if row.get("id") != key or not row.get("instance_id"):
            raise ValueError("invalid page revision")
        return row["instance_id"]
    return row.get("unit", {}).get("facet_id", row.get("facet_id"))


def _past_lease(row, now):
    # checked_job(completed=True) supports late acknowledgement until expires_at,
    # even after lease_until. Retain that proof throughout both finite fences.
    values = [row.get("lease_until"), row.get("expires_at")]
    if not all(values):
        return False
    fences = [datetime.fromisoformat(value) for value in values]
    return all(at.utcoffset() is not None and at <= now for at in fences)


def _at_capacity(remaining):
    return sorted(
        kind
        for kind in (*REVISION_KINDS, "job", "request", "coverage_request")
        if remaining.get(kind, 0) >= 4096
    )


def plan(scope, policy, snapshot, now):
    """Pure exact-census plan; no prefix/age assumption establishes reachability."""
    rows = {(r["kind"], r["identity"]): r["payload"] for r in snapshot["rows"]}
    remaining = dict(sorted(Counter(kind for kind, _ in rows).items()))
    result = dict(
        schema="question-gc-result/1",
        policy_id=policy.policy_id,
        state="unchanged",
        reason=None,
        inspected_records=len(rows),
        candidates=0,
        deleted=[],
        retained={},
        roots={},
        deferred=0,
        remaining=remaining,
        at_capacity=_at_capacity(remaining),
        receipt_retention="indefinite",
    )
    if any(kind not in KNOWN_KINDS for kind, _ in rows):
        return {**result, "state": "deferred", "reason": "question_gc_kind_unsupported"}
    candidates = set()
    roots, reasons = set(), Counter()

    def pin(node, reason):
        roots.add(node)
        reasons[reason] += 1

    try:
        for node, row in rows.items():
            kind, key = node
            if not isinstance(row, dict):
                raise ValueError("invalid row")
            if (
                kind in V7_SCHEMAS
                and row.get("state") != "erased"
                and row.get("status") != "erased"
                and row.get("schema") != V7_SCHEMAS[kind]
            ):
                raise ValueError("unsupported v7 schema")
            if kind not in COLLECTIBLE_KINDS or row.get("state") == "erased":
                pin(node, "retained_record")
                continue
            instance = _instance(kind, key, row, scope)
            if kind in REVISION_KINDS:
                candidates.add(node)
            elif kind == "job":
                if not key.startswith("question-unit:") or row.get("status") != "completed":
                    pin(node, "unfinished_or_legacy")
                    continue
                from ..operations.facet_refresh import valid_completion

                if not valid_completion(scope, row) or row.get("id") != key:
                    raise ValueError("invalid completed job")
                candidates.add(node)
                if not _past_lease(row, now):
                    pin(node, "lease_fence")
            else:
                execution = rows.get(("refresh_execution", key))
                if (
                    not execution
                    or execution.get("schema") != "refresh-execution/1"
                    or not execution.get("adapter_key", "").startswith("question-refresh/2:")
                    or execution.get("status") != "completed"
                ):
                    pin(node, "unfinished_or_legacy")
                    continue
                if kind == "refresh_publication" and (
                    row.get("schema") != "refresh-publication/1"
                    or row.get("state") != "committed"
                    or row.get("sha256") != digest({k: v for k, v in row.items() if k != "sha256"})
                ):
                    raise ValueError("invalid publication")
                candidates.add(node)
                if not _past_lease(execution, now):
                    pin(node, "lease_fence")
            if key in policy.retain_ids or instance in policy.retain_instances:
                pin(node, "host_retention")
    except (DerivedError, KeyError, TypeError, ValueError, RecursionError):
        return {**result, "state": "deferred", "reason": "question_gc_record_unsupported"}

    aliases = defaultdict(set)
    owners = defaultdict(set)
    for node in rows:
        owners[node[1]].add(node)
        for alias in (node[1], "derived:" + node[1]):
            aliases[alias].add(node)
    graph = {node: set(owners[node[1]]) for node in rows}
    graph_links = 0
    for node, row in rows.items():
        for value in _strings(row):
            graph[node].update(aliases.get(value, ()))
        graph_links += len(graph[node])
        if graph_links > policy.max_edges:
            return {**result, "state": "deferred", "reason": "question_gc_census_limit"}
    for edge in snapshot["edges"]:
        targets = aliases.get(edge["parent_id"], ())
        origin = owners.get(edge["revision_id"])
        if origin:
            for node in origin:
                graph[node].update(targets)
        else:
            # External edge owners (model-cache:, route:, legacy) are roots too.
            for node in targets:
                pin(node, "external_dependency")
    if sum(len(values) for values in graph.values()) > policy.max_edges:
        return {**result, "state": "deferred", "reason": "question_gc_census_limit"}
    for execution_id in snapshot["reservations"]:
        for node in owners.get(execution_id, ()):
            pin(node, "scheduler_reservation")

    # Demand units are obligation IDs, not revision IDs. A coincidentally
    # completed execution may not discard still-undischarged responsibility.
    outstanding = set()
    head_units = set()
    for (kind, _), row in rows.items():
        if kind == "refresh_demand":
            outstanding.update(row.get("requested", ()))
        if kind == "question_head" and row.get("unit"):
            head_units.add(digest(row["unit"]))
    for node in candidates:
        row = rows[node]
        if node[0] == "refresh_execution" and outstanding.intersection(row.get("claimed", ())):
            pin(node, "outstanding_obligation")
        if node[0] == "job" and digest(row.get("unit")) in head_units:
            pin(node, "current_unit")

    reachable, todo = set(roots), list(roots)
    while todo:
        for target in graph[todo.pop()] - reachable:
            reachable.add(target)
            todo.append(target)
    garbage = candidates - reachable
    # Delete complete unreachable components. A bounded batch must never leave
    # a deferred candidate referring to an object removed in the same batch.
    neighbors = {node: graph[node] & garbage for node in garbage}
    for node in garbage:
        for target in tuple(neighbors[node]):
            neighbors[target].add(node)
    selected, seen = [], set()
    for start in sorted(garbage):
        if start in seen:
            continue
        component, todo = set(), [start]
        while todo:
            node = todo.pop()
            if node in component:
                continue
            component.add(node)
            todo.extend(neighbors[node] - component)
        seen.update(component)
        if len(selected) + len(component) <= policy.max_delete:
            selected.extend(sorted(component))
    result.update(
        candidates=len(candidates),
        deleted=sorted(selected),
        retained=dict(sorted(Counter(node[0] for node in candidates & reachable).items())),
        roots=dict(sorted(reasons.items())),
        deferred=len(garbage) - len(selected),
    )
    remaining = Counter(kind for kind, _ in rows)
    remaining.subtract(kind for kind, _ in selected)
    result["remaining"] = dict(sorted(remaining.items()))
    result["at_capacity"] = _at_capacity(remaining)
    if result["deferred"]:
        result.update(state="deferred", reason="question_gc_batch_limit")
    elif selected:
        result["state"] = "collected"
    elif candidates:
        result.update(state="deferred", reason="question_gc_references_retained")
    return result


async def collect(service, policy):
    if type(policy) is not QuestionRetentionPolicy:
        raise DerivedError("invalid_question_retention_policy")
    policy = replace(policy)  # Own the host policy before the first lock wait.
    async with service.repository.unit_of_work() as uow:
        if getattr(uow, "question_gc_contract", None) != CONTRACT or any(
            not callable(getattr(uow, name, None))
            for name in ("derived_gc_snapshot", "derived_gc_delete")
        ):
            raise DerivedError("question_gc_backend_unsupported")
        await service._open(uow)
        from ..operations.refresh_demand import observed_clock

        now = await observed_clock(uow, service.scope, service.clock)
        snapshot = await uow.derived_gc_snapshot(
            service.scope,
            max_records=policy.max_records,
            max_edges=policy.max_edges,
            max_bytes=policy.max_bytes,
        )
        if snapshot is None:
            return dict(
                schema="question-gc-result/1",
                policy_id=policy.policy_id,
                state="deferred",
                reason="question_gc_census_limit",
                inspected_records=None,
                candidates=None,
                deleted=[],
                retained={},
                roots={},
                deferred=None,
                remaining=None,
                at_capacity=None,
                receipt_retention="indefinite",
            )
        result = plan(service.scope, policy, snapshot, now)
        for kind, key in result["deleted"]:
            await uow.derived_gc_delete(service.scope, kind, key)
        return result
