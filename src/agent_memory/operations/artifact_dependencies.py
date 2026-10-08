"""Typed, scope-checked artifact/feedback dependencies for evidence withdrawal."""

from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from typing import Any

MemoryKey = tuple[str, str]


def affected_memory_keys(
    rows: Sequence[Mapping[str, Any]], memory_keys: set[MemoryKey]
) -> set[MemoryKey]:
    """Close the complete graph before any feedback payload is redacted.

    IDs are only unique within a table. Typed provenance fields must not match
    another table's identity, and dependencies can only use visible source scopes.
    Callers provide an authoritative tenant/namespace snapshot, including archived
    artifacts and feedback so ARCHIVE followed by ERASE retains its dependency path.
    """
    by_key = {(row["table"], row["id"]): row for row in rows}
    dependents: dict[MemoryKey, set[MemoryKey]] = defaultdict(set)
    memory_tables = ("events", "claims", "artifacts")
    list_fields = {
        "event_ids": ("events",),
        "source_event_ids": ("events",),
        "source_episode_ids": ("artifacts",),
        "counterexample_episode_ids": ("artifacts",),
        "procedure_ids": ("artifacts",),
        "used_memory_ids": memory_tables,
        "returned_memory_ids": memory_tables,
        "memory_ids": memory_tables,
        "decision_ids": ("feedback",),
        "outcome_ids": ("feedback",),
        "retrieval_trace_ids": ("feedback",),
    }

    def visible(source, target):
        return (
            source["tenant_id"] == target["tenant_id"]
            and source["namespace"] == target["namespace"]
            and all(
                source[field] is None or source[field] == target[field]
                for field in (
                    "user_id",
                    "agent_id",
                    "workspace_id",
                    "session_id",
                )
            )
        )

    for key, row in by_key.items():
        if row["table"] not in {"artifacts", "feedback"}:
            continue
        payload = row.get("payload", {})
        payload = payload if isinstance(payload, Mapping) else {}
        provenance = row.get("provenance", {})
        provenance = provenance if isinstance(provenance, Mapping) else {}
        references: set[MemoryKey] = set()

        def add(values, tables, references=references):
            if isinstance(values, (list, tuple)):
                references.update(
                    (table, identity)
                    for identity in values
                    if isinstance(identity, str)
                    for table in tables
                )

        add(provenance.get("source_event_ids", ()), ("events",))
        for field, tables in list_fields.items():
            add(payload.get(field, ()), tables)
        for field in ("bundle_id", "decision_id", "outcome_id", "evaluation_id", "corrects_id"):
            add((payload.get(field),), ("feedback",))
        add((row.get("parent_id"),), ("feedback",))
        for source_key in references:
            source = by_key.get(source_key)
            if source is not None and visible(source, row):
                dependents[source_key].add(key)
    affected = set(by_key) & memory_keys
    pending = deque(affected)
    while pending:
        for key in dependents.get(pending.popleft(), ()):
            if key not in affected:
                affected.add(key)
                pending.append(key)
    return affected


def dependency_ids(row: Mapping[str, Any]) -> set[str]:
    """IDs to hydrate for bounded local dependency validation."""
    identities = {identity for _, identity, _ in _dependency_references(row)}
    payload = row.get("payload", {})
    if isinstance(payload, Mapping) and isinstance(payload.get("corrects_id"), str):
        identities.add(payload["corrects_id"])
    return identities


def _dependency_references(row):
    payload = row.get("payload", {})
    payload = payload if isinstance(payload, Mapping) else {}
    provenance = row.get("provenance", {})
    provenance = provenance if isinstance(provenance, Mapping) else {}
    fields = {
        "event_ids": (("events",), True),
        "source_event_ids": (("events",), True),
        "source_episode_ids": (("artifacts",), True),
        "counterexample_episode_ids": (("artifacts",), True),
        "decision_ids": (("feedback",), True),
        "outcome_ids": (("feedback",), True),
        "retrieval_trace_ids": (("feedback",), True),
        # These polymorphic IDs may come from a separately installed recall provider.
        "used_memory_ids": (("events", "claims", "artifacts"), False),
        "memory_ids": (("events", "claims", "artifacts"), False),
        "returned_memory_ids": (("events", "claims", "artifacts"), False),
        "procedure_ids": (("artifacts",), False),
    }
    for field, (tables, required) in fields.items():
        values = payload.get(field, ())
        if isinstance(values, (list, tuple)):
            for identity in values:
                if isinstance(identity, str):
                    yield tables, identity, required
    sources = provenance.get("source_event_ids", ())
    if isinstance(sources, (list, tuple)):
        for identity in sources:
            if isinstance(identity, str):
                yield ("events",), identity, True
    # A correction may legitimately replace a superseded predecessor. Its lineage
    # is an erasure edge, not an active-parent requirement for new observations.
    for field in ("bundle_id", "decision_id", "outcome_id", "evaluation_id"):
        identity = payload.get(field)
        if isinstance(identity, str) and not (
            field == "bundle_id" and row["table"] == "feedback" and identity == row["id"]
        ):
            yield ("feedback",), identity, True


def _visible(source, target):
    return (
        source["tenant_id"] == target["tenant_id"]
        and source["namespace"] == target["namespace"]
        and all(
            source[field] is None or source[field] == target[field]
            for field in (
                "user_id",
                "agent_id",
                "workspace_id",
                "session_id",
            )
        )
    )


class ArtifactValidity:
    """Validate hydrated local dependencies, preserving unknown external IDs."""

    def __init__(self, rows):
        self.rows = {
            (row["table"], row["id"]): row for row in rows if row["table"] != "memory_tombstones"
        }
        self.tombstones = tuple(row for row in rows if row["table"] == "memory_tombstones")
        self.memo = {}
        self.visiting = set()

    def accepts(self, row, *, writing=False):
        key = (row["table"], row["id"])
        if writing:
            # Validate replacement dependencies rather than the previous version.
            self.rows[key] = row
            self.memo.clear()
            return self._valid(row, writing=True)
        return self._valid(row)

    def _valid(self, row, *, writing=False, lineage=False):
        key = (row["table"], row["id"], lineage)
        if key in self.memo and not writing:
            return self.memo[key]
        if key in self.visiting:
            return False
        self.visiting.add(key)
        try:
            valid = self._check(row, writing=writing, lineage=lineage)
        finally:
            self.visiting.remove(key)
        if not writing:
            self.memo[key] = valid
        return valid

    def _check(self, row, *, writing, lineage):
        table = row["table"]
        if row.get("archived_at") is not None or row.get("invalidated_at") is not None:
            return False
        if table == "events":
            return True
        if table == "claims":
            return row.get("status") not in {"archived", "rejected"}
        if table == "feedback":
            allowed = {"accepted", "superseded"} if lineage else {"accepted"}
            if row.get("feedback_status") not in allowed or row.get("payload", {}).get("redacted"):
                return False
            previous_id = row.get("payload", {}).get("corrects_id")
            if previous_id:
                previous = self.rows.get(("feedback", previous_id))
                if (
                    previous is None
                    or not _visible(previous, row)
                    or not self._valid(previous, lineage=True)
                ):
                    return False
        if table == "artifacts":
            if any(
                t["memory_table"] == "artifacts"
                and t["id"] == row["id"]
                and t["partition_key"] == row["partition_key"]
                for t in self.tombstones
            ):
                return False
            if not writing and row.get("status") not in {"active", "candidate"}:
                return False
            payload = row.get("payload", {})
            provenance = row.get("provenance", {})
            if not isinstance(payload, Mapping) or not isinstance(provenance, Mapping):
                return False
            sources = provenance.get("source_event_ids")
            if not isinstance(sources, (list, tuple)) or not sources:
                return False
            if any(not isinstance(identity, str) for identity in sources):
                return False
            if row.get("kind") == "block":
                if not writing and row.get("status") != "active":
                    return False
                if (
                    not isinstance(payload.get("event_ids"), (list, tuple))
                    or not payload["event_ids"]
                ):
                    return False
        for tables, identity, required in _dependency_references(row):
            if any(
                t["memory_table"] in tables and t["id"] == identity and _visible(t, row)
                for t in self.tombstones
            ):
                return False
            candidates = [
                self.rows[(table, identity)]
                for table in tables
                if (table, identity) in self.rows and _visible(self.rows[(table, identity)], row)
            ]
            if candidates:
                if not any(self._valid(candidate) for candidate in candidates):
                    return False
            elif required:
                return False
        return True
