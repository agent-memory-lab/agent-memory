"""Pure contracts for bounded, current-time, source-supported Observations."""

import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from hashlib import sha256

from ..conditions import ContextAttribute, ProjectionPolicy, QueryContext
from ..domain import MemoryScope
from ..serialization import to_jsonable


class DerivedError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def digest(value):
    return sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        ).encode()
    ).hexdigest()


def identity(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise DerivedError("invalid_derived_identity")
    return value


def timestamp(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise DerivedError("derived_timezone_required")
    return value


@dataclass(frozen=True)
class FacetContext:
    """Expiring host routing binding, evaluated at the current time only."""

    query: QueryContext
    policy: ProjectionPolicy
    expires_at: datetime

    def __post_init__(self):
        if not isinstance(self.query, QueryContext) or not isinstance(
            self.policy, ProjectionPolicy
        ):
            raise DerivedError("trusted_facet_context_required")
        timestamp(self.expires_at)
        if (
            self.query.purpose != self.policy.purpose
            or self.query.valid_at != self.query.known_at
            or self.query.snapshot_token == "host-request"
            or not 0 < (self.expires_at - self.query.known_at).total_seconds() <= 86400
        ):
            raise DerivedError("invalid_facet_context")
        # Routing attributes are not an untracked arbitrary memory input channel.
        for attr in self.query.attributes:
            if (
                (attr.name == "project" and not isinstance(attr.value, str))
                or (attr.name == "holiday" and type(attr.value) is not bool)
                or attr.name not in {"project", "holiday"}
            ):
                raise DerivedError("unsupported_facet_context_attribute")

    def payload(self):
        return to_jsonable(self)

    @classmethod
    def from_payload(cls, payload):
        query = dict(payload["query"])
        query["scope"] = MemoryScope(**query["scope"])
        query["attributes"] = tuple(ContextAttribute(**a) for a in query["attributes"])
        for key in ("valid_at", "known_at"):
            query[key] = datetime.fromisoformat(query[key])
        return cls(
            QueryContext(**query),
            ProjectionPolicy(**payload["policy"]),
            datetime.fromisoformat(payload["expires_at"]),
        )

    def current(self, at):
        timestamp(at)
        if at < self.query.known_at:
            raise DerivedError("derived_context_future")
        if at >= self.expires_at:
            raise DerivedError("derived_context_expired")
        return replace(self.query, valid_at=at, known_at=at)


@dataclass(frozen=True)
class FacetDefinition:
    id: str
    subject_id: str
    predicates: tuple[str, ...] = ("locale",)
    facet: str = "communication.language"
    version: str = "1"
    purpose: str = "agent_context"
    readers: tuple[str, ...] = ("alice",)
    template_version: str = "locale-snapshot/1"
    context: FacetContext | None = None
    query_id: str | None = None
    authority_id: str | None = None
    history_mode: str | None = None

    def __post_init__(self):
        for value in (self.id, self.subject_id, self.facet, self.version, self.purpose):
            identity(value)
        if (
            self.facet != "communication.language"
            or self.predicates != ("locale",)
            or self.template_version not in {"locale-snapshot/1", "locale-context/1"}
        ):
            raise DerivedError("unsupported_derived_facet")
        if (self.context is not None and not isinstance(self.context, FacetContext)) or (
            (self.template_version == "locale-context/1") != isinstance(self.context, FacetContext)
        ):
            raise DerivedError("trusted_facet_context_required")
        if self.context is not None and (
            self.context.query.subject_id != self.subject_id
            or self.context.query.purpose != self.purpose
        ):
            raise DerivedError("derived_context_mismatch")
        if (
            not self.readers
            or len(self.readers) > 16
            or len(set(self.readers)) != len(self.readers)
        ):
            raise DerivedError("invalid_derived_readers")
        for reader in self.readers:
            identity(reader)
        for value in (self.query_id, self.authority_id):
            if value is not None:
                identity(value)
        if self.history_mode is not None and (
            self.history_mode != "published-point/1"
            or self.query_id is None
            or self.authority_id is None
        ):
            raise DerivedError("invalid_derived_history_definition")
        if self.history_mode is not None and self.template_version != "locale-snapshot/1":
            raise DerivedError("derived_history_template_unsupported")

    def payload(self):
        values = to_jsonable(self)
        if self.context is None:
            values.pop("context")  # Preserve the deployed v1 definition fingerprint.
        for key in ("query_id", "authority_id", "history_mode"):
            if values[key] is None:
                values.pop(key)
        return values


@dataclass(frozen=True)
class ProcessingGrant:
    source_id: str
    readers: tuple[str, ...]
    purposes: tuple[str, ...] = ("agent_context",)
    sensitivity: str = "private"
    retention_class: str = "session"
    expires_at: datetime | None = None
    revoked: bool = False

    def __post_init__(self):
        identity(self.source_id)
        if type(self.revoked) is not bool:
            raise DerivedError("invalid_processing_grant")
        if (
            not self.readers
            or len(self.readers) > 16
            or not self.purposes
            or len(self.purposes) > 16
        ):
            raise DerivedError("invalid_processing_grant")
        for value in (*self.readers, *self.purposes):
            identity(value)
        if self.sensitivity not in {"public", "private", "restricted"} or (
            self.retention_class not in {"session", "persistent", "ephemeral"}
        ):
            raise DerivedError("invalid_processing_grant")
        if self.expires_at is not None:
            timestamp(self.expires_at)

    def payload(self):
        values = asdict(self)
        values["expires_at"] = self.expires_at.isoformat() if self.expires_at else None
        return json.loads(json.dumps(values))


@dataclass(frozen=True)
class FacetRefreshUnit:
    """Fixed query target, independent of whether any sources remain."""

    facet_id: str
    definition_sha256: str
    definition_generation: int
    epoch: int
    query_generation: dict[str, int]
    safety_generation: int
    time_generation: int
    schema: str = "facet-refresh-unit/1"
    bindings: dict | None = None

    def __post_init__(self):
        identity(self.facet_id)
        if (
            self.schema not in {"facet-refresh-unit/1", "facet-refresh-unit/2"}
            or len(self.definition_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.definition_sha256)
        ):
            raise DerivedError("invalid_facet_refresh_unit")
        if (self.schema == "facet-refresh-unit/2") != (self.bindings is not None):
            raise DerivedError("invalid_facet_refresh_unit")
        if self.bindings is not None:
            if (
                not isinstance(self.bindings, dict)
                or not self.bindings
                or set(self.bindings) - {"query", "authority"}
            ):
                raise DerivedError("invalid_facet_refresh_unit")
            for kind, binding in self.bindings.items():
                generation = "generation" if kind == "query" else "version"
                if (
                    not isinstance(binding, dict)
                    or set(binding) != {"id", generation, "sha256"}
                    or (
                        type(binding[generation]) is not int
                        or binding[generation] < 1
                        or not isinstance(binding["sha256"], str)
                        or len(binding["sha256"]) != 64
                        or any(c not in "0123456789abcdef" for c in binding["sha256"])
                    )
                ):
                    raise DerivedError("invalid_facet_refresh_unit")
                identity(binding["id"])
        for value in (
            self.definition_generation,
            self.epoch,
            self.safety_generation,
            self.time_generation,
        ):
            if type(value) is not int or value < 0:
                raise DerivedError("invalid_facet_refresh_unit")
        if not 1 <= len(self.query_generation) <= 16:
            raise DerivedError("invalid_facet_refresh_unit")
        for key, value in self.query_generation.items():
            identity(key)
            if type(value) is not int or value < 0:
                raise DerivedError("invalid_facet_refresh_unit")

    def payload(self):
        values = asdict(self)
        if self.bindings is None:
            values.pop("bindings")
        return values

    @property
    def id(self):
        return "facet-unit:" + digest(self.payload())


def source_ids(payload):
    result = set()

    def visit(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "source_event_id" and isinstance(item, str):
                    result.add(item)
                elif key == "source_event_ids" and isinstance(item, list):
                    result.update(v for v in item if isinstance(v, str))
                elif isinstance(item, (dict, list)):
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(payload)
    return sorted(result)


def erase_rows(rows, parents, all_in_scope, slots=()):
    """Physical erasure plan, shared by live deletion and backup purge replay."""
    affected = {
        r["payload"].get("facet_id", r["identity"])
        for r in rows
        if r["kind"] == "revision"
        and (
            all_in_scope
            or "derived:" + r["identity"] in parents
            or parents.intersection(r["payload"].get("parents", []))
        )
    }
    # Historical empty coverage also depends on the query, including members
    # erased after that checkpoint. Never revive an older certificate afterward.
    affected.update(
        r["payload"]["facet_id"]
        for r in rows
        if r["kind"] == "history_point"
        and (all_in_scope or set(r["payload"].get("slots", ())).intersection(slots))
    )
    # Active jobs carry identities only; they remain useful after object deletion.
    result = []
    for item in rows:
        kind, key, row = item["kind"], item["identity"], item["payload"]
        if kind == "history_point" and (all_in_scope or row.get("facet_id") in affected):
            result.append((kind, key, {"id": key, "facet_id": row["facet_id"], "state": "erased"}))
        elif kind == "authority" and all_in_scope:
            result.append(
                (
                    kind,
                    key,
                    dict(
                        spec={"id": key, "revoked": True},
                        version=row["version"] + 1,
                        epoch=row["epoch"],
                        fingerprint=digest({"id": key, "revoked": True}),
                    ),
                )
            )
        elif kind == "query" and all_in_scope:
            result.append(
                (
                    kind,
                    key,
                    dict(
                        generation=row["generation"] + 1,
                        disabled=True,
                        epoch=row["epoch"],
                    ),
                )
            )
        elif kind == "grant" and (all_in_scope or "source:" + key in parents):
            result.append(
                (
                    kind,
                    key,
                    {
                        "source_id": key,
                        "version": row["version"] + 1,
                        "revoked": True,
                        **(
                            {"authority_id": row["authority_id"]} if row.get("authority_id") else {}
                        ),
                    },
                )
            )
        elif kind == "revision" and (all_in_scope or row.get("facet_id") in affected):
            result.append(
                (
                    kind,
                    key,
                    {
                        "id": key,
                        "facet_id": row["facet_id"],
                        "state": "erased",
                        "parents": [],
                        "reason": "deleted",
                    },
                )
            )
        elif kind == "head" and (all_in_scope or key in affected):
            result.append((kind, key, {"facet_id": key, "state": "erased", "revision_id": None}))
        elif kind == "definition" and (
            all_in_scope or key in affected or set(row["slots"]).intersection(slots)
        ):
            # Any deleted candidate in the query also invalidates zero-output views.
            row["dirty"] = True
            row["safety_generation"] += 1
            if all_in_scope:
                row["disabled"] = True
                row["spec"].pop("context", None)  # Erase retired host routing values too.
            result.append((kind, key, row))
        elif kind == "job" and all_in_scope:
            result.append(
                (
                    kind,
                    key,
                    {"id": key, "status": "cancelled", "unit": {}, "reason": "scope_erased"},
                )
            )
        elif kind == "request" and all_in_scope:
            result.append((kind, key, {"id": key, "invalidated": True}))
    return result, affected


def validate_edges(values, *, atom_ids, source_event_ids, slot_ids):
    """This release has no derived parents; every edge must name a snapshot input."""
    allowed = {
        "support": {"atom:" + key for key in atom_ids},
        "processing": {"atom:" + key for key in atom_ids}
        | {"source:" + key for key in source_event_ids},
        "query": {"facet:" + key for key in slot_ids},
    }
    if len(values) > 512:
        raise DerivedError("derived_dependency_capacity")
    for kind, parent in values:
        if kind not in allowed or parent not in allowed[kind]:
            raise DerivedError("derived_parent_unsupported")
