"""V7 opt-in question contracts; no storage, registration, or runtime capability.

These immutable records describe claims, not authority to use them. Constructing a
certificate does not verify source completeness, permission, or freshness. A later
transactional implementation must validate those claims before publication/delivery.
The strict v1 wire format is deliberately separate from legacy facet/page receipts.
"""

import json
import math
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..domain import MemoryScope
from ..serialization import to_jsonable
from .model import DerivedError, digest


class AnswerStatus(StrEnum):
    RESOLVED = "resolved"
    UNKNOWN = "unknown"
    CONTESTED = "contested"
    EMPTY = "empty"
    INCOMPLETE = "incomplete"


class AvailabilityStatus(StrEnum):
    VALID = "valid"
    STALE = "stale"
    INVALID = "invalid"
    ERASED = "erased"


class RefreshStatus(StrEnum):
    IDLE = "idle"
    PENDING = "pending"
    RUNNING = "running"
    RETRY = "retry"
    DEFERRED = "deferred"
    DEAD = "dead"


class TimeMode(StrEnum):
    CURRENT = "current"
    EXACT_HISTORICAL = "exact_historical"


class ComputeMode(StrEnum):
    FULL = "full"
    DELTA = "delta"
    PROOF_REUSE = "proof_reuse"


class SourceBasis(StrEnum):
    ADMITTED_L1 = "admitted_l1"
    PUBLICATION_MANIFEST = "publication_manifest"


def _fail(code="invalid_question_contract"):
    raise DerivedError(code)


def _name(value):
    if type(value) is not str or not value or value != value.strip() or len(value) > 256:
        _fail("invalid_question_identity")
    if unicodedata.normalize("NFC", value) != value:
        _fail("question_identity_not_normalized")
    return value


def _sha(value):
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        _fail("invalid_question_digest")
    return value


def _integer(value, minimum=0, maximum=2**63 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        _fail("invalid_question_integer")
    return value


def _names(value, maximum=128, *, empty=False):
    if type(value) is not tuple or not (0 if empty else 1) <= len(value) <= maximum:
        _fail("invalid_question_members")
    for item in value:
        _name(item)
    if len(set(value)) != len(value):
        _fail("duplicate_question_member")
    return value


def _time(value):
    if type(value) is not datetime or value.utcoffset() is None:
        _fail("question_timezone_required")
    return value.astimezone(UTC)


def _parse_time(value):
    if type(value) is not str:
        _fail("invalid_question_timestamp")
    try:
        return _time(datetime.fromisoformat(value))
    except ValueError:
        _fail("invalid_question_timestamp")


def _optional_time(value):
    return None if value is None else _parse_time(value)


def _enum(value, kind):
    if type(value) is not kind:
        _fail("invalid_question_enum")


def _parse_enum(kind):
    def parse(value):
        if type(value) is not str:
            _fail("invalid_question_enum")
        try:
            return kind(value)
        except ValueError:
            _fail("unsupported_question_mode")

    return parse


def _sequence(parse=lambda value: value):
    def decode(value):
        if type(value) is not list:
            _fail("invalid_question_array")
        return tuple(parse(item) for item in value)

    return decode


def _scope(value):
    if type(value) is not MemoryScope:
        _fail("invalid_question_scope")
    for item in fields(MemoryScope):
        field = getattr(value, item.name)
        if field is not None or item.name in {"tenant_id", "namespace"}:
            _name(field)
    return value


def _parse_scope(value):
    _keys(value, {field.name for field in fields(MemoryScope)})
    # Validate before MemoryScope's older, deliberately unchanged constructor.
    for key, item in value.items():
        if item is not None or key in {"tenant_id", "namespace"}:
            _name(item)
    return _scope(MemoryScope(**value))


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        _fail("invalid_question_fields")


def _freeze(value, depth=0):
    """Own every nested value; reject non-JSON/coercible keys and nonfinite numbers."""
    if depth > 16:
        _fail("question_value_capacity")
    if value is None or type(value) in {bool, int}:
        if type(value) is int:
            _integer(value, -(2**63), 2**63 - 1)
        return value
    if type(value) is float:
        if not math.isfinite(value):
            _fail("invalid_question_number")
        return 0.0 if value == 0 else value
    if type(value) is str:
        if len(value) > 65536:
            _fail("question_value_capacity")
        return unicodedata.normalize("NFC", value)
    if isinstance(value, Mapping):
        if len(value) > 512:
            _fail("question_value_capacity")
        result = {}
        for key, item in value.items():
            _name(key)
            result[key] = _freeze(item, depth + 1)
        return MappingProxyType(dict(sorted(result.items())))
    if type(value) in {list, tuple}:
        if len(value) > 4096:
            _fail("question_value_capacity")
        return tuple(_freeze(item, depth + 1) for item in value)
    _fail("invalid_question_json_type")


def _object(value):
    if not isinstance(value, Mapping):
        _fail("invalid_question_object")
    return _freeze(value)


def _set(obj, name, value):
    object.__setattr__(obj, name, value)


def _schema(value, depth=0):
    """A bounded JSON Schema subset. No refs, defaults, coercion, or open objects."""
    if not isinstance(value, Mapping) or depth > 8:
        _fail("unsupported_question_schema")
    kind = value.get("type")
    shapes = {
        "object": {"type", "properties", "required", "additionalProperties"},
        "array": {"type", "items", "maxItems", "uniqueItems"},
        "string": {"type", "maxLength"},
        "integer": {"type", "minimum", "maximum"},
        "number": {"type", "minimum", "maximum"},
        "boolean": {"type"},
        "null": {"type"},
    }
    if type(kind) is not str or kind not in shapes or set(value) - {"enum"} != shapes[kind]:
        _fail("unsupported_question_schema")
    if kind == "object":
        if (
            not isinstance(value["properties"], Mapping)
            or value["additionalProperties"] is not False
        ):
            _fail("unsupported_question_schema")
        _names(tuple(value["properties"]), empty=True)
        required = value["required"]
        if type(required) not in {list, tuple}:
            _fail("unsupported_question_schema")
        _names(tuple(required), empty=True)
        if not set(required) <= set(value["properties"]):
            _fail("unsupported_question_schema")
        for item in value["properties"].values():
            _schema(item, depth + 1)
    elif kind == "array":
        _integer(value["maxItems"], 0, 4096)
        if type(value["uniqueItems"]) is not bool:
            _fail("unsupported_question_schema")
        _schema(value["items"], depth + 1)
    elif kind == "string":
        _integer(value["maxLength"], 1, 65536)
    elif kind in {"integer", "number"}:
        for key in ("minimum", "maximum"):
            limit = value[key]
            if kind == "integer":
                _integer(limit, -(2**63), 2**63 - 1)
            elif type(limit) not in {int, float}:
                _fail("unsupported_question_schema")
            elif type(limit) is int:
                _integer(limit, -(2**63), 2**63 - 1)
            elif not math.isfinite(limit):
                _fail("unsupported_question_schema")
        if value["minimum"] > value["maximum"]:
            _fail("unsupported_question_schema")
    if "enum" in value:
        enum = value["enum"]
        if (
            kind in {"array", "object"}
            or type(enum) not in {list, tuple}
            or not 1 <= len(enum) <= 128
        ):
            _fail("unsupported_question_schema")
        base = dict(value)
        base.pop("enum")
        normalized = tuple(_validate(item, base) for item in enum)
        if len({digest(to_jsonable(item)) for item in normalized}) != len(enum):
            _fail("unsupported_question_schema")
    normalized = dict(value)
    if kind == "object":
        normalized["properties"] = {
            key: _schema(item, depth + 1) for key, item in value["properties"].items()
        }
        normalized["required"] = tuple(sorted(value["required"]))
    elif kind == "array":
        normalized["items"] = _schema(value["items"], depth + 1)
    if "enum" in value:
        normalized["enum"] = tuple(
            sorted(
                (_validate(item, base) for item in value["enum"]),
                key=lambda item: digest(to_jsonable(item)),
            )
        )
    return _freeze(normalized)


def _validate(value, schema):
    kind = schema["type"]
    if kind == "object":
        if not isinstance(value, Mapping) or not set(schema["required"]) <= set(value) <= set(
            schema["properties"]
        ):
            _fail("question_schema_mismatch")
        value = {key: _validate(item, schema["properties"][key]) for key, item in value.items()}
    elif kind == "array":
        if type(value) not in {list, tuple} or len(value) > schema["maxItems"]:
            _fail("question_schema_mismatch")
        value = tuple(_validate(item, schema["items"]) for item in value)
        if schema["uniqueItems"] and len({digest(to_jsonable(item)) for item in value}) != len(
            value
        ):
            _fail("question_schema_mismatch")
    elif kind == "string":
        if type(value) is not str or len(value) > 65536:
            _fail("question_schema_mismatch")
        value = unicodedata.normalize("NFC", value)
        if len(value) > schema["maxLength"]:
            _fail("question_schema_mismatch")
    elif kind in {"integer", "number"}:
        if type(value) not in ({int} if kind == "integer" else {int, float}):
            _fail("question_schema_mismatch")
        if type(value) is int:
            _integer(value, -(2**63), 2**63 - 1)
        if not math.isfinite(value) or not schema["minimum"] <= value <= schema["maximum"]:
            _fail("question_schema_mismatch")
        if kind == "number" and type(value) is float:
            # Preserve exact integer inputs. Casting them to float aliases values
            # above 2**53, which could merge distinct question instances.
            if value.is_integer() and -(2**63) <= value <= 2**63 - 1:
                value = int(value)
    elif kind == "boolean":
        if type(value) is not bool:
            _fail("question_schema_mismatch")
    elif kind == "null":
        if value is not None:
            _fail("question_schema_mismatch")
    if "enum" in schema:
        encoded = digest(to_jsonable(value))
        base = dict(schema)
        base.pop("enum")
        if encoded not in {digest(to_jsonable(_validate(item, base))) for item in schema["enum"]}:
            _fail("question_schema_mismatch")
    return _freeze(value)


def _wire_shape(value, depth=0):
    # Constructors accept owned immutable mappings/tuples; wire decoders accept
    # only actual JSON types, without implicit tuple/Enum/object coercion.
    if depth > 40:
        _fail("question_wire_capacity")
    if value is None or type(value) in {str, bool, int}:
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for item in value:
            _wire_shape(item, depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                _fail("invalid_question_json_type")
            _wire_shape(item, depth + 1)
        return
    _fail("invalid_question_json_type")


class _Contract:
    def payload(self):
        # The legacy serializer preserves StrEnum instances; this wire is plain JSON.
        return json.loads(json.dumps(to_jsonable(self), allow_nan=False))

    @classmethod
    def _parse(cls, payload, **parsers):
        _wire_shape(payload)
        _keys(payload, {field.name for field in fields(cls)})
        return cls(
            **{key: parsers.get(key, lambda value: value)(value) for key, value in payload.items()}
        )

    def _version(self, expected):
        if type(self.schema) is not str or self.schema != expected:
            _fail("unsupported_question_contract")


@dataclass(frozen=True)
class RefreshPolicyRef(_Contract):
    id: str
    revision: int
    schema: str = "question-refresh-policy-ref/1"

    def __post_init__(self):
        self._version("question-refresh-policy-ref/1")
        _name(self.id)
        _integer(self.revision, 1)

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(payload)


@dataclass(frozen=True)
class QuestionTime(_Contract):
    """Current instance is reusable; request time belongs to a finite target.

    Historical coordinates are always exact and never rounded or defaulted.
    Representability does not enable historical question reads.
    """

    mode: TimeMode
    valid_at: datetime | None
    known_at: datetime | None
    schema: str = "question-time/1"

    def __post_init__(self):
        self._version("question-time/1")
        _enum(self.mode, TimeMode)
        if self.mode == TimeMode.CURRENT:
            if self.valid_at is not None or self.known_at is not None:
                _fail("invalid_current_question_time")
        else:
            _set(self, "valid_at", _time(self.valid_at))
            _set(self, "known_at", _time(self.known_at))

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload, mode=_parse_enum(TimeMode), valid_at=_optional_time, known_at=_optional_time
        )


@dataclass(frozen=True)
class QuestionContext(_Contract):
    """Host-supplied material binding; this record itself grants no trust or ACL."""

    issuer_id: str
    revision: str
    attributes: Mapping
    expires_at: datetime
    schema: str = "question-context/1"

    def __post_init__(self):
        self._version("question-context/1")
        _name(self.issuer_id)
        _name(self.revision)
        _set(self, "attributes", _object(self.attributes))
        _set(self, "expires_at", _time(self.expires_at))

    @property
    def fingerprint(self):
        return digest(self.payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(payload, expires_at=_parse_time)


@dataclass(frozen=True)
class QuestionDefinition(_Contract):
    id: str
    version: str
    generation: int
    scope: MemoryScope
    parameter_schema: Mapping
    output_schema: Mapping
    scope_bindings: Mapping
    query_id: str
    query_fingerprint: str
    predicate_versions: Mapping
    qualification_policy_version: str
    business_policy_version: str
    unknown_semantics: str
    conflict_semantics: str
    empty_semantics: str
    required_fields: tuple[str, ...]
    allowed_parent_kinds: tuple[str, ...]
    source_basis: SourceBasis
    completeness: str
    time_mode: TimeMode
    timezone: str
    calendar_version: str
    renderer_id: str
    renderer_version: str
    allowed_modes: tuple[ComputeMode, ...]
    audiences: tuple[str, ...]
    purposes: tuple[str, ...]
    retention_policy_version: str
    max_output_bytes: int
    max_dependencies: int
    max_instances: int
    configuration_version: str
    refresh_policy: RefreshPolicyRef
    schema: str = "question-definition/1"

    def __post_init__(self):
        self._version("question-definition/1")
        for key in (
            "id",
            "version",
            "query_id",
            "qualification_policy_version",
            "business_policy_version",
            "unknown_semantics",
            "conflict_semantics",
            "empty_semantics",
            "timezone",
            "calendar_version",
            "renderer_id",
            "renderer_version",
            "retention_policy_version",
            "configuration_version",
        ):
            _name(getattr(self, key))
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            _fail("unsupported_question_timezone")
        _integer(self.generation, 1)
        _scope(self.scope)
        _sha(self.query_fingerprint)
        for key in ("parameter_schema", "output_schema"):
            value = _schema(getattr(self, key))
            if value["type"] != "object":
                _fail("unsupported_question_root_schema")
            _set(self, key, value)
        bindings = _object(self.scope_bindings)
        for parameter, coordinate in bindings.items():
            _name(coordinate)
            if parameter not in self.parameter_schema["required"] or coordinate not in {
                field.name for field in fields(MemoryScope)
            }:
                _fail("invalid_question_scope_binding")
            if (
                self.parameter_schema["properties"][parameter]["type"] != "string"
                or getattr(self.scope, coordinate) is None
            ):
                _fail("invalid_question_scope_binding")
        _set(self, "scope_bindings", bindings)
        predicates = _object(self.predicate_versions)
        if not 1 <= len(predicates) <= 128:
            _fail("invalid_question_predicates")
        for version in predicates.values():
            _name(version)
        _set(self, "predicate_versions", predicates)
        _set(self, "required_fields", tuple(sorted(_names(self.required_fields, empty=True))))
        if not set(self.required_fields) <= set(self.output_schema["properties"]):
            _fail("invalid_question_required_fields")
        _set(
            self,
            "allowed_parent_kinds",
            tuple(sorted(_names(self.allowed_parent_kinds, maximum=3))),
        )
        if set(self.allowed_parent_kinds) - {"l1", "observation", "question_view"}:
            _fail("unsupported_question_parent")
        _enum(self.source_basis, SourceBasis)
        _enum(self.time_mode, TimeMode)
        if type(self.completeness) is not str or self.completeness not in {
            "complete_candidates",
            "partial_allowed",
        }:
            _fail("unsupported_question_completeness")
        if type(self.allowed_modes) is not tuple or not self.allowed_modes:
            _fail("invalid_question_compute_modes")
        for mode in self.allowed_modes:
            _enum(mode, ComputeMode)
        if len(set(self.allowed_modes)) != len(self.allowed_modes):
            _fail("invalid_question_compute_modes")
        _set(self, "allowed_modes", tuple(sorted(self.allowed_modes)))
        if ComputeMode.FULL not in self.allowed_modes:
            _fail("question_full_oracle_required")
        for key in ("audiences", "purposes"):
            _set(self, key, tuple(sorted(_names(getattr(self, key), maximum=16))))
        for key in ("max_output_bytes", "max_dependencies", "max_instances"):
            _integer(getattr(self, key), 1)
        if type(self.refresh_policy) is not RefreshPolicyRef:
            _fail("invalid_question_refresh_policy")

    def semantic_payload(self):
        result = self.payload()
        result.pop("refresh_policy")
        return result

    @property
    def semantic_fingerprint(self):
        return digest(self.semantic_payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            scope=_parse_scope,
            required_fields=_sequence(),
            allowed_parent_kinds=_sequence(),
            source_basis=_parse_enum(SourceBasis),
            time_mode=_parse_enum(TimeMode),
            allowed_modes=_sequence(_parse_enum(ComputeMode)),
            audiences=_sequence(),
            purposes=_sequence(),
            refresh_policy=RefreshPolicyRef.from_payload,
        )


@dataclass(frozen=True)
class QuestionInstance(_Contract):
    """Embedded definition is explicit, not looked up or trusted by this contract."""

    definition: QuestionDefinition
    parameters: Mapping
    context: QuestionContext
    audience: tuple[str, ...]
    purpose: str
    time: QuestionTime
    schema: str = "question-instance/1"

    def __post_init__(self):
        self._version("question-instance/1")
        if (
            type(self.definition) is not QuestionDefinition
            or type(self.context) is not QuestionContext
            or type(self.time) is not QuestionTime
        ):
            _fail("invalid_question_instance_binding")
        _name(self.purpose)
        _set(self, "audience", tuple(sorted(_names(self.audience, maximum=16))))
        if (
            not set(self.audience) <= set(self.definition.audiences)
            or self.purpose not in self.definition.purposes
        ):
            _fail("question_partition_mismatch")
        if self.time.mode != self.definition.time_mode:
            _fail("question_time_mode_mismatch")
        normalized = _validate(self.parameters, self.definition.parameter_schema)
        for parameter, coordinate in self.definition.scope_bindings.items():
            if normalized[parameter] != getattr(self.definition.scope, coordinate):
                _fail("question_scope_binding_mismatch")
        _set(self, "parameters", normalized)
        if (
            self.time.mode == TimeMode.EXACT_HISTORICAL
            and self.time.known_at >= self.context.expires_at
        ):
            _fail("question_context_expired")

    def identity_payload(self):
        return dict(
            schema=self.schema,
            scope=to_jsonable(self.definition.scope),
            definition=self.definition.semantic_fingerprint,
            parameters=to_jsonable(self.parameters),
            context=self.context.payload(),
            audience=list(self.audience),
            purpose=self.purpose,
            time=self.time.payload(),
        )

    @property
    def id(self):
        return "question-instance:" + digest(self.identity_payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            definition=QuestionDefinition.from_payload,
            context=QuestionContext.from_payload,
            audience=_sequence(),
            time=QuestionTime.from_payload,
        )


@dataclass(frozen=True)
class InputReference(_Contract):
    kind: str
    id: str
    revision: str
    sha256: str
    schema: str = "question-input-ref/1"

    def __post_init__(self):
        self._version("question-input-ref/1")
        if type(self.kind) is not str or self.kind not in {
            "source",
            "atom",
            "derived_content",
            "derived_certificate",
            "query",
        }:
            _fail("unsupported_question_input_kind")
        _name(self.id)
        _name(self.revision)
        _sha(self.sha256)

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(payload)


@dataclass(frozen=True)
class InputManifest(_Contract):
    """Full input census, including unquoted and transitive processing inputs."""

    inputs: tuple[InputReference, ...]
    snapshot_token: str
    algorithm_version: str
    schema: str = "question-input-manifest/1"

    def __post_init__(self):
        self._version("question-input-manifest/1")
        _name(self.snapshot_token)
        _name(self.algorithm_version)
        if (
            type(self.inputs) is not tuple
            or len(self.inputs) > 4096
            or any(type(item) is not InputReference for item in self.inputs)
        ):
            _fail("invalid_question_manifest")
        identities = [(item.kind, item.id, item.revision) for item in self.inputs]
        if len(set(identities)) != len(identities):
            _fail("duplicate_question_input")
        # Ordering is retained: model input order can affect the generated content.

    @property
    def fingerprint(self):
        return digest(self.payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(payload, inputs=_sequence(InputReference.from_payload))


@dataclass(frozen=True)
class CoverageFrontier(_Contract):
    """Continuous sequence vectors and exact completed-unit sets are distinct.

    A vector is a claim of gap-free processing, never a maximum observed offset.
    These shapes do not supply the proof needed to substantiate that claim.
    """

    mode: str
    positions: Mapping
    units: tuple[str, ...]
    schema: str = "question-frontier/1"

    def __post_init__(self):
        self._version("question-frontier/1")
        if type(self.mode) is not str or self.mode not in {"continuous", "exact_units"}:
            _fail("unsupported_question_frontier")
        positions = _object(self.positions)
        _names(self.units, maximum=4096, empty=True)
        for position in positions.values():
            _integer(position)
        if self.mode == "continuous" and (not positions or self.units):
            _fail("invalid_question_frontier")
        if self.mode == "exact_units" and (positions or not self.units):
            _fail("invalid_question_frontier")
        _set(self, "positions", positions)
        _set(self, "units", tuple(sorted(self.units)))

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(payload, units=_sequence())


@dataclass(frozen=True)
class QueryCoverage(_Contract):
    query_id: str
    query_fingerprint: str
    query_generation: int
    subscription_version: str
    qualification_policy_version: str
    source_basis: SourceBasis
    frontier: CoverageFrontier
    candidates_complete: bool
    truncation_reason: str | None
    publication_manifest_digest: str | None
    publication_closed: bool | None
    schema: str = "question-query-coverage/1"

    def __post_init__(self):
        self._version("question-query-coverage/1")
        for value in (self.query_id, self.subscription_version, self.qualification_policy_version):
            _name(value)
        _sha(self.query_fingerprint)
        _integer(self.query_generation)
        _enum(self.source_basis, SourceBasis)
        if (
            type(self.frontier) is not CoverageFrontier
            or type(self.candidates_complete) is not bool
        ):
            _fail("invalid_question_coverage")
        if self.truncation_reason is not None:
            _name(self.truncation_reason)
        if self.candidates_complete == (self.truncation_reason is not None):
            _fail("invalid_question_coverage_completeness")
        if self.source_basis == SourceBasis.ADMITTED_L1:
            if self.publication_manifest_digest is not None or self.publication_closed is not None:
                _fail("invalid_question_publication_coverage")
        else:
            _sha(self.publication_manifest_digest)
            if type(self.publication_closed) is not bool or (
                self.candidates_complete and not self.publication_closed
            ):
                _fail("question_publication_not_closed")

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload, source_basis=_parse_enum(SourceBasis), frontier=CoverageFrontier.from_payload
        )


@dataclass(frozen=True)
class TimeCoverage(_Contract):
    """Half-open finite intervals; a null end is an exact point, never infinity."""

    valid_from: datetime
    valid_until: datetime | None
    known_from: datetime
    known_until: datetime | None
    schema: str = "question-time-coverage/1"

    def __post_init__(self):
        self._version("question-time-coverage/1")
        for start, end in (("valid_from", "valid_until"), ("known_from", "known_until")):
            _set(self, start, _time(getattr(self, start)))
            if getattr(self, end) is not None:
                _set(self, end, _time(getattr(self, end)))
                if getattr(self, end) <= getattr(self, start):
                    _fail("invalid_question_time_coverage")

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            valid_from=_parse_time,
            valid_until=_optional_time,
            known_from=_parse_time,
            known_until=_optional_time,
        )


@dataclass(frozen=True)
class QuestionContent(_Contract):
    instance: QuestionInstance
    answer_status: AnswerStatus
    value: Mapping
    structure: Mapping
    renderer_version: str
    model_version: str | None
    generation_manifest: InputManifest
    schema: str = "question-content/1"

    def __post_init__(self):
        self._version("question-content/1")
        if (
            type(self.instance) is not QuestionInstance
            or type(self.generation_manifest) is not InputManifest
        ):
            _fail("invalid_question_content_binding")
        _enum(self.answer_status, AnswerStatus)
        _name(self.renderer_version)
        if self.renderer_version != self.instance.definition.renderer_version:
            _fail("question_renderer_mismatch")
        if self.model_version is not None:
            _name(self.model_version)
        _set(self, "value", _validate(self.value, self.instance.definition.output_schema))
        _set(self, "structure", _object(self.structure))
        if self.answer_status == AnswerStatus.RESOLVED and not set(
            self.instance.definition.required_fields
        ) <= set(self.value):
            _fail("question_required_fields_missing")
        if len(self.generation_manifest.inputs) > self.instance.definition.max_dependencies:
            _fail("question_dependency_capacity")
        # Count the actual materialized value and structure, not only visible text.
        if (
            len(
                json.dumps(
                    to_jsonable({"value": self.value, "structure": self.structure}),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            )
            > self.instance.definition.max_output_bytes
        ):
            _fail("question_output_capacity")

    @property
    def value_digest(self):
        return digest(dict(status=self.answer_status.value, value=to_jsonable(self.value)))

    @property
    def structure_digest(self):
        return digest(to_jsonable(self.structure))

    @property
    def generation_manifest_digest(self):
        return self.generation_manifest.fingerprint

    @property
    def id(self):
        return "question-content:" + digest(
            dict(
                schema=self.schema,
                instance=self.instance.id,
                value_digest=self.value_digest,
                structure_digest=self.structure_digest,
                renderer_version=self.renderer_version,
                model_version=self.model_version,
                generation_manifest_digest=self.generation_manifest_digest,
            )
        )

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            instance=QuestionInstance.from_payload,
            answer_status=_parse_enum(AnswerStatus),
            generation_manifest=InputManifest.from_payload,
        )


@dataclass(frozen=True)
class QuestionCertificate(_Contract):
    content_revision_id: str
    instance_id: str
    scope: MemoryScope
    definition_fingerprint: str
    context_fingerprint: str
    validation_manifest: InputManifest
    input_frontier: CoverageFrontier
    query_coverage: QueryCoverage
    support: tuple[InputReference, ...]
    time_coverage: TimeCoverage
    safety_fingerprint: str
    safety_epoch: int
    validation_algorithm_version: str
    refresh_policy: RefreshPolicyRef
    validated_at: datetime
    schema: str = "question-certificate/1"

    def __post_init__(self):
        self._version("question-certificate/1")
        for value, prefix in (
            (self.content_revision_id, "question-content:"),
            (self.instance_id, "question-instance:"),
        ):
            _name(value)
            if not value.startswith(prefix):
                _fail("invalid_question_revision_id")
            _sha(value[len(prefix) :])
        _scope(self.scope)
        for value in (
            self.definition_fingerprint,
            self.context_fingerprint,
            self.safety_fingerprint,
        ):
            _sha(value)
        for key, kind in (
            ("validation_manifest", InputManifest),
            ("input_frontier", CoverageFrontier),
            ("query_coverage", QueryCoverage),
            ("time_coverage", TimeCoverage),
            ("refresh_policy", RefreshPolicyRef),
        ):
            if type(getattr(self, key)) is not kind:
                _fail("invalid_question_certificate_binding")
        if (
            type(self.support) is not tuple
            or len(self.support) > 4096
            or any(type(item) is not InputReference for item in self.support)
        ):
            _fail("invalid_question_support")
        keys = [digest(item.payload()) for item in self.support]
        if len(set(keys)) != len(keys) or not set(keys) <= {
            digest(item.payload()) for item in self.validation_manifest.inputs
        }:
            _fail("question_support_not_validated")
        _integer(self.safety_epoch)
        _name(self.validation_algorithm_version)
        _set(self, "validated_at", _time(self.validated_at))

    def validate_content_binding(self, content):
        """Check structural bindings only; never authorize publication or delivery."""
        if type(content) is not QuestionContent:
            _fail("invalid_question_content_binding")
        instance = content.instance
        definition = instance.definition
        if (
            self.content_revision_id != content.id
            or self.instance_id != instance.id
            or self.scope != definition.scope
            or self.definition_fingerprint != definition.semantic_fingerprint
            or self.context_fingerprint != instance.context.fingerprint
        ):
            _fail("question_certificate_content_mismatch")
        query = self.query_coverage
        if (
            query.query_id != definition.query_id
            or query.query_fingerprint != definition.query_fingerprint
            or query.qualification_policy_version != definition.qualification_policy_version
            or query.source_basis != definition.source_basis
        ):
            _fail("question_certificate_query_mismatch")
        if len(self.validation_manifest.inputs) > definition.max_dependencies:
            _fail("question_dependency_capacity")

    @property
    def support_digest(self):
        return digest(sorted(digest(item.payload()) for item in self.support))

    @property
    def validation_digest(self):
        return digest(
            dict(
                manifest=self.validation_manifest.payload(),
                query=self.query_coverage.payload(),
                input_frontier=self.input_frontier.payload(),
            )
        )

    @property
    def id(self):
        return "question-certificate:" + digest(self.payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            scope=_parse_scope,
            validation_manifest=InputManifest.from_payload,
            input_frontier=CoverageFrontier.from_payload,
            query_coverage=QueryCoverage.from_payload,
            support=_sequence(InputReference.from_payload),
            time_coverage=TimeCoverage.from_payload,
            refresh_policy=RefreshPolicyRef.from_payload,
            validated_at=_parse_time,
        )


@dataclass(frozen=True)
class CoverageTarget(_Contract):
    """Finite lower bound; compatible successor coverage may satisfy it.

    This is not an exact snapshot, and completion is not current readiness.
    """

    instance_id: str
    scope: MemoryScope
    definition_fingerprint: str
    context_fingerprint: str
    time: QuestionTime
    requested_at: datetime
    request_semantics_digest: str
    required_frontier: CoverageFrontier
    schema: str = "coverage_target/1"

    def __post_init__(self):
        self._version("coverage_target/1")
        _target(self)
        if self.time.mode != TimeMode.CURRENT:
            _fail("historical_requires_exact_target")
        if type(self.required_frontier) is not CoverageFrontier:
            _fail("invalid_question_target_frontier")

    @property
    def id(self):
        return "question-coverage-target:" + digest(self.payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            scope=_parse_scope,
            time=QuestionTime.from_payload,
            requested_at=_parse_time,
            required_frontier=CoverageFrontier.from_payload,
        )


@dataclass(frozen=True)
class ExactSnapshotTarget(_Contract):
    """Exact fixed unit identity; newer snapshots never substitute for this unit.

    A distinct V7 shape, without modifying any legacy FacetRefreshUnit contract.
    """

    instance_id: str
    scope: MemoryScope
    definition_fingerprint: str
    context_fingerprint: str
    time: QuestionTime
    requested_at: datetime
    request_semantics_digest: str
    snapshot_token: str
    unit_id: str
    frontier: CoverageFrontier
    schema: str = "exact_snapshot_target/1"

    def __post_init__(self):
        self._version("exact_snapshot_target/1")
        _target(self)
        _name(self.snapshot_token)
        _name(self.unit_id)
        if type(self.frontier) is not CoverageFrontier:
            _fail("invalid_question_target_frontier")

    @property
    def id(self):
        return "question-exact-target:" + digest(self.payload())

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(
            payload,
            scope=_parse_scope,
            time=QuestionTime.from_payload,
            requested_at=_parse_time,
            frontier=CoverageFrontier.from_payload,
        )


def _target(target):
    _name(target.instance_id)
    prefix = "question-instance:"
    if not target.instance_id.startswith(prefix):
        _fail("invalid_question_instance_id")
    _sha(target.instance_id[len(prefix) :])
    _scope(target.scope)
    for value in (
        target.definition_fingerprint,
        target.context_fingerprint,
        target.request_semantics_digest,
    ):
        _sha(value)
    if type(target.time) is not QuestionTime:
        _fail("invalid_question_target_time")
    _set(target, "requested_at", _time(target.requested_at))


@dataclass(frozen=True)
class QuestionHead(_Contract):
    """CAS coordinates only; writing a head requires the future atomic UoW gate."""

    instance_id: str
    scope: MemoryScope
    content_revision_id: str
    certificate_revision_id: str
    definition_fingerprint: str
    definition_generation: int
    epoch: int
    schema: str = "question-head/1"

    def __post_init__(self):
        self._version("question-head/1")
        _scope(self.scope)
        for value, prefix in (
            (self.instance_id, "question-instance:"),
            (self.content_revision_id, "question-content:"),
            (self.certificate_revision_id, "question-certificate:"),
        ):
            _name(value)
            if not value.startswith(prefix):
                _fail("invalid_question_head_identity")
            _sha(value[len(prefix) :])
        _sha(self.definition_fingerprint)
        _integer(self.definition_generation, 1)
        _integer(self.epoch)

    @classmethod
    def bind(cls, content, certificate, *, epoch):
        if type(certificate) is not QuestionCertificate:
            _fail("invalid_question_certificate_binding")
        certificate.validate_content_binding(content)
        definition = content.instance.definition
        return cls(
            content.instance.id,
            definition.scope,
            content.id,
            certificate.id,
            definition.semantic_fingerprint,
            definition.generation,
            epoch,
        )

    @classmethod
    def from_payload(cls, payload):
        return cls._parse(payload, scope=_parse_scope)


# Proposed protocol dependencies, not runtime-advertised capabilities. The listed
# prerequisite names describe acceptance gates; they do not claim implementations.
QUESTION_CAPABILITY_DEPENDENCIES = MappingProxyType(
    {
        "question-view/1": (
            "registered-question-definition/1",
            "query-subscriptions/1",
            "qualified-question-input/1",
            "current-delivery-guard/1",
        ),
        "query-subscriptions/1": ("transactional-query-barrier/1", "complete-query-coverage/1"),
        "refresh-policy/1": ("persistent-refresh-demand/1", "shared-refresh-budget/1"),
        "coverage_target/1": ("fixed-refresh-frontier/1", "verified-continuous-coverage/1"),
        "deterministic-delta/1": (
            "question-view/1",
            "continuous-change-log/1",
            "validated-delta-operator/1",
        ),
        "view-proof-reuse/1": (
            "question-view/1",
            "immutable-generation-manifest/1",
            "atomic-view-proof/1",
        ),
        "exact-answer-cache/1": (
            "view-proof-reuse/1",
            "model-input-guard/1",
            "model-dispatch-budget/1",
            "current-delivery-guard/1",
        ),
    }
)
ENABLED_QUESTION_CAPABILITIES = frozenset()


def require_question_capability(capability):
    """Fail closed until a separately accepted runtime explicitly implements it."""
    _name(capability)
    if capability not in QUESTION_CAPABILITY_DEPENDENCIES:
        _fail("unsupported_question_capability")
    _fail("question_capability_disabled")
