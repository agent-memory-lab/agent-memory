"""Frozen A9 host-resource observations and explicit, exact tariff projection.

This is an evaluation-only trust boundary, not a telemetry implementation. A
read-only host observer brackets each phase and supplies evidence-backed deltas.
CPU/wall observations come from ModelExperimentAccounting; absent GPU, I/O,
storage and network measurements are unknown, including on empty/cache phases.

Schema v1 uses milliseconds and integral byte/byte-second counters. Monetary
rates are decimal strings in *microunits per unit*. Products and their sum use
exact fractions, then round upward once per operation to an integer microunit.
Wall time is reporting-only unless an explicit, documented non-overlapping
wall tariff is frozen. This contract cannot verify the truth of host evidence.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from fractions import Fraction
from typing import Protocol

from .acceptance import _hash
from .evidence import _digest, _text
from .question_cost import CostPhase, CostSnapshot

RESOURCE_CONTRACT_VERSION = "question-resources/1"
ROUNDING_POLICY = "ceil_total_microunit"
RESOURCE_CHARGE_SCOPE = "host_local_resources_excluding_provider_bills/1"


class ResourceKind(StrEnum):
    CPU = "cpu"
    WALL = "wall"
    GPU = "gpu"
    IO = "io"
    STORAGE = "storage"
    NETWORK = "network"


@dataclass(frozen=True, slots=True)
class ResourceSpecification:
    resource: ResourceKind
    unit: str
    integral: bool


RESOURCE_SCHEMA = (
    ResourceSpecification(ResourceKind.CPU, "ms", False),
    ResourceSpecification(ResourceKind.WALL, "ms", False),
    ResourceSpecification(ResourceKind.GPU, "ms", False),
    ResourceSpecification(ResourceKind.IO, "bytes", True),
    ResourceSpecification(ResourceKind.STORAGE, "byte_seconds", True),
    ResourceSpecification(ResourceKind.NETWORK, "bytes", True),
)
RESOURCE_SCHEMA_SHA256 = _digest((RESOURCE_CONTRACT_VERSION, RESOURCE_SCHEMA))


def _currency(value: str) -> None:
    if type(value) is not str or len(value) != 3 or not all("A" <= c <= "Z" for c in value):
        raise ValueError("currency must be a three-letter uppercase code")


def _decimal(value: str | int | Decimal, name: str) -> str:
    """Canonical, bounded, exact decimal; never accept a binary float tariff."""
    if type(value) not in (str, int, Decimal) or len(str(value)) > 256:
        raise ValueError(f"{name} requires an exact decimal string, Decimal or integer")
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid {name}") from error
    if not number.is_finite() or number < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    if len(number.as_tuple().digits) > 128 or abs(number.as_tuple().exponent) > 128:
        raise ValueError(f"{name} exceeds supported decimal precision")
    if number == 0:
        return "0"
    result = format(number, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def _unit(resource: ResourceKind, unit: str) -> ResourceSpecification:
    spec = next(spec for spec in RESOURCE_SCHEMA if spec.resource == resource)
    if unit != spec.unit:
        raise ValueError(f"unsupported unit for {resource}: expected {spec.unit}")
    return spec


@dataclass(frozen=True, slots=True)
class ResourceMeasurement:
    resource: ResourceKind
    unit: str
    quantity: str
    evidence: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "resource", ResourceKind(self.resource))
        spec = _unit(self.resource, self.unit)
        quantity = _decimal(self.quantity, "resource quantity")
        number = Fraction(quantity)
        if spec.integral and (number.denominator != 1 or number > 2**63 - 1):
            raise ValueError("byte and byte-second counters must be signed-64-bit integers")
        object.__setattr__(self, "quantity", quantity)
        _text(self.evidence, "measurement evidence", 2048)


@dataclass(frozen=True, slots=True)
class ResourceRate:
    resource: ResourceKind
    unit: str
    microunits_per_unit: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "resource", ResourceKind(self.resource))
        _unit(self.resource, self.unit)
        object.__setattr__(
            self, "microunits_per_unit", _decimal(self.microunits_per_unit, "tariff rate")
        )


def _typed_unique(values: Sequence, expected: type, name: str) -> tuple:
    values = tuple(values)
    if any(type(value) is not expected for value in values):
        raise ValueError(f"{name} requires typed records")
    if len({value.resource for value in values}) != len(values):
        raise ValueError(f"duplicate {name} resource")
    return tuple(sorted(values, key=lambda value: value.resource))


@dataclass(frozen=True, slots=True)
class ResourceTariff:
    currency: str
    reference: str
    rates: tuple[ResourceRate, ...]
    wall_time_charge_basis: str | None = None
    rounding_policy: str = ROUNDING_POLICY

    def __post_init__(self) -> None:
        _currency(self.currency)
        _text(self.reference, "tariff reference", 2048)
        object.__setattr__(self, "rates", _typed_unique(self.rates, ResourceRate, "tariff"))
        if self.rounding_policy != ROUNDING_POLICY:
            raise ValueError("unsupported tariff rounding policy")
        wall = any(rate.resource == ResourceKind.WALL for rate in self.rates)
        if wall != (self.wall_time_charge_basis is not None):
            raise ValueError("wall tariff requires an explicit non-overlapping charge basis")
        if wall:
            _text(self.wall_time_charge_basis, "wall tariff charge basis", 2048)

    @property
    def fingerprint(self) -> str:
        return _digest(self)


@dataclass(frozen=True, slots=True)
class ResourcePricingProtocol:
    currency: str = "USD"
    observer_configuration_sha256: str | None = None
    tariff: ResourceTariff | None = None
    schema_version: str = RESOURCE_CONTRACT_VERSION
    schema_sha256: str = RESOURCE_SCHEMA_SHA256
    charge_scope: str = RESOURCE_CHARGE_SCOPE

    def __post_init__(self) -> None:
        _currency(self.currency)
        if self.charge_scope != RESOURCE_CHARGE_SCOPE:
            raise ValueError("unsupported resource charge scope")
        if self.schema_version != RESOURCE_CONTRACT_VERSION or (
            self.schema_sha256 != RESOURCE_SCHEMA_SHA256
        ):
            raise ValueError("unsupported resource schema")
        if self.observer_configuration_sha256 is not None:
            _hash(self.observer_configuration_sha256, "observer_configuration_sha256")
        if self.tariff is not None:
            if type(self.tariff) is not ResourceTariff:
                raise ValueError("typed resource tariff required")
            if self.tariff.currency != self.currency:
                raise ValueError("tariff currency differs from resource protocol")

    @property
    def fingerprint(self) -> str:
        return _digest(self)


def _frozen(protocol: ResourcePricingProtocol, expected_protocol_sha256: str) -> None:
    if type(protocol) is not ResourcePricingProtocol:
        raise ValueError("typed resource protocol required")
    if protocol.fingerprint != expected_protocol_sha256:
        raise ValueError("resource protocol changed after freeze")


@dataclass(frozen=True, slots=True)
class PhaseResourceRequest:
    """Immutable identity/evidence supplied to end(), never model-controlled."""

    operation_id: str
    phase: CostPhase
    currency: str
    phase_observation_sha256: str
    protocol_sha256: str
    observer_configuration_sha256: str | None
    cpu_ms: str
    wall_ms: str
    evidence: str
    failed: bool

    def __post_init__(self) -> None:
        _text(self.operation_id, "operation_id")
        object.__setattr__(self, "phase", CostPhase(self.phase))
        _currency(self.currency)
        for name in ("phase_observation_sha256", "protocol_sha256"):
            _hash(getattr(self, name), name)
        if self.observer_configuration_sha256 is not None:
            _hash(self.observer_configuration_sha256, "observer_configuration_sha256")
        for name in ("cpu_ms", "wall_ms"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        _text(self.evidence, "phase evidence", 2048)
        if type(self.failed) is not bool:
            raise ValueError("phase failed flag must be boolean")


def resource_observation_request(
    observation: Mapping,
    protocol: ResourcePricingProtocol,
    *,
    expected_protocol_sha256: str,
) -> PhaseResourceRequest:
    """Bind end() to the unchanged accounting observation recorded by measure()."""
    _frozen(protocol, expected_protocol_sha256)
    if not isinstance(observation, Mapping) or observation.get("pricing") != "unknown":
        raise ValueError("expected an unpriced accounting observation")
    for name in ("cpu_ms", "wall_ms"):
        if type(observation.get(name)) not in (int, float):
            raise ValueError(f"missing measured {name}")
    return PhaseResourceRequest(
        observation["operation_id"],
        observation["phase"],
        protocol.currency,
        _digest(observation),
        protocol.fingerprint,
        protocol.observer_configuration_sha256,
        _decimal(str(observation["cpu_ms"]), "measured CPU"),
        _decimal(str(observation["wall_ms"]), "measured wall time"),
        observation["evidence"],
        observation["failed"],
    )


@dataclass(frozen=True, slots=True)
class PhaseResourceObservation:
    request: PhaseResourceRequest
    measurements: tuple[ResourceMeasurement, ...] = ()
    finalized: bool = True
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        if type(self.request) is not PhaseResourceRequest:
            raise ValueError("typed observation binding required")
        object.__setattr__(
            self,
            "measurements",
            _typed_unique(self.measurements, ResourceMeasurement, "measurement"),
        )
        if type(self.finalized) is not bool:
            raise ValueError("finalized must be boolean")
        if self.failure_reason is not None:
            _text(self.failure_reason, "observer failure reason", 256)
            if self.finalized:
                raise ValueError("a failed observation cannot be finalized")


class HostResourceObserver(Protocol):
    """Host-owned, read-only telemetry adapter with no execution/provider access.

    begin/end must bracket the measured phase, including failed executions.
    end receives the operation identity allocated by accounting's finally block.
    Quantities must be local deltas for that operation, not overlapping host totals.
    Remote-provider GPU/resources already covered by model bills are excluded.
    A measured zero needs explicit evidence. finalize validates collector closure;
    if it raises, callers must not pass observer_finalized=True to reconciliation.
    Never include raw exception bodies, credentials or user data in failure_reason.
    """

    configuration_sha256: str

    def begin(self, phase: CostPhase, request_ids: tuple[str, ...]) -> object: ...

    def end(self, token: object, observation: PhaseResourceRequest) -> PhaseResourceObservation: ...

    def finalize(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PhaseResourcePricing:
    operation_id: str
    phase: CostPhase
    phase_observation_sha256: str | None
    protocol_sha256: str
    measurements: tuple[ResourceMeasurement, ...]
    missing_resources: tuple[ResourceKind, ...]
    missing_tariffs: tuple[ResourceKind, ...]
    reasons: tuple[str, ...]
    pricing: str
    priced_resource_microunits: int | None
    tariff_reference: str | None


@dataclass(frozen=True, slots=True)
class ResourcePricingResult:
    snapshot: CostSnapshot
    protocol: ResourcePricingProtocol
    phases: tuple[PhaseResourcePricing, ...]
    observer_finalized: bool


def reconcile_resources(
    observations: Sequence[Mapping],
    snapshot: CostSnapshot,
    protocol: ResourcePricingProtocol,
    *,
    expected_protocol_sha256: str,
    records: Sequence[PhaseResourceObservation] = (),
    observer_finalized: bool = False,
    observer_finalization_error: str | None = None,
) -> ResourcePricingResult:
    """Return a new snapshot and complete evidence/missing-data sidecars.

    Freeze the protocol in the plan before executing anything. Validate the host
    observer's configuration hash before begin(). Collect a reply after *every*
    accounting measure(), including snapshot and cleanup phases. On begin/end
    exceptions retain an unfinalized reply (or no reply); call finalize() after
    all phases and set observer_finalized only on success. Missing data/rates or
    failed finalization retain unknown costs. Conflicting bindings fail closed.

    No input is mutated. Model/provider bills and pending responsibilities are
    preserved. Unknown legacy CostEntry counter defaults are not observations;
    only the returned sidecar declares measured or missing resource dimensions.
    """
    _frozen(protocol, expected_protocol_sha256)
    if type(snapshot) is not CostSnapshot or snapshot.currency != protocol.currency:
        raise ValueError("resource snapshot currency differs from frozen protocol")
    if type(observer_finalized) is not bool:
        raise ValueError("observer_finalized must be boolean")
    if observer_finalization_error is not None:
        _text(observer_finalization_error, "observer finalization error", 256)
        if observer_finalized:
            raise ValueError("failed observer finalization cannot be successful")
    entries = {entry.operation_id: entry for entry in snapshot.entries}
    requests = {}
    for observation in observations:
        request = resource_observation_request(
            observation, protocol, expected_protocol_sha256=expected_protocol_sha256
        )
        if request.operation_id in requests:
            raise ValueError("duplicate phase operation identity")
        entry = entries.get(request.operation_id)
        if entry is None or entry.model_call or entry.actual_microunits is not None:
            raise ValueError("phase observation must bind an unpriced resource operation")
        if entry.phase != request.phase or Fraction(str(entry.cpu_ms)) != Fraction(request.cpu_ms):
            raise ValueError("phase observation differs from accounting snapshot")
        requests[request.operation_id] = request
    bound_records = {}
    for record in records:
        if type(record) is not PhaseResourceObservation:
            raise ValueError("typed resource observations required")
        identity = record.request.operation_id
        if identity in bound_records:
            raise ValueError("duplicate resource observation operation")
        if record.request != requests.get(identity):
            raise ValueError("resource observation binding differs from measured phase")
        if protocol.observer_configuration_sha256 is None:
            raise ValueError("resource observation requires a frozen observer configuration")
        bound_records[identity] = record

    tariff = protocol.tariff
    rates = {rate.resource: rate for rate in tariff.rates} if tariff else {}
    required_rates = {kind for kind in ResourceKind if kind != ResourceKind.WALL}
    if ResourceKind.WALL in rates:
        required_rates.add(ResourceKind.WALL)
    missing_tariffs = tuple(sorted(required_rates - rates.keys()))
    reports, replaced = [], {}
    for entry in snapshot.entries:
        if entry.model_call or entry.provider_billed_microunits is not None:
            continue
        if entry.priced_resource_microunits is not None:
            raise ValueError("cannot reprice an already priced resource operation")
        request = requests.get(entry.operation_id)
        record = bound_records.get(entry.operation_id)
        measurements, reasons = {}, []
        if request is None:
            reasons.append("missing_phase_observation")
        else:
            for kind, amount in (
                (ResourceKind.CPU, request.cpu_ms),
                (ResourceKind.WALL, request.wall_ms),
            ):
                measurements[kind] = ResourceMeasurement(kind, "ms", amount, request.evidence)
        if record is None:
            reasons.append("missing_resource_observation")
        elif not record.finalized:
            reasons.append(record.failure_reason or "resource_observation_not_finalized")
        else:
            for measurement in record.measurements:
                old = measurements.get(measurement.resource)
                if old is not None and old.quantity != measurement.quantity:
                    raise ValueError("observer changed measured CPU or wall time")
                if old is None:
                    measurements[measurement.resource] = measurement
        if not observer_finalized:
            reasons.append(observer_finalization_error or "observer_not_finalized")
        missing_resources = tuple(sorted(set(ResourceKind) - measurements.keys()))
        if missing_resources:
            reasons.append("missing_resource_measurements")
        if missing_tariffs:
            reasons.append("missing_resource_tariffs")
        price = reference = None
        if not reasons:
            total = sum(
                (
                    Fraction(measurements[kind].quantity)
                    * Fraction(rates[kind].microunits_per_unit)
                    for kind in required_rates
                ),
                Fraction(0),
            )
            price = -(-total.numerator // total.denominator)
            if price > 2**63 - 1:
                raise ValueError("priced resource cost exceeds signed-64-bit microunits")
            reference = f"{tariff.reference}#sha256={tariff.fingerprint}"
            # CostEntry bounds references to 2048 characters, including hash suffix.
            if len(reference) > 2048:
                reference = f"resource-tariff-sha256:{tariff.fingerprint}"
            replaced[entry.operation_id] = replace(
                entry,
                priced_resource_microunits=price,
                tariff_reference=reference,
                io_bytes=int(measurements[ResourceKind.IO].quantity),
                storage_byte_seconds=int(measurements[ResourceKind.STORAGE].quantity),
                network_bytes=int(measurements[ResourceKind.NETWORK].quantity),
            )
        reports.append(
            PhaseResourcePricing(
                entry.operation_id,
                entry.phase,
                request.phase_observation_sha256 if request else None,
                protocol.fingerprint,
                tuple(measurements[kind] for kind in sorted(measurements)),
                missing_resources,
                missing_tariffs,
                tuple(sorted(set(reasons))),
                "priced" if price is not None else "unknown",
                price,
                reference,
            )
        )
    return ResourcePricingResult(
        replace(
            snapshot,
            entries=tuple(replaced.get(entry.operation_id, entry) for entry in snapshot.entries),
        ),
        protocol,
        tuple(reports),
        observer_finalized,
    )
