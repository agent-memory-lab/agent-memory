"""Synthetic receipts validate resource contracts; they are not telemetry evidence."""

import json
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, localcontext

import pytest

from agent_memory.evaluation.question_cost import (
    CostEntry,
    CostPhase,
    CostSnapshot,
    PendingResponsibility,
)
from agent_memory.evaluation.question_resources import (
    RESOURCE_SCHEMA,
    RESOURCE_SCHEMA_SHA256,
    PhaseResourceObservation,
    ResourceKind,
    ResourceMeasurement,
    ResourcePricingProtocol,
    ResourceRate,
    ResourceTariff,
    reconcile_resources,
    resource_observation_request,
)
from agent_memory.serialization import to_jsonable


def protocol(*, wall=False, rate="0.1", rates=None):
    rates = (
        rates
        if rates is not None
        else tuple(
            ResourceRate(spec.resource, spec.unit, rate)
            for spec in RESOURCE_SCHEMA
            if wall or spec.resource != ResourceKind.WALL
        )
    )
    return ResourcePricingProtocol(
        "USD",
        "a" * 64,
        ResourceTariff(
            "USD",
            "synthetic-tariff:test-only",
            rates,
            "Separate elapsed-service allocation, excluding process CPU" if wall else None,
        ),
    )


def phase(identity="resources:synthetic-1", cpu=1.0, wall=100.0, *, failed=False):
    observation = dict(
        operation_id=identity,
        phase="foreground",
        evidence="synthetic counter observation, not a real experiment",
        cpu_ms=cpu,
        wall_ms=wall,
        failed=failed,
        pricing="unknown",
        unmeasured_resources=["gpu", "io", "storage", "network"],
    )
    entry = CostEntry(identity, CostPhase.FOREGROUND, ("request-1",), cpu_ms=cpu)
    return observation, CostSnapshot("USD", (entry,), (), (CostPhase.FOREGROUND,))


def receipt(observation, config, *, amount="1", omit=()):
    return PhaseResourceObservation(
        resource_observation_request(
            observation, config, expected_protocol_sha256=config.fingerprint
        ),
        tuple(
            ResourceMeasurement(spec.resource, spec.unit, amount, "synthetic measured delta")
            for spec in RESOURCE_SCHEMA
            if spec.resource not in {ResourceKind.CPU, ResourceKind.WALL, *omit}
        ),
    )


def project(observation, snapshot, config, records=None, **kwargs):
    return reconcile_resources(
        (observation,),
        snapshot,
        config,
        expected_protocol_sha256=config.fingerprint,
        records=(receipt(observation, config),) if records is None else records,
        observer_finalized=kwargs.pop("observer_finalized", True),
        **kwargs,
    )


def test_complete_exact_sum_rounds_once_and_wall_is_reporting_only():
    observation, snapshot = phase()
    result = project(observation, snapshot, protocol())
    (entry,) = result.snapshot.entries
    (report,) = result.phases
    # Five resources at 0.1 microunits each total 0.5, rounded once to 1.
    assert entry.priced_resource_microunits == 1
    assert entry.io_bytes == entry.storage_byte_seconds == entry.network_bytes == 1
    assert report.pricing == "priced" and not report.reasons
    assert not report.missing_resources and not report.missing_tariffs
    assert {m.resource for m in report.measurements} == set(ResourceKind)
    assert next(m.quantity for m in report.measurements if m.resource == "wall") == "100"
    assert entry.tariff_reference.endswith(result.protocol.tariff.fingerprint)
    assert snapshot.entries[0].actual_microunits is None  # Inputs were not mutated.
    assert observation["pricing"] == "unknown"


def test_explicit_wall_tariff_is_frozen_and_charged_once():
    observation, snapshot = phase()
    config = protocol(wall=True)
    result = project(observation, snapshot, config)
    assert result.snapshot.entries[0].actual_microunits == 11
    assert config.fingerprint != protocol().fingerprint
    with pytest.raises(ValueError, match="non-overlapping"):
        replace(config.tariff, wall_time_charge_basis=None)
    with pytest.raises(ValueError, match="non-overlapping"):
        replace(protocol().tariff, wall_time_charge_basis="unbound wall charge")


def test_decimal_context_never_changes_rate_quantities_hash_or_price():
    observation, snapshot = phase(cpu=1.0)
    config = protocol(rate="0.200000000000000000000000000000000000000001")
    expected = project(observation, snapshot, config)
    with localcontext() as context:
        context.prec = 2
        low_precision = protocol(rate=Decimal("0.200000000000000000000000000000000000000001"))
        actual = project(observation, snapshot, low_precision)
    assert actual == expected
    assert actual.snapshot.entries[0].actual_microunits == 2


def test_explicit_observed_zero_can_be_priced_but_missing_zero_cannot():
    observation, snapshot = phase(cpu=0, wall=0)
    config = protocol()
    record = receipt(observation, config, amount="0")
    measured = project(observation, snapshot, config, (record,))
    assert measured.snapshot.entries[0].actual_microunits == 0
    missing = project(observation, snapshot, config, ())
    assert missing.snapshot.entries[0].actual_microunits is None
    assert set(missing.phases[0].missing_resources) == {"gpu", "io", "storage", "network"}


@pytest.mark.parametrize(
    "kind", [ResourceKind.GPU, ResourceKind.IO, ResourceKind.STORAGE, ResourceKind.NETWORK]
)
def test_each_missing_dimension_remains_unknown_even_with_tariff(kind):
    observation, snapshot = phase()
    config = protocol()
    result = project(observation, snapshot, config, (receipt(observation, config, omit=(kind,)),))
    assert result.snapshot.entries[0].actual_microunits is None
    assert result.phases[0].missing_resources == (kind,)


@pytest.mark.parametrize(
    "kind",
    [
        ResourceKind.CPU,
        ResourceKind.GPU,
        ResourceKind.IO,
        ResourceKind.STORAGE,
        ResourceKind.NETWORK,
    ],
)
def test_each_missing_tariff_remains_unknown_even_when_quantity_is_zero(kind):
    observation, snapshot = phase(cpu=0, wall=0)
    rates = tuple(rate for rate in protocol().tariff.rates if rate.resource != kind)
    config = protocol(rates=rates)
    result = project(observation, snapshot, config, (receipt(observation, config, amount="0"),))
    assert result.snapshot.entries[0].actual_microunits is None
    assert result.phases[0].missing_tariffs == (kind,)
    assert not result.phases[0].missing_resources


def test_no_tariff_no_observer_preserves_unknown_and_lists_all_missing_rates():
    observation, snapshot = phase()
    config = ResourcePricingProtocol()
    result = project(observation, snapshot, config, (), observer_finalized=False)
    report = result.phases[0]
    assert report.pricing == "unknown"
    assert set(report.missing_tariffs) == set(ResourceKind) - {ResourceKind.WALL}
    assert set(report.missing_resources) == {"gpu", "io", "storage", "network"}
    assert {m.resource for m in report.measurements} == {"cpu", "wall"}


def test_failed_observation_and_failed_finalization_cannot_be_priced():
    observation, snapshot = phase()
    config = protocol()
    good = receipt(observation, config)
    failed = replace(good, finalized=False, failure_reason="observer_error:OSError")
    first = project(observation, snapshot, config, (failed,))
    assert first.snapshot.entries[0].actual_microunits is None
    assert "observer_error:OSError" in first.phases[0].reasons
    second = project(
        observation,
        snapshot,
        config,
        (good,),
        observer_finalized=False,
        observer_finalization_error="observer_finalization_error:OSError",
    )
    assert second.snapshot.entries[0].actual_microunits is None
    assert "observer_finalization_error:OSError" in second.phases[0].reasons
    assert not second.phases[0].missing_resources
    with pytest.raises(ValueError, match="finalization"):
        project(observation, snapshot, config, observer_finalization_error="failed")


def test_finalization_must_be_explicit_even_for_complete_observations():
    observation, snapshot = phase()
    config = protocol()
    result = reconcile_resources(
        (observation,),
        snapshot,
        config,
        expected_protocol_sha256=config.fingerprint,
        records=(receipt(observation, config),),
    )
    assert result.snapshot.entries[0].actual_microunits is None
    assert "observer_not_finalized" in result.phases[0].reasons


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_id", "resources:wrong"),
        ("phase", CostPhase.WRITE),
        ("currency", "EUR"),
        ("phase_observation_sha256", "b" * 64),
        ("protocol_sha256", "b" * 64),
        ("observer_configuration_sha256", "b" * 64),
        ("evidence", "different source evidence"),
        ("cpu_ms", "2"),
        ("wall_ms", "2"),
        ("failed", True),
    ],
)
def test_wrong_observation_binding_fails_closed(field, value):
    observation, snapshot = phase()
    config = protocol()
    good = receipt(observation, config)
    wrong = replace(good, request=replace(good.request, **{field: value}))
    with pytest.raises(ValueError, match="binding"):
        project(observation, snapshot, config, (wrong,))


def test_phase_source_hash_binds_all_original_evidence_fields():
    observation, snapshot = phase()
    config = protocol()
    record = receipt(observation, config)
    changed = {**observation, "unmeasured_resources": []}
    with pytest.raises(ValueError, match="binding"):
        project(changed, snapshot, config, (record,))


def test_cpu_wall_cannot_be_overridden_by_host_observer():
    observation, snapshot = phase()
    config = protocol()
    good = receipt(observation, config)
    for resource in (ResourceKind.CPU, ResourceKind.WALL):
        wrong = replace(
            good,
            measurements=(
                *good.measurements,
                ResourceMeasurement(resource, "ms", "999", "conflicting sensor"),
            ),
        )
        with pytest.raises(ValueError, match="changed measured"):
            project(observation, snapshot, config, (wrong,))


@pytest.mark.parametrize(
    "value", ["NaN", "Infinity", "-Infinity", "-1", True, 0.1, "nonsense", "1e999999"]
)
def test_nonfinite_negative_inexact_or_invalid_quantities_and_rates_rejected(value):
    with pytest.raises(ValueError):
        ResourceMeasurement(ResourceKind.GPU, "ms", value, "measured")
    with pytest.raises(ValueError):
        ResourceRate(ResourceKind.GPU, "ms", value)


def test_wrong_units_unknown_dimensions_and_nonintegral_counters_rejected():
    for constructor in (ResourceMeasurement, ResourceRate):
        args = ("1", "evidence") if constructor is ResourceMeasurement else ("1",)
        with pytest.raises(ValueError, match="unsupported unit"):
            constructor("gpu", "seconds", *args)
        with pytest.raises(ValueError):
            constructor("energy", "joules", *args)
    for kind, unit in (("io", "bytes"), ("storage", "byte_seconds"), ("network", "bytes")):
        with pytest.raises(ValueError, match="integers"):
            ResourceMeasurement(kind, unit, "0.5", "evidence")
        with pytest.raises(ValueError, match="integers"):
            ResourceMeasurement(kind, unit, str(2**63), "evidence")
    with pytest.raises(ValueError, match="evidence"):
        ResourceMeasurement("gpu", "ms", "0", "")


def test_currency_schema_rounding_and_protocol_freeze_fail_closed():
    observation, snapshot = phase()
    config = protocol()
    with pytest.raises(ValueError, match="currency"):
        replace(config, currency="EUR")
    with pytest.raises(ValueError, match="currency"):
        project(observation, replace(snapshot, currency="EUR"), config)
    with pytest.raises(ValueError, match="schema"):
        replace(config, schema_sha256="b" * 64)
    with pytest.raises(ValueError, match="schema"):
        replace(config, schema_version="question-resources/999")
    with pytest.raises(ValueError, match="charge scope"):
        replace(config, charge_scope="include_remote_provider_resources")
    with pytest.raises(ValueError, match="rounding"):
        replace(config.tariff, rounding_policy="truncate")
    changed = replace(config, tariff=replace(config.tariff, reference="changed tariff"))
    with pytest.raises(ValueError, match="after freeze"):
        reconcile_resources(
            (observation,), snapshot, changed, expected_protocol_sha256=config.fingerprint
        )


def test_duplicate_phase_records_rates_and_measurements_rejected():
    observation, snapshot = phase()
    config = protocol()
    good = receipt(observation, config)
    with pytest.raises(ValueError, match="duplicate phase"):
        reconcile_resources(
            (observation, observation),
            snapshot,
            config,
            expected_protocol_sha256=config.fingerprint,
        )
    with pytest.raises(ValueError, match="duplicate resource"):
        project(observation, snapshot, config, (good, good))
    with pytest.raises(ValueError, match="duplicate tariff"):
        replace(config.tariff, rates=(*config.tariff.rates, config.tariff.rates[0]))
    with pytest.raises(ValueError, match="duplicate measurement"):
        replace(good, measurements=(*good.measurements, good.measurements[0]))


def test_missing_phase_source_retains_unknown_without_counter_defaults_as_evidence():
    _, snapshot = phase()
    config = protocol()
    result = reconcile_resources((), snapshot, config, expected_protocol_sha256=config.fingerprint)
    report = result.phases[0]
    assert report.pricing == "unknown" and not report.measurements
    assert set(report.missing_resources) == set(ResourceKind)
    assert "missing_phase_observation" in report.reasons


def test_source_must_match_unpriced_resource_entry_and_measured_cpu():
    observation, snapshot = phase()
    config = protocol()
    with pytest.raises(ValueError, match="unpriced"):
        project({**observation, "pricing": "free"}, snapshot, config, ())
    with pytest.raises(ValueError, match="snapshot"):
        project({**observation, "cpu_ms": 4.0}, snapshot, config, ())
    with pytest.raises(ValueError, match="unpriced"):
        project({**observation, "operation_id": "missing"}, snapshot, config, ())
    for quantity in (float("nan"), float("inf"), -1.0):
        with pytest.raises(ValueError):
            project({**observation, "cpu_ms": quantity}, snapshot, config, ())


def test_existing_model_bills_pending_and_failed_work_are_preserved():
    observation, snapshot = phase(failed=True)
    model = CostEntry(
        "model-1",
        CostPhase.FOREGROUND,
        model_call=True,
        provider="fixture",
        provider_billed_microunits=123,
        billing_reference="synthetic receipt",
    )
    pending = PendingResponsibility("still-pending", (), 99, CostPhase.FOREGROUND)
    snapshot = replace(snapshot, entries=(*snapshot.entries, model), pending=(pending,))
    result = project(observation, snapshot, protocol())
    assert result.snapshot.entries[1] == model
    assert result.snapshot.pending == (pending,)
    assert result.snapshot.entries[0].actual_microunits == 1
    assert len(result.phases) == 1


def test_cost_overflow_and_repricing_are_rejected():
    observation, snapshot = phase()
    with pytest.raises(ValueError, match="signed-64-bit microunits"):
        project(observation, snapshot, protocol(rate=str(2**63)))
    config = protocol()
    result = project(observation, snapshot, config)
    with pytest.raises(ValueError, match="unpriced"):
        project(observation, result.snapshot, config)
    with pytest.raises(ValueError, match="reprice"):
        reconcile_resources(
            (), result.snapshot, config, expected_protocol_sha256=config.fingerprint
        )


def test_contract_records_are_immutable_canonical_and_json_serializable():
    config = protocol(rate="0.10000")
    assert config == protocol(rate=Decimal("0.1"))
    assert config.fingerprint == protocol(rate="1e-1").fingerprint
    assert config.schema_sha256 == RESOURCE_SCHEMA_SHA256
    with pytest.raises(FrozenInstanceError):
        config.currency = "EUR"
    observation, snapshot = phase()
    encoded = json.loads(json.dumps(to_jsonable(project(observation, snapshot, config))))
    assert encoded["protocol"]["tariff"]["rates"][0]["microunits_per_unit"] == "0.1"
    assert encoded["phases"][0]["pricing"] == "priced"


def test_observer_configuration_must_be_frozen_before_accepting_records():
    observation, snapshot = phase()
    config = replace(protocol(), observer_configuration_sha256=None)
    with pytest.raises(ValueError, match="frozen observer"):
        project(observation, snapshot, config)


def test_explicit_zero_rates_are_allowed_but_do_not_waive_observation():
    observation, snapshot = phase()
    config = protocol(rate="0")
    assert project(observation, snapshot, config).snapshot.entries[0].actual_microunits == 0
    assert project(observation, snapshot, config, ()).snapshot.entries[0].actual_microunits is None


def test_distinct_operations_are_priced_once_and_partial_work_is_retained():
    first, first_snapshot = phase("resources:first")
    second, second_snapshot = phase("resources:second")
    config = protocol()
    snapshot = replace(first_snapshot, entries=(*first_snapshot.entries, *second_snapshot.entries))
    result = reconcile_resources(
        (first, second),
        snapshot,
        config,
        expected_protocol_sha256=config.fingerprint,
        records=(
            receipt(first, config),
            receipt(second, config, omit=(ResourceKind.NETWORK,)),
        ),
        observer_finalized=True,
    )
    assert [entry.actual_microunits for entry in result.snapshot.entries] == [1, None]
    assert [report.operation_id for report in result.phases] == [
        "resources:first",
        "resources:second",
    ]
    assert result.phases[1].missing_resources == (ResourceKind.NETWORK,)


def test_unfinalized_record_cannot_claim_success_or_suppress_failure():
    observation, _ = phase()
    record = receipt(observation, protocol())
    with pytest.raises(ValueError, match="failed observation"):
        replace(record, failure_reason="observer_error:OSError")
    with pytest.raises(ValueError, match="boolean"):
        replace(record, finalized=1)


def test_decimal_measurements_use_exact_reported_amounts_with_noninteger_time():
    observation, snapshot = phase(cpu=0.125, wall=0.25)
    config = protocol(rate="0.00001")
    record = receipt(observation, config, amount="0")
    record = replace(
        record,
        measurements=tuple(
            replace(value, quantity="0.000000000000000000000000000001")
            if value.resource == ResourceKind.GPU
            else value
            for value in record.measurements
        ),
    )
    result = project(observation, snapshot, config, (record,))
    assert result.snapshot.entries[0].actual_microunits == 1
    assert (
        next(
            value.quantity
            for value in result.phases[0].measurements
            if value.resource == ResourceKind.CPU
        )
        == "0.125"
    )
