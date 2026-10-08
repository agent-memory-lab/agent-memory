"""V7-B0 pure protocol tests, not evidence for live question-view capabilities."""

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from agent_memory.derived.model import DerivedError, FacetRefreshUnit, digest
from agent_memory.derived.question_model import (
    ENABLED_QUESTION_CAPABILITIES,
    QUESTION_CAPABILITY_DEPENDENCIES,
    AnswerStatus,
    AvailabilityStatus,
    ComputeMode,
    CoverageFrontier,
    CoverageTarget,
    ExactSnapshotTarget,
    InputManifest,
    InputReference,
    QueryCoverage,
    QuestionCertificate,
    QuestionContent,
    QuestionContext,
    QuestionDefinition,
    QuestionHead,
    QuestionInstance,
    QuestionTime,
    RefreshPolicyRef,
    RefreshStatus,
    SourceBasis,
    TimeCoverage,
    TimeMode,
    require_question_capability,
)
from agent_memory.domain import MemoryScope

NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)
SHA = "a" * 64
OTHER_SHA = "b" * 64


def object_schema(properties, required=()):
    return dict(
        type="object", properties=properties, required=list(required), additionalProperties=False
    )


def definition(**changes):
    values = dict(
        id="project.owner",
        version="1",
        generation=1,
        scope=MemoryScope("tenant", user_id="alice", workspace_id="project-a"),
        parameter_schema=object_schema(
            {"project": dict(type="string", maxLength=128)}, ["project"]
        ),
        output_schema=object_schema({"owner": dict(type="string", maxLength=256)}),
        scope_bindings={"project": "workspace_id"},
        query_id="owner-candidates",
        query_fingerprint=SHA,
        predicate_versions={"project.owner": "1"},
        qualification_policy_version="qualified-owner/1",
        business_policy_version="owner/1",
        unknown_semantics="missing-is-unknown/1",
        conflict_semantics="exclusive-is-contested/1",
        empty_semantics="explicit-no-owner-only/1",
        required_fields=("owner",),
        allowed_parent_kinds=("l1",),
        source_basis=SourceBasis.ADMITTED_L1,
        completeness="complete_candidates",
        time_mode=TimeMode.CURRENT,
        timezone="UTC",
        calendar_version="iso/1",
        renderer_id="owner-renderer",
        renderer_version="1",
        allowed_modes=(ComputeMode.FULL,),
        audiences=("alice", "bob"),
        purposes=("agent_context", "inspection"),
        retention_policy_version="project-retention/1",
        max_output_bytes=2048,
        max_dependencies=16,
        max_instances=128,
        configuration_version="1",
        refresh_policy=RefreshPolicyRef("owner-refresh", 1),
    )
    values.update(changes)
    return QuestionDefinition(**values)


def context(**changes):
    values = dict(
        issuer_id="host",
        revision="acl-context/1",
        attributes={"project": "project-a", "nested": ["value"]},
        expires_at=NOW + timedelta(days=1),
    )
    values.update(changes)
    return QuestionContext(**values)


def instance(**changes):
    values = dict(
        definition=definition(),
        parameters={"project": "project-a"},
        context=context(),
        audience=("alice",),
        purpose="agent_context",
        time=QuestionTime(TimeMode.CURRENT, None, None),
    )
    values.update(changes)
    return QuestionInstance(**values)


def manifest(source="private-source", **changes):
    values = dict(
        inputs=(InputReference("source", source, "revision/1", SHA),),
        snapshot_token="snapshot/1",
        algorithm_version="census/1",
    )
    values.update(changes)
    return InputManifest(**values)


def frontier(**changes):
    values = dict(mode="continuous", positions={"facts": 7}, units=())
    values.update(changes)
    return CoverageFrontier(**values)


def coverage(**changes):
    values = dict(
        query_id="owner-candidates",
        query_fingerprint=SHA,
        query_generation=4,
        subscription_version="1",
        qualification_policy_version="qualified-owner/1",
        source_basis=SourceBasis.ADMITTED_L1,
        frontier=frontier(),
        candidates_complete=True,
        truncation_reason=None,
        publication_manifest_digest=None,
        publication_closed=None,
    )
    values.update(changes)
    return QueryCoverage(**values)


def content(**changes):
    values = dict(
        instance=instance(),
        answer_status=AnswerStatus.RESOLVED,
        value={"owner": "Alice"},
        structure={"blocks": ["owner"]},
        renderer_version="1",
        model_version=None,
        generation_manifest=manifest(),
    )
    values.update(changes)
    return QuestionContent(**values)


def certificate(**changes):
    result = content()
    validation = manifest("public-source")
    values = dict(
        content_revision_id=result.id,
        instance_id=result.instance.id,
        scope=result.instance.definition.scope,
        definition_fingerprint=result.instance.definition.semantic_fingerprint,
        context_fingerprint=result.instance.context.fingerprint,
        validation_manifest=validation,
        input_frontier=frontier(),
        query_coverage=coverage(),
        support=validation.inputs,
        time_coverage=TimeCoverage(NOW, NOW + timedelta(hours=1), NOW, None),
        safety_fingerprint=SHA,
        safety_epoch=0,
        validation_algorithm_version="validate/1",
        refresh_policy=RefreshPolicyRef("owner-refresh", 1),
        validated_at=NOW,
    )
    values.update(changes)
    return QuestionCertificate(**values)


def target_args():
    value = instance()
    return dict(
        instance_id=value.id,
        scope=value.definition.scope,
        definition_fingerprint=value.definition.semantic_fingerprint,
        context_fingerprint=value.context.fingerprint,
        time=value.time,
        requested_at=NOW,
        request_semantics_digest=SHA,
    )


def contracts():
    return [
        RefreshPolicyRef("policy", 1),
        QuestionTime(TimeMode.CURRENT, None, None),
        QuestionTime(TimeMode.EXACT_HISTORICAL, NOW, NOW - timedelta(days=1)),
        context(),
        definition(),
        instance(),
        manifest().inputs[0],
        manifest(),
        frontier(),
        frontier(mode="exact_units", positions={}, units=("unit-b", "unit-a")),
        coverage(),
        TimeCoverage(NOW, None, NOW, None),
        content(),
        certificate(),
        QuestionHead.bind(content(), certificate(), epoch=0),
        CoverageTarget(**target_args(), required_frontier=frontier()),
        ExactSnapshotTarget(
            **target_args(), snapshot_token="snapshot/1", unit_id="unit/1", frontier=frontier()
        ),
    ]


@pytest.mark.parametrize("record", contracts(), ids=lambda value: type(value).__name__)
def test_strict_json_roundtrip(record):
    payload = record.payload()
    decoded = type(record).from_payload(payload)
    assert decoded == record
    assert type(record).from_payload(json.loads(json.dumps(payload))) == record
    assert decoded.payload() == payload
    if hasattr(record, "id"):
        assert decoded.id == record.id


@pytest.mark.parametrize("record", contracts(), ids=lambda value: type(value).__name__)
@pytest.mark.parametrize("damage", ["unknown", "missing", "schema", "nonobject"])
def test_all_wire_envelopes_reject_shape_drift(record, damage):
    wire = record.payload()
    if damage == "unknown":
        wire["unexpected"] = True
    elif damage == "missing":
        wire.pop("schema")  # Dataclass defaults must never fabricate wire compatibility.
    elif damage == "schema":
        wire["schema"] = "unknown/99"
    else:
        wire = list(wire.items())
    with pytest.raises(DerivedError):
        type(record).from_payload(wire)


@pytest.mark.parametrize(
    "path",
    [
        ("definition",),
        ("definition", "scope"),
        ("definition", "refresh_policy"),
        ("context",),
        ("time",),
    ],
)
def test_unknown_nested_contract_fields_rejected(path):
    wire = instance().payload()
    node = wire
    for name in path:
        node = node[name]
    node["unknown"] = "not ignored"
    with pytest.raises(DerivedError):
        QuestionInstance.from_payload(wire)


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation", True),
        ("generation", 1.0),
        ("generation", "1"),
        ("max_instances", 0),
        ("max_output_bytes", -1),
        ("max_dependencies", False),
        ("scope_bindings", {"project": ["workspace_id"]}),
        ("scope_bindings", {"project": "session_id"}),
        ("parameter_schema", {"type": "object"}),
        ("query_fingerprint", "A" * 64),
        ("predicate_versions", {}),
        ("predicate_versions", {"owner": True}),
        ("time_mode", "current"),
        ("source_basis", "admitted_l1"),
        ("allowed_modes", ("full",)),
        ("allowed_modes", (ComputeMode.DELTA,)),
        ("allowed_modes", (ComputeMode.FULL, ComputeMode.FULL)),
        ("allowed_modes", (["full"],)),
        ("allowed_parent_kinds", ("raw_l0",)),
        ("audiences", ("alice", "alice")),
        ("purposes", []),
        ("timezone", "not/a/timezone"),
        ("completeness", "top_k_is_complete"),
        ("refresh_policy", {"id": "policy", "revision": 1}),
        ("required_fields", ("missing",)),
        ("id", " trailing "),
    ],
)
def test_definition_rejects_bad_types_modes_and_shapes(field, value):
    with pytest.raises(DerivedError):
        definition(**{field: value})


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "object", "properties": {}, "required": [], "additionalProperties": True},
        {"type": "string", "maxLength": 0},
        {"type": "string", "maxLength": 16, "default": "fabricated"},
        {"type": ["string", "null"], "maxLength": 16},
        {"$ref": "https://example.invalid/schema"},
        {"type": "array", "items": {"type": "boolean"}, "maxItems": True, "uniqueItems": False},
        {"type": "number", "minimum": 0, "maximum": float("inf")},
        {"type": "integer", "minimum": False, "maximum": 10},
        {"type": "string", "maxLength": 8, "enum": ["same", "same"]},
        {"type": "boolean", "enum": [1]},
    ],
)
def test_unsupported_schema_shapes_rejected(schema):
    with pytest.raises(DerivedError):
        definition(parameter_schema=object_schema({"value": schema}), scope_bindings={})


def test_parameter_normalization_is_explicit_and_schema_checked():
    schema = object_schema(
        {
            "label": {"type": "string", "maxLength": 32},
            "count": {"type": "integer", "minimum": 0, "maximum": 10},
            "number": {"type": "number", "minimum": 0, "maximum": 10},
            "flag": {"type": "boolean"},
            "tags": {
                "type": "array",
                "items": {"type": "string", "maxLength": 8},
                "maxItems": 4,
                "uniqueItems": True,
            },
        },
        ["label", "count", "number", "flag", "tags"],
    )
    spec = definition(parameter_schema=schema, scope_bindings={})
    args = dict(label="cafe\u0301", count=1, number=1, flag=True, tags=["a", "b"])
    first = instance(definition=spec, parameters=args)
    second = instance(definition=spec, parameters={**args, "label": "caf\u00e9", "number": 1.0})
    assert first.id == second.id
    assert first.parameters["label"] == "caf\u00e9"
    for changed in (
        {**args, "count": True},
        {**args, "count": 1.0},
        {**args, "flag": 1},
        {**args, "number": float("nan")},
        {**args, "tags": ["a", "a"]},
        {**args, "unknown": "extra"},
        {key: value for key, value in args.items() if key != "label"},
    ):
        with pytest.raises(DerivedError):
            instance(definition=spec, parameters=changed)
    assert first.id != instance(definition=spec, parameters={**args, "tags": ["b", "a"]}).id


def test_policy_binding_does_not_change_semantic_or_content_identity():
    first = definition()
    revised = replace(first, refresh_policy=RefreshPolicyRef("other-policy", 99))
    assert first.payload() != revised.payload()
    assert first.semantic_fingerprint == revised.semantic_fingerprint
    old = instance(definition=first)
    new = instance(definition=revised)
    assert old.id == new.id
    assert content(instance=old).id == content(instance=new).id
    assert certificate().id != certificate(refresh_policy=revised.refresh_policy).id
    for change in (
        dict(renderer_version="2"),
        dict(business_policy_version="2"),
        dict(query_fingerprint=OTHER_SHA),
        dict(qualification_policy_version="2"),
        dict(completeness="partial_allowed"),
        dict(empty_semantics="different/2"),
    ):
        assert replace(first, **change).semantic_fingerprint != first.semantic_fingerprint


def test_set_like_schema_order_is_normalized():
    first = definition(audiences=("alice", "bob"), required_fields=("owner",))
    assert (
        first.semantic_fingerprint
        == replace(first, audiences=("bob", "alice")).semantic_fingerprint
    )
    assert instance(audience=("bob", "alice")).id == instance(audience=("alice", "bob")).id
    schema = object_schema({"a": {"type": "boolean"}, "b": {"type": "boolean"}}, ["a", "b"])
    left = definition(parameter_schema=schema, scope_bindings={})
    schema["required"].reverse()
    assert (
        left.semantic_fingerprint
        == definition(parameter_schema=schema, scope_bindings={}).semantic_fingerprint
    )


def test_identity_partitions_scope_context_audience_purpose_and_history():
    base = instance()
    variants = [
        instance(
            definition=replace(
                base.definition, scope=replace(base.definition.scope, tenant_id="other")
            )
        ),
        instance(context=context(revision="context/2")),
        instance(context=context(attributes={"project": "project-b"})),
        instance(audience=("bob",)),
        instance(purpose="inspection"),
        instance(
            definition=definition(time_mode=TimeMode.EXACT_HISTORICAL),
            time=QuestionTime(TimeMode.EXACT_HISTORICAL, NOW, NOW),
        ),
    ]
    assert len({base.id, *(item.id for item in variants)}) == len(variants) + 1
    with pytest.raises(DerivedError):
        instance(parameters={"project": "project-b"})
    with pytest.raises(DerivedError):
        instance(audience=("mallory",))
    with pytest.raises(DerivedError):
        instance(purpose="unregistered")


def test_input_ownership_and_returned_payloads_are_deeply_isolated():
    attrs = {"nested": [{"value": "original"}]}
    record = context(attributes=attrs)
    before = record.fingerprint
    attrs["nested"][0]["value"] = "changed"
    assert record.fingerprint == before
    with pytest.raises(TypeError):
        record.attributes["nested"][0]["value"] = "changed"
    with pytest.raises(FrozenInstanceError):
        record.revision = "changed"
    payload = record.payload()
    payload["attributes"]["nested"][0]["value"] = "mutated wire"
    assert record.fingerprint == before
    schema = object_schema({"project": {"type": "string", "maxLength": 128}}, ["project"])
    spec = definition(parameter_schema=schema)
    schema["properties"]["project"]["maxLength"] = 1
    assert spec.parameter_schema["properties"]["project"]["maxLength"] == 128
    positions = {"facts": 7}
    fixed = frontier(positions=positions)
    positions["facts"] = 900
    assert fixed.positions["facts"] == 7


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), {1: "bad key"}, {"bad": object()}, {"bad": {1, 2}}]
)
def test_json_values_do_not_coerce_unsupported_types(value):
    with pytest.raises(DerivedError):
        context(attributes={"value": value})


def test_original_generation_manifest_never_becomes_new_validation_evidence():
    old = content()
    proof = certificate()
    assert old.generation_manifest.inputs[0].id == "private-source"
    assert proof.validation_manifest.inputs[0].id == "public-source"
    assert proof.content_revision_id == old.id
    assert old.generation_manifest.inputs[0].id == "private-source"
    regenerated = replace(old, generation_manifest=proof.validation_manifest)
    assert regenerated.value_digest == old.value_digest
    assert regenerated.structure_digest == old.structure_digest
    assert regenerated.id != old.id
    assert proof.id != replace(proof, validation_algorithm_version="validate/2").id
    assert old.id != replace(old, model_version="model/2").id


def test_value_digest_keeps_business_status_and_structural_digest_separate():
    base = content()
    assert replace(base, answer_status=AnswerStatus.CONTESTED).value_digest != base.value_digest
    changed_structure = replace(base, structure={"blocks": ["owner", "explanation"]})
    assert changed_structure.value_digest == base.value_digest
    assert changed_structure.structure_digest != base.structure_digest
    assert set(AnswerStatus) == {"resolved", "unknown", "contested", "empty", "incomplete"}
    assert set(AvailabilityStatus) == {"valid", "stale", "invalid", "erased"}
    assert set(RefreshStatus) == {"idle", "pending", "running", "retry", "deferred", "dead"}
    with pytest.raises(DerivedError):
        content(answer_status="resolved")
    with pytest.raises(DerivedError):
        content(value={})
    assert (
        content(value={}, answer_status=AnswerStatus.UNKNOWN).answer_status == AnswerStatus.UNKNOWN
    )
    with pytest.raises(DerivedError):
        content(instance=instance(definition=definition(max_output_bytes=1)))
    with pytest.raises(DerivedError):
        content(renderer_version="wrong")


@pytest.mark.parametrize(
    "changes",
    [
        dict(mode="maximum_seen"),
        dict(positions={"facts": True}),
        dict(positions={"facts": -1}),
        dict(positions={}),
        dict(units=("unit",)),
        dict(mode="exact_units", positions={}, units=()),
        dict(mode="exact_units", positions={}, units=("unit", "unit")),
    ],
)
def test_frontiers_cannot_mix_vectors_units_or_unknown_coverage(changes):
    with pytest.raises(DerivedError):
        frontier(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        dict(candidates_complete=1),
        dict(candidates_complete=True, truncation_reason="top_k"),
        dict(candidates_complete=False),
        dict(publication_closed=True),
        dict(source_basis=SourceBasis.PUBLICATION_MANIFEST),
        dict(
            source_basis=SourceBasis.PUBLICATION_MANIFEST,
            publication_manifest_digest=SHA,
            publication_closed=False,
        ),
    ],
)
def test_query_completeness_never_fabricates_publication_closure(changes):
    with pytest.raises(DerivedError):
        coverage(**changes)
    partial = coverage(candidates_complete=False, truncation_reason="budget")
    assert not partial.candidates_complete


def test_closed_publication_and_empty_manifest_are_explicit():
    full = coverage(
        source_basis=SourceBasis.PUBLICATION_MANIFEST,
        publication_manifest_digest=SHA,
        publication_closed=True,
    )
    assert QueryCoverage.from_payload(full.payload()) == full
    empty = manifest(inputs=())
    assert empty.inputs == ()  # Empty supported result still carries query coverage.
    with pytest.raises(DerivedError):
        certificate(validation_manifest=empty)
    assert certificate(validation_manifest=empty, support=()).support == ()


def test_time_coordinates_are_exact_and_current_target_is_not_historical():
    offset = NOW.astimezone(timezone(timedelta(hours=2)))
    first = QuestionTime(TimeMode.EXACT_HISTORICAL, offset, offset)
    assert first.payload() == QuestionTime(TimeMode.EXACT_HISTORICAL, NOW, NOW).payload()
    for values in (
        (TimeMode.CURRENT, NOW, NOW),
        (TimeMode.EXACT_HISTORICAL, NOW, None),
        (TimeMode.EXACT_HISTORICAL, NOW.replace(tzinfo=None), NOW),
    ):
        with pytest.raises(DerivedError):
            QuestionTime(*values)
    with pytest.raises(DerivedError):
        TimeCoverage(NOW, NOW, NOW, None)
    with pytest.raises(DerivedError):
        CoverageTarget(**{**target_args(), "time": first}, required_frontier=frontier())
    exact = ExactSnapshotTarget(
        **{**target_args(), "time": first},
        snapshot_token="snapshot/1",
        unit_id="unit/1",
        frontier=frontier(),
    )
    assert exact.time == first
    with pytest.raises(DerivedError):
        instance(
            definition=definition(time_mode=TimeMode.EXACT_HISTORICAL),
            time=QuestionTime(TimeMode.EXACT_HISTORICAL, NOW, NOW + timedelta(days=1)),
        )


def test_exact_and_coverage_wire_are_not_interchangeable_and_old_unit_unchanged():
    exact = ExactSnapshotTarget(
        **target_args(), snapshot_token="snapshot/1", unit_id="unit/1", frontier=frontier()
    )
    lower_bound = CoverageTarget(**target_args(), required_frontier=frontier())
    assert exact.id != lower_bound.id
    assert replace(exact, snapshot_token="snapshot/2").id != exact.id
    for cls, payload in (
        (CoverageTarget, exact.payload()),
        (ExactSnapshotTarget, lower_bound.payload()),
    ):
        with pytest.raises(DerivedError):
            cls.from_payload(payload)
    legacy = FacetRefreshUnit("facet", SHA, 1, 0, {"slot": 1}, 0, 0)
    expected = dict(
        facet_id="facet",
        definition_sha256=SHA,
        definition_generation=1,
        epoch=0,
        query_generation={"slot": 1},
        safety_generation=0,
        time_generation=0,
        schema="facet-refresh-unit/1",
    )
    assert legacy.payload() == expected
    assert legacy.id == "facet-unit:" + digest(expected)
    with pytest.raises(DerivedError):
        CoverageTarget.from_payload(legacy.payload())


def test_head_binds_content_certificate_definition_and_epoch_without_publishing():
    body = content()
    proof = certificate()
    head = QuestionHead.bind(body, proof, epoch=4)
    assert head.content_revision_id == body.id
    assert head.certificate_revision_id == proof.id
    assert head.definition_generation == body.instance.definition.generation
    assert head.epoch == 4
    for changes in (
        dict(instance_id="question-instance:" + OTHER_SHA),
        dict(content_revision_id="question-content:" + OTHER_SHA),
        dict(definition_fingerprint=OTHER_SHA),
        dict(context_fingerprint=OTHER_SHA),
        dict(scope=MemoryScope("other")),
        dict(query_coverage=coverage(query_fingerprint=OTHER_SHA)),
    ):
        with pytest.raises(DerivedError):
            QuestionHead.bind(body, replace(proof, **changes), epoch=4)
    for changes in (
        dict(epoch=True),
        dict(definition_generation=0),
        dict(content_revision_id="legacy"),
        dict(certificate_revision_id="legacy"),
    ):
        with pytest.raises(DerivedError):
            replace(head, **changes)


def test_unicode_identity_and_enum_collisions_are_not_silently_merged():
    with pytest.raises(DerivedError):
        context(attributes={"cafe\u0301": "value", "caf\u00e9": "other"})
    with pytest.raises(DerivedError):
        definition(
            parameter_schema=object_schema(
                {"label": {"type": "string", "maxLength": 32, "enum": ["cafe\u0301", "caf\u00e9"]}}
            ),
            scope_bindings={},
        )
    with pytest.raises(DerivedError):
        definition(id="cafe\u0301")


def test_all_proposed_runtime_capabilities_stay_disabled():
    assert not ENABLED_QUESTION_CAPABILITIES
    for capability, prerequisites in QUESTION_CAPABILITY_DEPENDENCIES.items():
        assert prerequisites
        with pytest.raises(DerivedError, match="question_capability_disabled"):
            require_question_capability(capability)
    with pytest.raises(DerivedError, match="unsupported_question_capability"):
        require_question_capability("semantic-similar-answer/1")
    with pytest.raises(TypeError):
        QUESTION_CAPABILITY_DEPENDENCIES["unreviewed/1"] = ()


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema",), 1),
        (("query_generation",), True),
        (("candidates_complete",), "true"),
        (("publication_closed",), 0),
        (("frontier", "positions", "facts"), True),
    ],
)
def test_wire_primitives_do_not_coerce_types(path, value):
    wire = coverage().payload()
    node = wire
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(DerivedError):
        QueryCoverage.from_payload(wire)


def test_number_parameters_normalize_signed_zero_and_reject_oversized_integers():
    spec = definition(
        parameter_schema=object_schema(
            {"number": {"type": "number", "minimum": -10, "maximum": 10}}, ["number"]
        ),
        scope_bindings={},
    )
    ids = {instance(definition=spec, parameters={"number": value}).id for value in (0, 0.0, -0.0)}
    assert len(ids) == 1
    with pytest.raises(DerivedError):
        instance(definition=spec, parameters={"number": 10**10000})
    with pytest.raises(DerivedError):
        definition(
            parameter_schema=object_schema(
                {"number": {"type": "number", "minimum": 0, "maximum": 10**10000}}
            ),
            scope_bindings={},
        )


def test_wire_nested_values_are_json_types_without_python_coercion():
    wire = definition().payload()
    wire["parameter_schema"]["required"] = ("project",)
    with pytest.raises(DerivedError):
        QuestionDefinition.from_payload(wire)
    wire = instance().payload()
    wire["time"]["mode"] = TimeMode.CURRENT
    with pytest.raises(DerivedError):
        QuestionInstance.from_payload(wire)


def test_number_normalization_never_aliases_distinct_large_integers():
    spec = definition(
        parameter_schema=object_schema(
            {"number": {"type": "number", "minimum": -1e20, "maximum": 1e20}}, ["number"]
        ),
        scope_bindings={},
    )
    first = instance(definition=spec, parameters={"number": 9007199254740992})
    next_integer = instance(definition=spec, parameters={"number": 9007199254740993})
    same_float = instance(definition=spec, parameters={"number": 9007199254740992.0})
    assert first.id != next_integer.id
    assert first.parameters["number"] == 9007199254740992
    assert next_integer.parameters["number"] == 9007199254740993
    assert first.id == same_float.id


def test_string_length_applies_after_canonical_unicode_normalization():
    spec = definition(
        parameter_schema=object_schema({"label": {"type": "string", "maxLength": 1}}, ["label"]),
        scope_bindings={},
    )
    composed = instance(definition=spec, parameters={"label": "é"})
    decomposed = instance(definition=spec, parameters={"label": "e\u0301"})
    assert composed.id == decomposed.id
    with pytest.raises(DerivedError):
        instance(definition=spec, parameters={"label": "éx"})
