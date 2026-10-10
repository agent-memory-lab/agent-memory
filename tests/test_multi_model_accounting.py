from dataclasses import replace

import pytest
from test_question_cost_v7 import ALL_PHASES, OTHER, H, entry, run

from agent_memory.evaluation.question_cost import CostPhase, CostSnapshot, ModelEvidence
from agent_memory.serialization import to_jsonable


def test_generation_review_and_audit_keep_individual_configuration_and_cost_identity():
    models = ModelEvidence.group((H, OTHER), "real")
    calls = (
        entry("extract", CostPhase.WRITE, model_role="semantic_model"),
        entry(
            "review", CostPhase.WRITE, model_role="semantic_model", model_configuration_sha256=OTHER
        ),
    )
    report = run(
        semantic_model=models,
        generation_model=ModelEvidence(H, "not_used"),
        costs=CostSnapshot("USD", calls, (), ALL_PHASES),
    )
    assert report.costs.entries == calls
    assert len({call.model_configuration_sha256 for call in report.costs.entries}) == 2
    assert models.configuration_sha256 not in {H, OTHER}
    assert models == ModelEvidence.group((OTHER, H), "real")


def test_undeclared_configuration_and_unexecuted_group_do_not_become_verified_evidence():
    models = ModelEvidence.group((H, OTHER), "real")
    with pytest.raises(ValueError, match="declared role"):
        run(
            semantic_model=models,
            costs=CostSnapshot(
                "USD",
                (entry(model_role="semantic_model", model_configuration_sha256="c" * 64),),
                (),
                ALL_PHASES,
            ),
        )
    with pytest.raises(ValueError):
        ModelEvidence.group((H, OTHER), "not_used")
    with pytest.raises(ValueError):
        replace(models, configuration_sha256=H)
    assert ModelEvidence(H, "real").execution_configurations == ()


def test_legacy_single_model_wire_shape_stays_unchanged():
    assert to_jsonable(ModelEvidence(H, "real")) == {"configuration_sha256": H, "execution": "real"}
    assert to_jsonable(ModelEvidence.group((H, OTHER), "real"))["execution_configurations"] == [
        H,
        OTHER,
    ]
