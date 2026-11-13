"""Review gates cannot reopen terminal host admission decisions."""

import asyncio

import pytest
from test_atom_extraction import AUTHORITY, Generator, Reviewer, candidate

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline, _key, _ReviewedPolicy
from agent_memory.consolidation.business_policy import BusinessAdmissionPolicy, MemoryRule
from agent_memory.domain import AtomDraft, MemoryEvent, MemoryScope, PredicateSpec, SourceAuthority
from agent_memory.operations.domain_verification import DomainVerificationQueue, VerificationToolSpec
from agent_memory.retrieval.model_contracts import ModelError
from agent_memory.sqlite import SQLiteMemoryRepository


class HostDecisionPolicy(AdmissionPolicy):
    def __init__(self, action="REJECT"):
        super().__init__([PredicateSpec("home_city")])
        self.action = action

    def config_payload(self):
        return {**super().config_payload(), "host_action": self.action}

    def evaluate(self, event, draft, authority):
        return self.action, ("host_decision", "shared_reason")


@pytest.mark.parametrize("base", ["ACCEPT", "PENDING_VERIFICATION", "CONTESTED", "L0_ONLY", "REJECT"])
@pytest.mark.parametrize("gate", ["ACCEPT", "PENDING_VERIFICATION", "L0_ONLY", "REJECT"])
def test_terminal_host_decisions_survive_review_gate_composition(base, gate):
    draft = AtomDraft("alice", "home_city", "Shanghai", "city", "Shanghai")
    policy = _ReviewedPolicy(
        HostDecisionPolicy(base),
        {_key(draft): (gate, ("review_decision", "shared_reason"))},
        {},
    )
    if "REJECT" in (base, gate):
        expected = "REJECT"
    elif base == "L0_ONLY":
        expected = "L0_ONLY"
    else:
        # Do not invent an ordering between contested and pending decisions.
        expected = gate if gate != "ACCEPT" else base
    action, reasons = policy.evaluate(None, draft, None)
    assert action == expected
    assert set(reasons) == {"host_decision", "review_decision", "shared_reason"}
    assert reasons.count("shared_reason") == 1


def terminal_policy(kind):
    if kind == "host_reject":
        return HostDecisionPolicy(), "asserted", "REJECT", "host_decision"
    if kind == "business_l0":
        return BusinessAdmissionPolicy(
            [PredicateSpec("home_city")], [MemoryRule("home_city", storage="l0_only")],
            revision="storage-regression-v1",
        ), "asserted", "L0_ONLY", "business_storage_l0_only"
    return AdmissionPolicy([PredicateSpec("home_city")]), "planned", "L0_ONLY", "non_asserted_modality"


async def assert_not_verifiable(repository, source, record):
    class Tool:
        spec = VerificationToolSpec(
            "registry", "1", SourceAuthority("registry", "document", ("alice",), ("home_city",)),
            ("alice",), ("home_city",),
        )

        async def verify(self, *_):
            pytest.fail("a terminal candidate must not reach a verifier")

    async def authorize(*_):
        return True

    async def publisher(*_):
        pytest.fail("a terminal candidate must not reach a publisher")

    queue = DomainVerificationQueue(
        repository, source.scope, (Tool(),), authorize=authorize, publisher=publisher,
    )
    with pytest.raises(ModelError, match="verification_candidate_already_decided"):
        await queue.schedule(record["id"], record["version"], tool_id="registry", request_id="terminal")
    assert await queue.claim("worker") is None


@pytest.mark.parametrize("kind", ["planned", "business_l0", "host_reject"])
@pytest.mark.parametrize("review,retention", [
    ("supported", "durable"), ("uncertain", "uncertain"),
    ("supported", "transient"), ("unsupported", "durable"),
])
def test_sqlite_publication_preserves_terminal_policy_without_guard(tmp_path, kind, review, retention):
    async def run():
        repository = SQLiteMemoryRepository(tmp_path / "terminal.db")
        await repository.initialize()
        source = MemoryEvent(MemoryScope("terminal", user_id="alice", session_id="one"), "message", "Shanghai", actor="alice")
        policy, modality, expected, reason = terminal_policy(kind)
        raw = {**candidate(), "modality": modality, "source_quote": source.content}
        pipeline = AtomExtractionPipeline(Generator([raw]), Reviewer(review, retention))
        receipt = await pipeline.process(repository, source, authority=AUTHORITY, policy=policy)
        if review == "unsupported":
            expected = "REJECT"
        assert [decision.action for decision in receipt.decisions] == [expected]
        assert reason in receipt.decisions[0].reasons
        assert not receipt.admission.claim_ids and not receipt.admission.pending_ids
        record = await repository.admission_record(source.scope, receipt.admission.candidate_ids[0])
        assert record["payload"]["action"] == expected
        assert record["payload"]["decisions"][-1]["action"] == expected
        await assert_not_verifiable(repository, source, record)
        cached = await pipeline.process(repository, source, authority=AUTHORITY, policy=policy)
        assert cached.admission.duplicate and cached.decisions[0].action == expected
        assert not cached.admission.claim_ids and not cached.admission.pending_ids
    asyncio.run(run())
