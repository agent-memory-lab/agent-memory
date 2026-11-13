"""Synthetic guard contracts, not evidence of general extraction accuracy."""

import asyncio
from dataclasses import replace

import pytest
from test_atom_extraction import AUTHORITY, POLICY, Generator, Reviewer, candidate

from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.attribution import ConservativeAttributionGuard, attribution_risks
from agent_memory.domain import MemoryEvent, MemoryScope

SCOPE = MemoryScope("guard-tests", user_id="alice", session_id="one")


def event(text="I live in Shanghai.", **changes):
    return MemoryEvent(SCOPE, "message", text, actor="alice", **changes)


def pipeline(source, *, guard=True, subject="alice", review="supported", quote=None):
    raw = candidate()
    raw.update(subject_id=subject, source_quote=quote or source.content)
    return AtomExtractionPipeline(
        Generator([raw]), Reviewer(review),
        attribution_guard=ConservativeAttributionGuard(SCOPE, "alice") if guard else None,
    )


def prepare(source, **kwargs):
    return asyncio.run(pipeline(source, **kwargs).prepare(source, authority=AUTHORITY, policy=POLICY))


@pytest.mark.parametrize("text", [
    'Mira said, I live in Shanghai.',
    'Mira told me: I live in Shanghai.',
    'Mira wrote that I live in Shanghai.',
    'Mira replied: I live in Shanghai.',
    'Someone asks whether I live in Shanghai.',
    'Mira said, "I live in Shanghai."',
    '“I live in Shanghai.”',
    "'I live in Shanghai.'", '「我住在上海」',
    '她说我住在上海。', '她告訴他，我住在上海。',
    'I live in Shanghai. Someone quoted a different story later.',
    'I live in Shanghai. "An unclosed quote',
])
def test_risky_source_cannot_publish_even_if_reviewer_approves(text):
    prepared = prepare(event(text))
    report = prepared["audit"]["reports"][0]
    assert report["action"] == "PENDING_VERIFICATION"
    assert "source_attribution_uncertain" in report["reasons"]
    assert prepared["audit"]["attribution_guard"]["primary_subject_id"] == "alice"


@pytest.mark.parametrize("text", ["I live in Shanghai.", "我住在上海。", "I'm in Shanghai."])
def test_guard_does_not_change_unflagged_reviewed_candidate(text):
    assert prepare(event(text))["audit"]["reports"][0]["action"] == "ACCEPT"
    assert not attribution_risks(text)


def test_whole_source_risk_cannot_be_hidden_with_smaller_quote():
    source = event('Mira said: I live in Shanghai.')
    assert prepare(source, quote="live in Shanghai")["audit"]["reports"][0]["action"] == "PENDING_VERIFICATION"


def test_default_remains_compatible_and_opt_in_fingerprint_differs():
    source = event('Mira said, I live in Shanghai.')
    original, guarded = prepare(source, guard=False), prepare(source)
    assert original["audit"]["reports"][0]["action"] == "ACCEPT"
    assert "attribution_guard" not in original["audit"]
    assert original["audit"]["input_fingerprint"] != guarded["audit"]["input_fingerprint"]
    assert "attribution_guard" not in pipeline(source, guard=False).config_payload()


@pytest.mark.parametrize("changes", [
    {"actor": "bob"}, {"actor": "Alice"},
    {"scope": replace(SCOPE, user_id="bob")},
    {"scope": replace(SCOPE, tenant_id="elsewhere")},
    {"scope": replace(SCOPE, namespace="other")},
    {"scope": replace(SCOPE, session_id="two")},
])
def test_actor_and_scope_are_exact_host_bindings_before_generation(changes):
    source = replace(event(), **changes)
    p = pipeline(source)
    with pytest.raises(ValueError, match="scope or source actor"):
        asyncio.run(p.prepare(source, authority=AUTHORITY, policy=POLICY))
    assert p.generator.calls == 0


def test_other_subject_is_unknown_not_an_alias_for_actor():
    report = prepare(event(), subject="person:alice")["audit"]["reports"][0]
    assert report["action"] == "PENDING_VERIFICATION"
    assert "subject_identity_not_bound_to_source_actor" in report["reasons"]


@pytest.mark.parametrize("review", ["unsupported", "uncertain"])
def test_guard_never_overrules_a_negative_or_unknown_review(review):
    report = prepare(event('"I live in Shanghai."'), review=review)["audit"]["reports"][0]
    assert report["action"] == ("REJECT" if review == "unsupported" else "PENDING_VERIFICATION")


def test_policy_removal_or_source_revision_invalidates_prepared_result():
    async def run():
        source = event()
        p = pipeline(source)
        prepared = await p.prepare(source, authority=AUTHORITY, policy=POLICY)
        p.attribution_guard = None
        with pytest.raises(ValueError, match="input changed"):
            await p.validate_prepared_source(source, prepared, authority=AUTHORITY, policy=POLICY)
        p.attribution_guard = ConservativeAttributionGuard(SCOPE, "alice")
        with pytest.raises(ValueError, match="input changed"):
            await p.validate_prepared_source(
                replace(source, content='She said, I live in Shanghai.'), prepared,
                authority=AUTHORITY, policy=POLICY,
            )
    asyncio.run(run())


def test_risk_detector_is_explicitly_incomplete_not_a_direct_speech_classifier():
    # No claim to detect every unpunctuated/colloquial quotation or every language.
    assert not attribution_risks("She was like I live in Shanghai")
    assert not attribution_risks("Elle a déclaré que je vis à Shanghai")


@pytest.mark.parametrize("value", [None, 3, "x" * 32001])
def test_risk_detector_input_is_bounded(value):
    with pytest.raises(ValueError):
        attribution_risks(value)


def test_guard_is_snapshotted_before_awaited_untrusted_review():
    async def run():
        source = event('Someone said I live in Shanghai.')
        p = pipeline(source)

        class MutatingReviewer(Reviewer):
            async def review_atoms(self, source, candidates):
                p.attribution_guard = None
                return await super().review_atoms(source, candidates)

        p.reviewer = MutatingReviewer()
        prepared = await p.prepare(source, authority=AUTHORITY, policy=POLICY)
        assert prepared["audit"]["reports"][0]["action"] == "PENDING_VERIFICATION"
        with pytest.raises(ValueError, match="input changed"):
            await p.validate_prepared_source(source, prepared, authority=AUTHORITY, policy=POLICY)
    asyncio.run(run())


def test_native_sqlite_publication_keeps_flagged_candidate_pending(tmp_path):
    from agent_memory.sqlite import SQLiteMemoryRepository

    async def run():
        repository = SQLiteMemoryRepository(tmp_path / "guard.db")
        await repository.initialize()
        source = event('Someone said I live in Shanghai.')
        p = pipeline(source)
        receipt = await p.process(repository, source, authority=AUTHORITY, policy=POLICY)
        assert [d.action for d in receipt.decisions] == ["PENDING_VERIFICATION"]
        assert not receipt.admission.claim_ids
        record = await repository.admission_record(
            source.scope, receipt.admission.candidate_ids[0]
        )
        assert record["payload"]["decisions"][0]["policy"]["attribution_guard"]["primary_subject_id"] == "alice"
        cached = await p.process(repository, source, authority=AUTHORITY, policy=POLICY)
        assert cached.admission.duplicate and not cached.admission.claim_ids
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["planned", "business_l0", "host_reject"])
@pytest.mark.parametrize("guard", [False, True])
def test_sqlite_attribution_hold_cannot_reopen_terminal_host_policy(tmp_path, kind, guard):
    from test_reviewed_admission_policy import assert_not_verifiable, terminal_policy

    from agent_memory.sqlite import SQLiteMemoryRepository

    async def run():
        repository = SQLiteMemoryRepository(tmp_path / "guard-terminal.db")
        await repository.initialize()
        source = event("Someone said I live in Shanghai.")
        policy, modality, expected, reason = terminal_policy(kind)
        raw = {**candidate(), "modality": modality, "source_quote": source.content}
        p = AtomExtractionPipeline(
            Generator([raw]), Reviewer(),
            attribution_guard=ConservativeAttributionGuard(SCOPE, "alice") if guard else None,
        )
        receipt = await p.process(repository, source, authority=AUTHORITY, policy=policy)
        assert [decision.action for decision in receipt.decisions] == [expected]
        assert reason in receipt.decisions[0].reasons
        assert ("source_attribution_uncertain" in receipt.decisions[0].reasons) is guard
        assert not receipt.admission.claim_ids and not receipt.admission.pending_ids
        record = await repository.admission_record(source.scope, receipt.admission.candidate_ids[0])
        assert record["payload"]["action"] == expected
        assert record["payload"]["decisions"][-1]["action"] == expected
        await assert_not_verifiable(repository, source, record)
        cached = await p.process(repository, source, authority=AUTHORITY, policy=policy)
        assert cached.admission.duplicate and cached.decisions[0].action == expected
        assert not cached.admission.claim_ids and not cached.admission.pending_ids
    asyncio.run(run())
