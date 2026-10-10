import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as base

from agent_memory.consolidation.admission import authority_to_payload
from agent_memory.consolidation.business_policy import BusinessAdmissionPolicy, MemoryRule
from agent_memory.domain import PredicateSpec, ScopeLevel, SourceAuthority
from agent_memory.operations.domain_verification import VerificationToolSpec
from agent_memory.retrieval.model_contracts import ModelError

store = base.store


def policy(**options):
    return BusinessAdmissionPolicy(
        (PredicateSpec("city"),),
        (MemoryRule("city", verification="domain", verification_sources=("registry",), **options),),
        revision="business/1",
        source_families=(("user:alice", "user:alice"), ("registry", "city-registry")),
    )


def test_source_fidelity_and_model_confidence_do_not_replace_domain_verification():
    decision = policy().assess(
        base.source(base.MemoryScope("business", user_id="alice", session_id="session")),
        base.atom(),
        base.SELF,
    )
    assert decision.action == "PENDING_VERIFICATION"
    assert "business_requires_domain_verification" in decision.reasons
    assert decision.verification == "domain"


def test_storage_scope_and_fact_verification_are_independent_decisions():
    event, draft, authority = (
        base.source(base.MemoryScope("business", user_id="alice", session_id="session")),
        base.atom(),
        base.SELF,
    )
    assert policy(storage="l0_only").evaluate(event, draft, authority)[0] == "L0_ONLY"
    assert (
        policy(allowed_scopes=(ScopeLevel.USER,)).evaluate(event, draft, authority)[0] == "L0_ONLY"
    )
    personal = BusinessAdmissionPolicy(
        (PredicateSpec("city"),),
        (MemoryRule("city", verification="source"),),
        revision="personal/1",
    )
    assert personal.evaluate(event, draft, authority)[0] == "ACCEPT"


def test_policy_identity_changes_with_storage_verifier_and_source_family_contracts():
    original = policy()
    assert original.fingerprint != policy(storage="l0_only").fingerprint
    with pytest.raises(ValueError):
        MemoryRule("city", verification="domain")
    with pytest.raises(ValueError):
        BusinessAdmissionPolicy((PredicateSpec("city"),), (), revision="missing")


def test_authenticated_verifier_resolves_durably_but_original_and_copied_sources_cannot(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            rules = policy()
            receipt = await engine.admit(
                base.source(scope), (base.atom(),), authority=base.SELF, policy=rules
            )
            identity = receipt.candidate_ids[0]
            row = await engine.repository.admission_record(scope, identity)
            verifier = SourceAuthority("registry", "tool_observation", ("alice",), ("city",))
            await engine.resolve(
                scope,
                identity,
                event=base.source(scope, identity="witness"),
                authority=verifier,
                policy=rules,
                expected_version=row["version"],
                accept=True,
                source_quote="Alice lives in Hangzhou",
                support_from=base.at(1),
            )
            updated = await engine.repository.admission_record(scope, identity)
            assert updated["payload"]["action"] == "ACCEPT"
            assert updated["payload"]["decisions"][-1]["policy"]["revision"] == "business/1"
            checks = rules.evaluate_verification(
                base.source(scope),
                base.atom(),
                verifier,
                original_authority=SourceAuthority(
                    "registry", "tool_observation", ("alice",), ("city",)
                ),
            )
            assert checks[0] != "ACCEPT"
            copied = BusinessAdmissionPolicy(
                (PredicateSpec("city"),),
                (MemoryRule("city", verification="domain", verification_sources=("registry",)),),
                revision="copied/1",
                source_families=(("user:alice", "same"), ("registry", "same")),
            )
            assert (
                copied.evaluate_verification(
                    base.source(scope),
                    base.atom(),
                    verifier,
                    original_authority=base.SELF,
                )[0]
                != "ACCEPT"
            )

    asyncio.run(run())


def test_domain_authority_must_be_registered_and_claim_is_not_accepted_on_failed_resolution(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            rules = policy()
            receipt = await engine.admit(
                base.source(scope), (base.atom(),), authority=base.SELF, policy=rules
            )
            identity = receipt.candidate_ids[0]
            with pytest.raises(ValueError, match="business_verifier_not_registered"):
                await engine.resolve(
                    scope,
                    identity,
                    event=base.source(scope, identity="wrong-witness"),
                    authority=SourceAuthority("random", "tool_observation", ("alice",), ("city",)),
                    policy=rules,
                    expected_version=1,
                    accept=True,
                    source_quote="Alice lives in Hangzhou",
                    support_from=base.at(1),
                )
            row = await engine.repository.admission_record(scope, identity)
            assert row["payload"]["action"] == "PENDING_VERIFICATION" and row["version"] == 1

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode", ["same_source", "same_family", "unregistered", "l0_only", "changed"]
)
def test_native_domain_publication_policy_is_atomic_and_independent(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            rules = policy(storage="l0_only") if mode == "l0_only" else policy()
            if mode == "same_family":
                rules.source_families["registry"] = "user:alice"
            receipt = await engine.admit(
                base.source(scope), (base.atom(),), authority=base.SELF, policy=rules
            )
            row = await engine.repository.admission_record(scope, receipt.candidate_ids[0])
            if mode == "same_source":
                row["payload"]["authority"] = authority_to_payload(
                    SourceAuthority("registry", "tool_observation", ("alice",), ("city",))
                )
            identity = "wrong" if mode == "unregistered" else "registry"
            spec = VerificationToolSpec(
                identity,
                "1",
                SourceAuthority(identity, "tool_observation", ("alice",), ("city",)),
                ("alice",),
                ("city",),
            )
            calls = []

            async def native(uow, *_):
                calls.append(True)
                await uow.derived_put(scope, "test-marker", "fact", {"would_publish": True})
                rules.revision = "changed-during-await"

            guarded = rules.guard_domain_publisher(native)
            with pytest.raises(ModelError):
                async with engine.repository.unit_of_work() as uow:
                    await guarded(uow, row, object(), spec)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "test-marker", "fact") is None
            assert calls == ([True] if mode == "changed" else [])

    asyncio.run(run())


def test_non_asserted_and_unregistered_facts_do_not_gain_authority_from_business_rules():
    event = base.source(base.MemoryScope("business", user_id="alice", session_id="session"))
    rules = policy()
    assert (
        rules.evaluate(event, replace(base.atom(), modality="planned"), base.SELF)[0] == "L0_ONLY"
    )
    decision = rules.assess(event, replace(base.atom(), predicate="unregistered"), base.SELF)
    assert decision.action == "PENDING_VERIFICATION" and decision.storage == "unregistered"
    verifier = SourceAuthority("registry", "self_report", ("alice",), ("city",))
    action, reasons = rules.evaluate_verification(
        event, base.atom(), verifier, original_authority=base.SELF
    )
    assert (
        action == "PENDING_VERIFICATION" and "business_verifier_kind_not_authoritative" in reasons
    )
