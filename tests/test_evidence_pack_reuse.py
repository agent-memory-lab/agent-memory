"""Exact registered pack reuse; real backend proof, no new retained-body cache."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.retrieval.evidence_packs import (
    EvidencePackConfiguration,
    QuestionEvidencePacks,
)
from agent_memory.retrieval.feature_gate import RetrievalFeatureApproval

store = project.store


async def fixture(engine, scope, clock, *, populate=True):
    svc = runtime(engine, scope, clock)
    if populate:
        item = await project.stage(svc.admission, scope)
        await project.qualify(svc.admission, *item)
    await register(svc)
    await fresh(svc, clock)
    return svc, QuestionEvidencePacks(svc, enabled=True, contract_test_only=True)


async def read(packs, **kwargs):
    return await packs.read("project-a:owner", actor=ACTOR, purpose="project_questions", **kwargs)


def test_pack_reuses_exact_materialization_without_mutable_cache(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, packs = await fixture(engine, scope, clock)
            first, second = await read(packs), await read(packs)
            assert first == second and first.qualification == "contract-test-only"
            assert first.model_calls == 0
            result = json.loads(first.result_json)
            assert result["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            assert result["citations"] and result["processing_references"]
            assert result["result"]["world_negative"] is False
            manifest = json.loads(first.proof_manifest_json)
            assert manifest["original_generation_manifest"] == result["generation_manifest"]
            assert manifest["header"]["proof"] and manifest["source_ids"] == ["source"]
            result["result"].clear()
            assert json.loads((await read(packs)).result_json)["result"]
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "question_content")) == 1
                assert len(await uow.derived_records(scope, "question_certificate")) == 1
                assert not await uow.derived_records(scope, "evidence_pack_cache")
            changed = QuestionEvidencePacks(
                svc,
                enabled=True,
                contract_test_only=True,
                configuration=EvidencePackConfiguration(max_output_bytes=524_288),
            )
            assert (await read(changed)).key != first.key

    asyncio.run(run())


def test_disabled_and_unqualified_do_not_load_data(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await fixture(engine, scope, clock)

            async def forbidden(*args, **kwargs):
                pytest.fail("disabled adapter read storage")

            monkeypatch.setattr(svc, "model_input_header", forbidden)
            with pytest.raises(DerivedError, match="disabled"):
                await read(QuestionEvidencePacks(svc))
            with pytest.raises(ValueError, match="qualified"):
                QuestionEvidencePacks(svc, enabled=True)

    asyncio.run(run())


@pytest.mark.parametrize("change", ["purpose", "audience", "actor", "valid_at", "known_at"])
def test_identity_and_temporal_boundaries_precede_body(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, packs = await fixture(engine, scope, clock)

            async def forbidden(*args, **kwargs):
                pytest.fail("unauthorized body read")

            monkeypatch.setattr(svc, "_read_in_uow", forbidden)
            kwargs = dict(actor=ACTOR, purpose="project_questions")
            kwargs[change] = clock[0] if change.endswith("_at") else "other"
            with pytest.raises(DerivedError):
                await packs.read("project-a:owner", **kwargs)

    asyncio.run(run())


@pytest.mark.parametrize("change", ["new_source", "grant", "context", "membership", "erase"])
def test_every_reuse_revalidates_complete_frontier_and_rights(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, packs = await fixture(engine, scope, clock)
            await read(packs)
            if change == "new_source":
                await project.stage(svc.admission, scope, identity="late", value="Bob")
            elif change == "grant":
                await svc.grant(
                    ProcessingGrant("source", (ACTOR,), ("project_questions",), revoked=True),
                    expected_version=1,
                )
            elif change == "context":
                svc.context = replace(svc.context, revision="context/2")
            elif change == "membership":
                svc.admission.memberships["a"] = replace(
                    project.MEMBERSHIPS[0], registry_revision="registry/2"
                )
            else:
                await erase(kernel, scope, "source")
            with pytest.raises(DerivedError):
                await read(packs)

    asyncio.run(run())


def test_empty_pack_cannot_hide_new_matching_source(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, packs = await fixture(engine, scope, clock, populate=False)
            first = await read(packs)
            assert json.loads(first.result_json)["answer_status"] == "unknown"
            await project.stage(svc.admission, scope)
            with pytest.raises(DerivedError):
                await read(packs)

    asyncio.run(run())


def test_final_storage_await_cannot_cross_expiry(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, packs = await fixture(engine, scope, clock)
            original = type(engine.repository.unit_of_work()).derived_get
            fired = False

            async def crossing(self, scope, kind, identity):
                nonlocal fired
                result = await original(self, scope, kind, identity)
                if kind == "question_content" and not fired:
                    fired = True
                    clock[0] += timedelta(days=11)
                return result

            monkeypatch.setattr(type(engine.repository.unit_of_work()), "derived_get", crossing)
            with pytest.raises(DerivedError):
                await read(packs)
            assert fired

    asyncio.run(run())


def test_old_backup_reuse_denied_after_authoritative_erase(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, packs = await fixture(engine, scope, clock)
            await read(packs)
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, "source")
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                restored = runtime(AdmissionEngine(backup), scope, clock)
                restored_packs = QuestionEvidencePacks(
                    restored, enabled=True, contract_test_only=True
                )
                for adapter in (packs, restored_packs):
                    with pytest.raises(DerivedError):
                        await read(adapter)

    asyncio.run(run())


def test_verified_promotion_can_be_rolled_back_between_reads(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await fixture(engine, scope, clock)
            config = EvidencePackConfiguration()
            approval = RetrievalFeatureApproval(
                "evidence-pack-reuse", config.fingerprint, "a" * 64, "b" * 64, "host-rollback"
            )
            allowed = [True]
            # Contract test exercises the trusted verifier seam; these literal
            # digests are NOT real efficacy evidence or production approval.
            packs = QuestionEvidencePacks(
                svc,
                enabled=True,
                configuration=config,
                approval=approval,
                verify_approval=lambda receipt: allowed[0],
            )
            assert (await read(packs)).qualification == "controlled-real"
            allowed[0] = False
            with pytest.raises(PermissionError):
                await read(packs)

    asyncio.run(run())


def test_capacity_never_truncates_original_support(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await fixture(engine, scope, clock)
            packs = QuestionEvidencePacks(
                svc,
                enabled=True,
                contract_test_only=True,
                configuration=EvidencePackConfiguration(max_output_bytes=1),
            )
            with pytest.raises(DerivedError, match="output_capacity"):
                await read(packs)

    asyncio.run(run())


def test_promotion_rollback_during_storage_await_blocks_delivery(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await fixture(engine, scope, clock)
            config = EvidencePackConfiguration()
            approval = RetrievalFeatureApproval(
                "evidence-pack-reuse", config.fingerprint, "a" * 64, "b" * 64, "host-rollback"
            )
            allowed = [True]
            packs = QuestionEvidencePacks(
                svc,
                enabled=True,
                configuration=config,
                approval=approval,
                verify_approval=lambda receipt: allowed[0],
            )
            original = svc._read_in_uow

            async def revoke(*args, **kwargs):
                result = await original(*args, **kwargs)
                allowed[0] = False
                return result

            monkeypatch.setattr(svc, "_read_in_uow", revoke)
            with pytest.raises(PermissionError):
                await read(packs)

    asyncio.run(run())


def test_replaced_configuration_cannot_reuse_another_approval(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await fixture(engine, scope, clock)
            config = EvidencePackConfiguration()
            approval = RetrievalFeatureApproval(
                "evidence-pack-reuse", config.fingerprint, "a" * 64, "b" * 64, "host-rollback"
            )
            packs = QuestionEvidencePacks(
                svc,
                enabled=True,
                configuration=config,
                approval=approval,
                verify_approval=lambda receipt: True,
            )
            packs.configuration = replace(config, max_output_bytes=524_288)

            async def forbidden(*args, **kwargs):
                pytest.fail("changed configuration must fail before body access")

            monkeypatch.setattr(svc, "_read_in_uow", forbidden)
            with pytest.raises(DerivedError, match="configuration_changed"):
                await read(packs)

    asyncio.run(run())


@pytest.mark.parametrize(
    "change",
    ["expiry", "clock_rollback", "context", "configuration", "inplace_config", "approval_binding"],
)
def test_last_promotion_callback_cannot_cross_delivery_boundaries(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await fixture(engine, scope, clock)
            config = EvidencePackConfiguration()
            approval = RetrievalFeatureApproval(
                "evidence-pack-reuse", config.fingerprint, "a" * 64, "b" * 64, "host-rollback"
            )
            calls = []

            def verify(receipt):
                calls.append(receipt)
                if len(calls) == 3:
                    if change == "expiry":
                        clock[0] += timedelta(days=11)
                    elif change == "clock_rollback":
                        clock[0] -= timedelta(seconds=1)
                    elif change == "context":
                        svc.context = replace(svc.context, revision="context/changed")
                    elif change == "configuration":
                        packs.configuration = replace(config, max_output_bytes=524_288)
                    elif change == "inplace_config":
                        object.__setattr__(config, "max_output_bytes", 524_288)
                    else:
                        object.__setattr__(approval, "configuration_sha256", "c" * 64)
                return True

            packs = QuestionEvidencePacks(
                svc, enabled=True, configuration=config, approval=approval, verify_approval=verify
            )
            with pytest.raises(DerivedError):
                await read(packs)
            assert len(calls) == 3

    asyncio.run(run())


def test_payload_fingerprinting_precedes_final_expiry_guard(store, monkeypatch):
    from agent_memory.retrieval import evidence_packs

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, packs = await fixture(engine, scope, clock)
            original = evidence_packs.digest

            def slow_manifest(value):
                result = original(value)
                if (
                    isinstance(value, dict)
                    and value.get("schema") == "question-evidence-pack-proof/1"
                ):
                    clock[0] += timedelta(days=11)
                return result

            monkeypatch.setattr(evidence_packs, "digest", slow_manifest)
            with pytest.raises(DerivedError):
                await read(packs)

    asyncio.run(run())
