"""A real independently connected budget process, killed by the parent test."""

import asyncio
import json
import sys

from agent_memory.operations.model_budget import ModelBudget


async def main():
    config = json.loads(sys.argv[1])
    if config["backend"] == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        repository = PostgresMemoryRepository.from_dsn(config["database"], max_size=2)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        repository = SQLiteMemoryRepository(config["database"])
    await repository.initialize()
    if config.get("phase") == "governed_http":
        from datetime import datetime

        import test_atom_admission as base
        from test_governed_models_v7 import TEMPLATE

        from agent_memory.derived import ObservationService
        from agent_memory.domain import MemoryScope
        from agent_memory.retrieval.model_answers import GovernedModelAnswers
        from agent_memory.retrieval.model_authority import SourceModelAuthority
        from agent_memory.retrieval.model_contracts import ModelConfiguration, ModelCoordinates
        from agent_memory.retrieval.ollama import OllamaPort

        cfg = ModelConfiguration(**config["configuration"])
        service = ObservationService(
            repository,
            MemoryScope(**config["scope"]),
            base.POLICY,
            clock=lambda: datetime.fromisoformat(config["clock"]),
            authority_id="local-host",
            authority_min_version=1,
        )

        async def proof(uow, coordinates):
            return True

        authority = SourceModelAuthority(
            service, configuration=cfg, public_template=TEMPLATE, verify_coordinates=proof
        )
        sealed = await authority.prepare(
            ModelCoordinates(**config["coordinates"]), config["sources"]
        )
        answers = GovernedModelAnswers(
            authority,
            OllamaPort(cfg),
            account_keys=config["accounts"],
            validate_output=lambda value, _: value == "zh-CN",
        )
        await answers.answer(sealed)
        return
    budget = ModelBudget(repository)
    row = await budget.reserve(
        operation_id="crash",
        attempt_id="one",
        request_sha256="a" * 64,
        account_keys=config["accounts"],
        maximum_microunits=100,
        upper_bound_evidence="synthetic-bound",
    )
    if config["phase"] == "intent_before_commit":
        async with repository.unit_of_work() as uow:
            await budget.intent(uow, row["key"], row["request_sha256"])
            print("ready", flush=True)
            await asyncio.Event().wait()
    async with repository.unit_of_work() as uow:
        await budget.intent(uow, row["key"], row["request_sha256"])
    if config["phase"] == "settled":
        await budget.settle(
            row["key"], actual_microunits=70, receipt_id="bill", provider_request_id="provider-call"
        )
    # intent_after_commit represents the provider acceptance/local receipt gap.
    # No simulated bill is fabricated for that unresolved attempt.
    print("ready", flush=True)
    await asyncio.Event().wait()


asyncio.run(main())
