"""Run governed local model extraction/review over an authored disposable fixture.

python examples/model_memory_host.py --endpoint http://127.0.0.1:11434
    --model qwen3.5:9b --allow-real-model --output /tmp/model-host.json
This measures a synthetic integration, not real business acceptance. No model pull,
cloud fallback, inferred authority or automatic production grant is performed.
"""

import argparse
import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.model_extraction import (
    GENERATOR_PROMPT,
    GENERATOR_SCHEMA,
    REVIEWER_PROMPT,
    REVIEWER_SCHEMA,
    GovernedSourceCalls,
    ModelAtomGenerator,
    ModelAtomReviewer,
)
from agent_memory.derived import HostGrantAuthority, ObservationService, ProcessingGrant
from agent_memory.domain import MemoryEvent, MemoryScope, PredicateSpec, SourceAuthority, utc_now
from agent_memory.evaluation.ollama_smoke import freeze_plan
from agent_memory.operations.memory_host import MemoryHost
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_contracts import ModelConfiguration, canonical, digest
from agent_memory.retrieval.ollama import OllamaPort
from agent_memory.sqlite import SQLiteMemoryRepository


async def main(args):
    if not args.allow_real_model:
        raise ValueError("explicit real model opt-in required")
    frozen = await freeze_plan(args.endpoint, args.model, allow_real_model=True, timeout_seconds=90)
    base = ModelConfiguration(**frozen["model"])
    plan = dict(
        schema="model-extraction-smoke/1",
        evidence_class="authored_synthetic",
        model_revision=base.model_revision,
        runtime_manifest_sha256=base.runtime_manifest_sha256,
        generator_prompt_sha256=digest(GENERATOR_PROMPT),
        reviewer_prompt_sha256=digest(REVIEWER_PROMPT),
        real_business_acceptance=False,
        exact_ollama_token_budget=False,
        costs="unknown",
    )
    with TemporaryDirectory(prefix="governed-model-host-") as folder:
        repository = SQLiteMemoryRepository(Path(folder) / "memory.db")
        await repository.initialize()
        scope = MemoryScope("authored-model-demo", user_id="alice", session_id="demo")
        policy = AdmissionPolicy([PredicateSpec("locale"), PredicateSpec("response_language")])
        service = ObservationService(
            repository, scope, policy, authority_id="demo-host", authority_min_version=0
        )
        await service.set_authority(
            HostGrantAuthority("demo-host", ("alice",), utc_now() + timedelta(hours=1))
        )
        budget = ModelBudget(repository)
        accounts = await budget.configure(
            (BudgetAccount("extraction-demo", "run", "USD", "1", None),)
        )

        async def guard(uow, coords):
            return coords.principal == "alice" and coords.purpose == "agent_context"

        calls = []
        for prompt, schema, role in zip(
            (GENERATOR_PROMPT, REVIEWER_PROMPT),
            (GENERATOR_SCHEMA, REVIEWER_SCHEMA),
            ("atom_generation", "atom_review"),
            strict=True,
        ):
            cfg = replace(
                base,
                template_sha256=digest(prompt),
                output_schema_json=canonical(schema),
                prompt_revision="model-extraction/1",
                output_revision="model-extraction/1",
                options_json=canonical(
                    {"num_ctx": 32768, "num_predict": 768, "temperature": 0, "seed": 917}
                ),
                keep_alive="5m",
            )
            calls.append(
                GovernedSourceCalls(
                    service,
                    OllamaPort(cfg),
                    public_template=prompt,
                    account_keys=accounts,
                    principal="alice",
                    project="demo",
                    purpose="agent_context",
                    host_guard=guard,
                    role=role,
                )
            )
        pipeline = AtomExtractionPipeline(
            ModelAtomGenerator(
                calls[0],
                subjects=("alice",),
                predicates=("response_language",),
                primary_subject_id="alice",
            ),
            ModelAtomReviewer(calls[1]),
            timeout_seconds=30,
        )

        async def accept(uow, event):
            await service.grant(ProcessingGrant(event.id, ("alice",)), _unit_of_work=uow)
            await (
                calls[0]
                .authority({}, utc_now())
                .allow_processing(
                    event.id,
                    readers=("alice",),
                    purposes=("agent_context",),
                    expires_at=utc_now() + timedelta(hours=1),
                    _unit_of_work=uow,
                )
            )
            return True

        host = MemoryHost(
            repository,
            scope,
            pipeline,
            policy,
            SourceAuthority(
                "authenticated-alice",
                subjects=("alice",),
                predicates=("response_language",),
            ),
            on_accept=accept,
        )
        event = MemoryEvent(
            scope,
            "message",
            "For future conversations, I prefer responses in Simplified Chinese.",
            actor="alice",
        )
        await host.submit(event, request_id="authored-one", producer_id="demo")
        preview = await host.preview(event.id)
        cycle = await host.run_once()
        candidates = await repository.admission_records(scope)
        if cycle["extraction"]["completed"] != 1 or not candidates:
            Path(args.output).write_text(
                json.dumps(
                    dict(
                        plan=plan,
                        cycle=cycle,
                        preview_audit=preview["audit"],
                        metrics=await host.metrics(),
                    ),
                    indent=2,
                )
                + "\n"
            )
            raise AssertionError("governed model extraction did not complete a candidate")
        plan.update(
            plan_sha256=digest(plan),
            preview_audit=preview["audit"],
            cycle=cycle,
            metrics=await host.metrics(),
            candidate_dispositions=[c["payload"]["action"] for c in candidates],
            candidate_reasons=[c["payload"]["reasons"] for c in candidates],
            model_calls=await budget.snapshot(),
            audit=[c["payload"].get("extraction") for c in candidates],
        )
        host.stop()
        Path(args.output).write_text(json.dumps(plan, indent=2) + "\n")
        print(
            json.dumps(
                dict(
                    status="passed",
                    cycle=cycle,
                    candidate_dispositions=plan["candidate_dispositions"],
                    evidence_class=plan["evidence_class"],
                )
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--allow-real-model", action="store_true")
    parser.add_argument("--output", required=True)
    asyncio.run(main(parser.parse_args()))
