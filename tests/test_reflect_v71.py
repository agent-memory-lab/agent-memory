import asyncio
import json
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_durable_purge import source_id
from test_governed_models_v7 import configuration

from agent_memory.consolidation.model_extraction import GovernedSourceCalls
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_contracts import ModelError, ModelResponse, canonical, digest
from agent_memory.retrieval.reflect import REFLECT_PROMPT, REFLECT_REVIEW_PROMPT, ReadOnlyReflect

store = base.store


class Port:
    def __init__(self, prompt, bad=False):
        self.configuration = configuration(template_sha256=digest(prompt))
        self.bad, self.calls = bad, 0

    async def generate(self, sealed):
        self.calls += 1
        messages = json.loads(sealed.payload_json)["messages"]
        request = json.loads(messages[-1]["content"])
        if request["operation"] == "reflect":
            source = json.loads(messages[1]["content"])
            result = dict(
                answer="A tentative explanation",
                citations=[
                    dict(
                        source_id=source["source_id"],
                        quote="missing" if self.bad else source["content"],
                    )
                ],
                uncertainties=["Not independently verified"],
            )
        else:
            result = dict(supported=True, reason_codes=["supported"])
        return ModelResponse(canonical(result), 20, 10, 1000)


@pytest.mark.parametrize("bad", [False, True])
def test_reflect_is_bounded_governed_and_never_admits_memory(store, bad):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, *_ = await configured(engine, kernel, scope, clock)
            ledger = ModelBudget(engine.repository)
            accounts = await ledger.configure((BudgetAccount("reflect", "run", "USD", "1", None),))
            ports = [Port(REFLECT_PROMPT, bad), Port(REFLECT_REVIEW_PROMPT)]

            async def guard(uow, coordinates):
                return coordinates.principal == "alice"

            calls = [
                GovernedSourceCalls(
                    svc,
                    p,
                    public_template=prompt,
                    account_keys=accounts,
                    principal="alice",
                    project="a",
                    purpose="agent_context",
                    host_guard=guard,
                    role=role,
                    cost_phase="foreground",
                )
                for p, prompt, role in zip(
                    ports,
                    (REFLECT_PROMPT, REFLECT_REVIEW_PROMPT),
                    ("reflect", "reflect-review"),
                    strict=True,
                )
            ]
            async with engine.repository.unit_of_work() as uow:
                event = await uow.get_source_event(scope, source_id(scope, "1"))
            await (
                calls[0]
                .authority({}, clock[0])
                .allow_processing(
                    event.id,
                    readers=("alice",),
                    purposes=("agent_context",),
                    expires_at=clock[0] + timedelta(hours=1),
                )
            )
            before = await engine.repository.admission_records(scope)
            reflect = ReadOnlyReflect(*calls)
            if bad:
                with pytest.raises(ModelError, match="invalid_reflect_citation"):
                    await reflect.answer("Explain this preference", event)
                assert [p.calls for p in ports] == [1, 0]
            else:
                result = await reflect.answer("Explain this preference", event)
                assert result["status"] == "proposed" and result["memory_published"] is False
                assert [p.calls for p in ports] == [1, 1]
            assert await engine.repository.admission_records(scope) == before

    asyncio.run(run())
