import asyncio

import pytest
import test_atom_admission as base
from test_persona_evolution import fixture

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.extraction_rules import RuleBasedAtomAdapter
from agent_memory.domain import PredicateSpec, SourceAuthority
from agent_memory.operations.memory_host import MemoryHost

store = base.store


def test_host_automatically_reconciles_and_runs_registered_persona_on_shared_scheduler(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, _, _, _ = await fixture(engine, kernel, scope, clock)
            rules = RuleBasedAtomAdapter("alice")

            async def accept(*_):
                return True

            host = MemoryHost(
                engine.repository,
                scope,
                AtomExtractionPipeline(rules, rules),
                AdmissionPolicy((PredicateSpec("locale"),)),
                SourceAuthority("alice", subjects=("alice",), predicates=("locale",)),
                on_accept=accept,
                evolutions=(lifecycle,),
                clock=lambda: clock[0],
            )
            cycle = await host.run_once()
            assert not cycle["stage_errors"]
            assert cycle["refresh"]["completed"] == 1
            assert host.refresh.queue is lifecycle.queue
            result = await lifecycle.read("communication", actor="alice", context={})
            assert result["state"] == "active"
            assert cycle["evolution_discovered"] >= 1
            host.stop()
            # The long-lived host may restart with a persona-only shared queue.
            task = asyncio.create_task(host.run(poll_seconds=0.01))
            await asyncio.sleep(0.02)
            host.stop()
            await asyncio.wait_for(task, timeout=2)

    asyncio.run(run())


def test_host_rejects_evolution_from_an_unrelated_scope_or_queue(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, _, _, _ = await fixture(engine, kernel, scope, clock)
            rules = RuleBasedAtomAdapter("alice")

            async def accept(*_):
                return True

            lifecycle.scope = base.MemoryScope("other", user_id="alice", session_id="session")
            with pytest.raises(ValueError, match="evolution"):
                MemoryHost(
                    engine.repository,
                    scope,
                    AtomExtractionPipeline(rules, rules),
                    AdmissionPolicy((PredicateSpec("locale"),)),
                    SourceAuthority("alice", subjects=("alice",), predicates=("locale",)),
                    on_accept=accept,
                    evolutions=(lifecycle,),
                    clock=lambda: clock[0],
                )

    asyncio.run(run())
