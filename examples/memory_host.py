"""Authored synthetic full capture -> extraction -> authoritative verification host.

Run: python examples/memory_host.py
Uses a disposable SQLite database and an explicitly authenticated synthetic registry.
Replace the rule pipeline with ModelAtomGenerator/Reviewer only with explicit
source-recipient grants. Optional QuestionService uses the same refresh owner.
"""

import asyncio
import json
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.extraction_rules import RuleBasedAtomAdapter
from agent_memory.consolidation.verification_tools import LocalRecordVerifier
from agent_memory.domain import MemoryEvent, MemoryScope, PredicateSpec, SourceAuthority, utc_now
from agent_memory.operations.domain_verification import (
    DomainVerificationQueue,
    VerificationToolSpec,
    admission_publisher,
)
from agent_memory.operations.memory_host import MemoryHost
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    with TemporaryDirectory(prefix="memory-host-demo-") as folder:
        scope = MemoryScope("authored-demo", user_id="alice", session_id="demo")
        repository = SQLiteMemoryRepository(Path(folder) / "memory.db")
        rules = RuleBasedAtomAdapter("alice")
        policy = AdmissionPolicy([PredicateSpec("home_city", allow_self_report=False)])
        user = SourceAuthority("alice-login", subjects=("alice",), predicates=("home_city",))
        issuer = SourceAuthority(
            "authored-registry", "tool_observation", ("alice",), ("home_city",)
        )
        now = utc_now()
        path = Path(folder) / "registry.json"
        path.write_text(
            json.dumps(
                {
                    "schema": "authoritative-domain-records/1",
                    "issuer": issuer.source_id,
                    "records": [
                        {
                            "subject_id": "alice",
                            "predicate": "home_city",
                            "value": "Hangzhou",
                            "valid_from": (now - timedelta(days=1)).isoformat(),
                            "valid_to": None,
                            "recorded_at": now.isoformat(),
                        }
                    ],
                }
            )
        )
        tool = LocalRecordVerifier(
            path,
            expected_sha256=sha256(path.read_bytes()).hexdigest(),
            spec=VerificationToolSpec("registry", "1", issuer, ("alice",), ("home_city",)),
        )

        async def authorized(*_):
            return True  # Only this scoped, authored demo fixture is authorized.

        verification = DomainVerificationQueue(
            repository,
            scope,
            (tool,),
            authorize=authorized,
            publisher=admission_publisher(AdmissionEngine(repository), policy),
        )
        host = MemoryHost(
            repository,
            scope,
            AtomExtractionPipeline(rules, rules),
            policy,
            user,
            on_accept=authorized,
            verification=verification,
        )
        event = MemoryEvent(scope, "message", "我住在杭州")
        await host.submit(event, request_id="source-one", producer_id="authored-host")
        print(json.dumps(await host.run_once(), ensure_ascii=False))
        print(json.dumps(await host.metrics(), ensure_ascii=False))
        restarted = MemoryHost(
            repository,
            scope,
            host.pipeline,
            policy,
            user,
            on_accept=authorized,
            verification=verification,
        )
        assert (await restarted.run_once())["extraction"]["claimed"] == 0
        host.stop()


if __name__ == "__main__":
    asyncio.run(main())
