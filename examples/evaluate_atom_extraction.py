"""Run offline authored cases: python examples/evaluate_atom_extraction.py."""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory import (
    AdmissionPolicy,
    AtomExtractionPipeline,
    MemoryScope,
    PredicateSpec,
    RuleBasedAtomAdapter,
    SourceAuthority,
)
from agent_memory.evaluation.extraction import evaluate_extraction
from agent_memory.sqlite import SQLiteMemoryRepository


async def main():
    fixture = json.loads((Path(__file__).parent / "data/atom_extraction_cases.json").read_text())
    adapter = RuleBasedAtomAdapter("alice")
    predicates = ("home_city", "response_language", "response_style")
    with TemporaryDirectory(prefix="atom-extraction-evaluation-") as directory:
        repository = SQLiteMemoryRepository(Path(directory) / "evaluation.db")
        await repository.initialize()
        report = await evaluate_extraction(
            repository,
            fixture["cases"],
            pipeline=AtomExtractionPipeline(adapter, adapter),
            scope=MemoryScope("offline-evaluation", user_id="alice", session_id="test"),
            authority=SourceAuthority(
                "authenticated-alice", subjects=("alice",), predicates=predicates
            ),
            policy=AdmissionPolicy([PredicateSpec(p) for p in predicates]),
        )
        print(
            json.dumps({"dataset": fixture["description"], **report}, ensure_ascii=False, indent=2)
        )


if __name__ == "__main__":
    asyncio.run(main())
