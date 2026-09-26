import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
import sys

from agent_memory import MemoryScope
from agent_memory.unified_memory import UnifiedMemory
from agent_memory.comparison_adapters import (
    RawInteraction, AgentMemoryComparisonAdapter, Mem0ComparisonAdapter,
    GraphitiComparisonAdapter, compare_raw_interactions,
)


def test_raw_comparison_protocol_uses_real_local_store_and_external_contract_doubles(tmp_path, monkeypatch):
    """Contract doubles do not measure Mem0 or Graphiti quality."""
    class Mem0:
        def __init__(self):
            self.items = []
            self.received = []
        def add(self, messages, *, user_id, timestamp, infer):
            assert infer is True
            self.received.append(messages[0]["content"])
            self.items.append(dict(id=str(len(self.items)), memory=messages[0]["content"]))
        def search(self, query, *, filters, top_k):
            assert filters["user_id"].startswith("memory-eval-")
            return {"results": self.items[:top_k]}
        def delete_all(self, *, user_id):
            self.items.clear()

    class Graphiti:
        def __init__(self):
            self.items = {}
            self.received = []
        async def add_episode(self, **kwargs):
            self.received.append(kwargs["episode_body"])
            episode_id = str(len(self.items))
            self.items[episode_id] = kwargs["episode_body"]
            return SimpleNamespace(episode=SimpleNamespace(uuid=episode_id))
        async def search(self, query, *, group_ids, num_results):
            assert group_ids[0].startswith("memory-eval-")
            return [SimpleNamespace(uuid=k, fact=v) for k,v in self.items.items()][:num_results]
        async def remove_episode(self, episode_id):
            del self.items[episode_id]

    monkeypatch.setitem(sys.modules, "graphiti_core.nodes", SimpleNamespace(EpisodeType=SimpleNamespace(text="text")))
    async def scenario():
        memory = UnifiedMemory.local(tmp_path / "core.db", MemoryScope("comparison", session_id="test"))
        await memory.initialize()
        first = RawInteraction("one", "user", "I prefer tea", datetime(2026, 9, 26, tzinfo=UTC))
        second = RawInteraction("two", "user", "Now I prefer coffee", first.occurred_at)
        mem0, graphiti = Mem0(), Graphiti()
        try:
            report = await compare_raw_interactions(
                (AgentMemoryComparisonAdapter(memory), Mem0ComparisonAdapter(mem0), GraphitiComparisonAdapter(graphiti)),
                (first,), (second,), ("preference",))
            assert all(a["status"] == "completed" for a in report["arms"])
            assert mem0.received == graphiti.received == [first.body(), second.body()]
            assert all(a["steps"][-1]["results"] == [] for a in report["arms"])
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_comparison_records_failure_without_exposing_provider_message():
    class Broken:
        name = "broken"
        async def add(self, event):
            raise RuntimeError("private api_key=must-not-leak")
    event = RawInteraction("one", "user", "input", datetime(2026, 9, 26, tzinfo=UTC))
    report = asyncio.run(compare_raw_interactions((Broken(),), (event,), (), ("query",)))
    assert report["arms"][0]["status"] == "failed"
    assert report["arms"][0]["cleanup_required"]
    assert "must-not-leak" not in str(report)
