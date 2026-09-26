"""Real-client adapters for raw-input comparison, not precomputed observations.

No SDK imports or network activity until a host supplies configured clients.
Use fresh isolated evaluation scopes, never production user/group IDs.
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from time import perf_counter
from uuid import uuid4


@dataclass(frozen=True)
class RawInteraction:
    event_id: str
    role: str
    content: str
    occurred_at: datetime

    def __post_init__(self):
        if self.role not in ("user", "assistant", "tool"):
            raise ValueError("unsupported raw role")
        if not isinstance(self.event_id, str) or not 1 <= len(self.event_id) <= 128:
            raise ValueError("invalid event ID")
        if not isinstance(self.content, str) or len(self.content.encode()) > 12000:
            raise ValueError("raw content exceeds comparison input budget")
        if self.occurred_at.utcoffset() is None:
            raise ValueError("comparison timestamps must be timezone aware")

    def body(self):
        return f"[{self.occurred_at.isoformat()}] {self.role}: {self.content}"


class AgentMemoryComparisonAdapter:
    name = "agent-memory"
    def __init__(self, memory):
        self.memory = memory

    async def add(self, event):
        return await self.memory.capture(event_id=event.event_id, role=event.role,
            content=event.body(), run_id="comparison", occurred_at=event.occurred_at)

    async def search(self, query, limit=8):
        bundle = await self.memory.recall(query)
        return [dict(id=i.id, text=i.text) for i in bundle.relevant_memories[:limit]]

    async def clear(self):
        await self.memory.forget_sources(all_in_scope=True)


class Mem0ComparisonAdapter:
    """Current synchronous mem0.Memory contract; not Mem0 Platform."""
    name = "mem0-oss"
    def __init__(self, client):
        self.client = client
        self.user_id = "memory-eval-" + uuid4().hex

    async def add(self, event):
        return await asyncio.to_thread(self.client.add,
            [{"role": "user", "content": event.body()}], user_id=self.user_id,
            timestamp=event.occurred_at.isoformat(), infer=True)

    async def search(self, query, limit=8):
        result = await asyncio.to_thread(self.client.search, query,
            filters={"user_id": self.user_id}, top_k=limit)
        if not isinstance(result, dict) or not isinstance(result.get("results"), list):
            raise ValueError("unsupported Mem0 search contract")
        return [dict(id=v["id"], text=v["memory"]) for v in result["results"]]

    async def clear(self):
        await asyncio.to_thread(self.client.delete_all, user_id=self.user_id)


class GraphitiComparisonAdapter:
    """Graphiti, not Zep Cloud. Delete recorded episodes through the real API."""
    name = "graphiti"
    def __init__(self, client):
        self.client = client
        self.group_id = "memory-eval-" + uuid4().hex
        self.episodes = []

    async def add(self, event):
        if len(self.episodes) >= 256:
            raise ValueError("comparison episode budget exceeded")
        from graphiti_core.nodes import EpisodeType
        result = await self.client.add_episode(name=event.event_id,
            episode_body=event.body(), source_description="raw comparison interaction",
            reference_time=event.occurred_at, source=EpisodeType.text, group_id=self.group_id)
        self.episodes.append(result.episode.uuid)
        return result.episode.uuid

    async def search(self, query, limit=8):
        result = await self.client.search(query, group_ids=[self.group_id], num_results=limit)
        return [dict(id=e.uuid, text=e.fact) for e in result]

    async def clear(self):
        for episode in tuple(self.episodes):
            await self.client.remove_episode(episode)
            self.episodes.remove(episode)


async def compare_raw_interactions(adapters, initial, updates, queries):
    """Record actual results and elapsed time without inventing accuracy scores.

    Updates are new raw interactions, never gold claims. Clear is whole-run
    deletion, not proof of per-source cascading or physical erasure. Each arm
    should be constructed afresh; callers own cleanup after failed ingestion.
    """
    adapters, initial, updates, queries = map(tuple, (adapters, initial, updates, queries))
    if not 1 <= len(adapters) <= 8 or len({a.name for a in adapters}) != len(adapters):
        raise ValueError("one to eight distinct adapters required")
    events = initial + updates
    if not 1 <= len(events) <= 256 or len({e.event_id for e in events}) != len(events):
        raise ValueError("one to 256 uniquely identified interactions required")
    if not 1 <= len(queries) <= 64 or any(not isinstance(q, str) or not 1 <= len(q) <= 4096 for q in queries):
        raise ValueError("provide one to 64 bounded queries")
    manifest = dict(initial=[e.body() for e in initial], updates=[e.body() for e in updates], queries=queries)
    digest = sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    report = dict(input_digest=digest, arms=[])
    for adapter in adapters:
        arm = dict(name=adapter.name, steps=[], status="running")
        report["arms"].append(arm)
        async def record(operation, call):
            start = perf_counter()
            value = await call
            arm["steps"].append(dict(operation=operation, seconds=perf_counter()-start,
                                     results=value if operation.startswith("search") else None))
        try:
            for event in initial:
                await record("add", adapter.add(event))
            for query in queries:
                await record("search.initial:" + query, adapter.search(query))
            for event in updates:
                await record("update.add", adapter.add(event))
            for query in queries:
                await record("search.updated:" + query, adapter.search(query))
            await record("delete.run", adapter.clear())
            for query in queries:
                await record("search.deleted:" + query, adapter.search(query))
            arm["status"] = "completed"
        except Exception as error:
            arm["status"] = "failed"
            arm["error_type"] = type(error).__name__
            # Provider messages can contain credentials or private input.
            arm["cleanup_required"] = True
    return report
