"""Run a small local Agent Memory, Mem0 OSS, and Graphiti comparison pilot."""

from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from datetime import UTC, datetime
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import tempfile
from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

os.environ["MEM0_TELEMETRY"] = "false"
os.environ["GRAPHITI_TELEMETRY_ENABLED"] = "false"

import ollama
from graphiti_core import Graphiti
from graphiti_core.cross_encoder.openai_reranker_client import OpenAIRerankerClient
from graphiti_core.driver.falkordb_driver import FalkorDriver
from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient
from mem0 import Memory
from redislite.async_falkordb_client import AsyncFalkorDB

from agent_memory import MemoryScope
from agent_memory.evaluation.comparison import (
    AgentMemoryComparisonAdapter,
    GraphitiComparisonAdapter,
    Mem0ComparisonAdapter,
    RawInteraction,
    compare_raw_interactions,
)
from agent_memory.composition import build_local_kernel
from agent_memory.providers import GeneratedTrajectoryClaimExtractor
from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory


MODEL = "qwen3.5:9b"
EMBED_MODEL = "nomic-embed-text"
OLLAMA_URL = "http://localhost:11434"
QUERY_SPECS = (
    {
        "id": "current-residence",
        "query": "林晓现在住在哪里？",
        "required_aliases": [["苏州", "Suzhou"]],
    },
    {
        "id": "cat-preference",
        "query": "林晓的猫叫什么、最喜欢吃什么？",
        "required_aliases": [["点点", "DianDian", "Dian Dian"], ["鸡胸肉", "chicken breast"]],
    },
    {
        "id": "project-settings",
        "query": "林晓项目现在使用什么数据库和 Python 版本？",
        "required_aliases": [["PostgreSQL", "postgres"], ["3.13"]],
    },
    {
        "id": "planned-meeting",
        "query": "林晓计划什么时候、在哪里和谁见面？",
        "required_aliases": [
            ["陈晨", "Chen Chen"],
            ["2026-10-02", "2026年10月2日", "October 2"],
            ["15:00", "下午3点", "3 p.m."],
            ["西湖", "West Lake"],
        ],
    },
)
INITIAL = (
    RawInteraction("residence-v1", "user", "用户林晓目前常住杭州。", datetime(2026, 9, 26, tzinfo=UTC)),
    RawInteraction("cat-preference", "user", "林晓的猫叫点点，最喜欢吃鸡胸肉。", datetime(2026, 9, 26, tzinfo=UTC)),
    RawInteraction("project-v1", "user", "林晓项目环境使用 Python 3.13，数据库是 SQLite。", datetime(2026, 9, 26, tzinfo=UTC)),
    RawInteraction("planned-meeting", "user", "林晓计划于2026年10月2日15:00在杭州西湖边与陈晨见面。", datetime(2026, 9, 26, tzinfo=UTC)),
)
UPDATES = (
    RawInteraction("residence-v2", "user", "更正居住地：林晓当前住在苏州；杭州是此前的居住地。", datetime(2026, 9, 27, tzinfo=UTC)),
    RawInteraction("project-v2", "user", "林晓项目数据库已从 SQLite 切换为 PostgreSQL；Python 版本保持 3.13。", datetime(2026, 9, 27, tzinfo=UTC)),
)


def _contains_alias_groups(text: str, groups: list[list[str]]) -> bool:
    normalized = text.casefold()
    return all(any(alias.casefold() in normalized for alias in group) for group in groups)


class OllamaClaimGenerator:
    def __init__(self, client: ollama.AsyncClient, calls: dict[str, int]):
        self.client = client
        self.calls = calls

    async def generate_claims(self, event):
        self.calls["agent-memory"] += 1
        prompt = (
            "Extract stable user facts and explicit corrections from this one message. "
            "Return JSON with a claims array; each claim has key, value, text, scope, confidence. "
            "Use session scope. Use consistent keys for the same fact, retain correction meaning, "
            "and return an empty array when there is no fact. Do not invent information.\n"
            f"Message: {event.content}"
        )
        response = await self.client.chat(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            format="json",
            think=False,
            options={"temperature": 0, "num_predict": 512},
        )
        claims = json.loads(response.message.content).get("claims", [])
        if not isinstance(claims, list):
            raise ValueError("Qwen claim output must contain a claims list")
        return claims


class OllamaEmbeddings:
    dimensions = 768

    def __init__(self, client: ollama.AsyncClient):
        self.client = client

    async def embed(self, texts):
        response = await self.client.embed(model=EMBED_MODEL, input=list(texts))
        if any(len(vector) != self.dimensions for vector in response.embeddings):
            raise ValueError("unexpected Ollama embedding dimensions")
        return response.embeddings


class NativeOllamaCompletionClient:
    """Adapt Graphiti's OpenAI client contract to Ollama chat with thinking off."""

    def __init__(
        self,
        client: ollama.AsyncClient,
        calls: dict[str, int],
        *,
        max_extraction_items: int | None = None,
    ):
        self.client = client
        self.calls = calls
        self.max_extraction_items = max_extraction_items
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    async def create(
        self,
        *,
        model,
        messages,
        temperature=0,
        max_tokens=2048,
        response_format=None,
        **_kwargs,
    ):
        self.calls["graphiti"] += 1
        ollama_format = "json"
        if response_format and response_format.get("type") == "json_schema":
            ollama_format = deepcopy(response_format["json_schema"]["schema"])
            if self.max_extraction_items is not None:
                properties = ollama_format.get("properties", {})
                for field_name in ("extracted_entities", "edges"):
                    if field_name in properties:
                        properties[field_name]["maxItems"] = self.max_extraction_items
        started = perf_counter()
        response = await self.client.chat(
            model=model,
            messages=messages,
            format=ollama_format,
            think=False,
            options={"temperature": temperature, "num_predict": max_tokens, "num_ctx": 32768},
        )
        if os.environ.get("MEMORY_BENCH_PROFILE") == "1":
            schema_name = (
                response_format.get("json_schema", {}).get("name", "json")
                if response_format else "json"
            )
            print(
                f"graphiti llm schema={schema_name} seconds={perf_counter() - started:.1f} "
                f"prompt_tokens={response.prompt_eval_count} output_tokens={response.eval_count} "
                f"done_reason={response.done_reason}",
                flush=True,
            )
        message = SimpleNamespace(content=response.message.content)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


async def _make_arm_clients(
    run_dir: Path,
    client: ollama.AsyncClient,
    calls: dict[str, int],
    *,
    mem0_collection_name: str | None = None,
    graphiti_max_extraction_items: int | None = None,
):
    llm_config = LLMConfig(
        api_key="ollama",
        model=MODEL,
        small_model=MODEL,
        base_url=OLLAMA_URL + "/v1",
        temperature=0,
        max_tokens=8192,
    )
    graphiti_http = NativeOllamaCompletionClient(
        client, calls, max_extraction_items=graphiti_max_extraction_items
    )
    graphiti_llm = OpenAIGenericClient(
        config=llm_config,
        client=graphiti_http,
        max_tokens=8192,
        structured_output_mode="json_schema",
    )
    graph_embedder = OpenAIEmbedder(
        config=OpenAIEmbedderConfig(
            api_key="ollama",
            embedding_model=EMBED_MODEL,
            embedding_dim=768,
            base_url=OLLAMA_URL + "/v1",
        )
    )
    lite_db = AsyncFalkorDB(dbfilename=str(run_dir / "graphiti.db"))
    graph_driver = FalkorDriver(falkor_db=lite_db)
    graphiti = Graphiti(
        graph_driver=graph_driver,
        llm_client=graphiti_llm,
        embedder=graph_embedder,
        cross_encoder=OpenAIRerankerClient(client=graphiti_llm, config=llm_config),
        max_coroutines=1,
    )
    await graphiti.build_indices_and_constraints()

    mem0 = Memory.from_config(
        {
            "vector_store": {
                "provider": "qdrant",
                "config": {
                    "path": str(run_dir / "qdrant"),
                    "collection_name": mem0_collection_name or "memory_comparison_" + uuid4().hex,
                    "embedding_model_dims": 768,
                },
            },
            "llm": {
                "provider": "ollama",
                "config": {
                    "model": MODEL,
                    "temperature": 0,
                    "max_tokens": 2048,
                    "ollama_base_url": OLLAMA_URL,
                },
            },
            "embedder": {
                "provider": "ollama",
                "config": {"model": EMBED_MODEL, "ollama_base_url": OLLAMA_URL},
            },
        }
    )
    original_mem0_chat = mem0.llm.client.chat

    def mem0_chat_without_thinking(**kwargs):
        calls["mem0-oss"] += 1
        kwargs["options"] = {**kwargs.get("options", {}), "num_ctx": 32768}
        return original_mem0_chat(think=False, **kwargs)

    mem0.llm.client.chat = mem0_chat_without_thinking
    return mem0, graphiti, lite_db


async def _answer(client: ollama.AsyncClient, query: str, memories: list[dict]):
    context = "\n".join(f"- {item['text']}" for item in memories) or "（没有检索到记忆）"
    prompt = (
        "只根据给定记忆回答问题，忽略不在记忆中的知识。用中文给出简短答案；"
        "信息不足时回答‘未知’。输出 JSON：{\"answer\":\"...\"}\n"
        f"问题：{query}\n记忆：\n{context}"
    )
    started = perf_counter()
    response = await client.chat(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        format="json",
        think=False,
        options={"temperature": 0, "num_predict": 256},
    )
    return json.loads(response.message.content)["answer"], perf_counter() - started


async def _stop_falkordblite(pid: int | None):
    """Stop the run-scoped embedded DB process after its client has closed."""
    if not pid:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    for _ in range(20):
        await asyncio.sleep(0.1)
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def run(output_json: Path | None = None):
    started = perf_counter()
    calls = {"agent-memory": 0, "mem0-oss": 0, "graphiti": 0, "answer": 0}
    client = ollama.AsyncClient(host=OLLAMA_URL)
    with tempfile.TemporaryDirectory(prefix="agent-memory-comparison-") as sandbox:
        run_dir = Path(sandbox)
        mem0, graphiti, lite_db = await _make_arm_clients(run_dir, client, calls)
        scope = MemoryScope("memory-comparison-pilot", user_id="synthetic-user", session_id="pilot")
        local_path = run_dir / "agent-memory.sqlite3"
        local_memory = UnifiedMemory(
            build_local_kernel(
                local_path,
                extractor=GeneratedTrajectoryClaimExtractor(
                    OllamaClaimGenerator(client, calls),
                    provider="ollama",
                    model=MODEL,
                    event_types=("user.message", "agent.model.completed", "agent.tool.completed"),
                    fail_open=False,
                ),
                embedding_provider=OllamaEmbeddings(client),
            ),
            scope,
            journal=SQLiteDeletionJournal(str(local_path) + ".deletions.db"),
        )
        await local_memory.initialize()
        adapters = (
            AgentMemoryComparisonAdapter(local_memory),
            Mem0ComparisonAdapter(mem0),
            GraphitiComparisonAdapter(graphiti),
        )
        try:
            raw_report = await compare_raw_interactions(
                adapters,
                INITIAL,
                UPDATES,
                tuple(spec["query"] for spec in QUERY_SPECS),
            )
            scored_arms = []
            for arm in raw_report["arms"]:
                steps = arm["steps"]
                updated_results = {
                    step["operation"].removeprefix("search.updated:"): step["results"] or []
                    for step in steps
                    if step["operation"].startswith("search.updated:")
                }
                cases = []
                if arm["status"] == "completed":
                    for spec in QUERY_SPECS:
                        memories = updated_results.get(spec["query"], [])
                        evidence = "\n".join(item["text"] for item in memories)
                        retrieval_hit = _contains_alias_groups(evidence, spec["required_aliases"])
                        answer, seconds = await _answer(client, spec["query"], memories)
                        calls["answer"] += 1
                        cases.append(
                            {
                                "case_id": spec["id"],
                                "query": spec["query"],
                                "retrieved_count": len(memories),
                                "retrieval_hit": retrieval_hit,
                                "answer": answer,
                                "answer_seconds": seconds,
                                "answer_hit": _contains_alias_groups(answer, spec["required_aliases"]),
                                "required_aliases": spec["required_aliases"],
                            }
                        )
                post_delete = {
                    step["operation"].removeprefix("search.deleted:"): step["results"] or []
                    for step in steps
                    if step["operation"].startswith("search.deleted:")
                }
                scored_arms.append(
                    {
                        "name": arm["name"],
                        "version": arm.get("version"),
                        "status": arm["status"],
                        "error_type": arm.get("error_type"),
                        "memory_model_calls": calls.get(arm["name"], 0),
                        "retrieval_hit_rate": sum(case["retrieval_hit"] for case in cases) / len(cases) if cases else None,
                        "answer_hit_rate": sum(case["answer_hit"] for case in cases) / len(cases) if cases else None,
                        "delete_pass": bool(post_delete) and all(not results for results in post_delete.values()),
                        "operation_timings": [
                            {"operation": step["operation"], "seconds": step["seconds"]}
                            for step in steps
                        ],
                        "cases": cases,
                    }
                )
            report = {
                "schema_version": 1,
                "run_kind": "single-run-pilot",
                "input_digest": raw_report["input_digest"],
                "model": MODEL,
                "embedding_model": EMBED_MODEL,
                "model_settings": {"temperature": 0, "thinking": False},
                "dataset": "synthetic-chinese-memory-pilot-v1",
                "package_versions": {
                    name: importlib.metadata.version(name)
                    for name in ("agent-memory", "mem0ai", "graphiti-core", "ollama")
                },
                "arms": scored_arms,
                "shared_answer_calls": calls["answer"],
                "total_seconds": perf_counter() - started,
                "limitations": [
                    "single run; no confidence intervals",
                    "substring grader; review answers manually before broad claims",
                    "Graphiti is Zep open-source Graphiti, not Zep Cloud",
                    "local embedding model is nomic-embed-text; Qwen is used for extraction and answer generation",
                ],
            }
            if output_json:
                output_json.parent.mkdir(parents=True, exist_ok=True)
                output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
            return report
        finally:
            graph_pid = getattr(getattr(lite_db, "client", None), "pid", None)
            for adapter in adapters:
                try:
                    await adapter.clear()
                except Exception:
                    pass
            try:
                await local_memory.close()
            finally:
                try:
                    await graphiti.close()
                finally:
                    try:
                        await lite_db.close()
                    finally:
                        await _stop_falkordblite(graph_pid)
            qdrant_client = getattr(getattr(mem0, "vector_store", None), "client", None)
            if qdrant_client is not None:
                qdrant_client.close()
            await client._client.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path, help="Write the scored report to this file")
    args = parser.parse_args()
    report = asyncio.run(run(args.output_json))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
