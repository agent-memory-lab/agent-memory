"""Typed query/document embedding roles without model coupling in the kernel."""

import ast
import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_memory import EmbeddingReranker, MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.providers import ReciprocalRankFusionReranker

SCOPE = MemoryScope("typed-embeddings", session_id="session")
QUERY = MemoryQuery(SCOPE, "vehicle transport")
ITEMS = (
    MemoryItem("literal", MemoryKind.EVENT, "vehicle timetable", 1.0, datetime.now(UTC)),
    MemoryItem("meaning", MemoryKind.EVENT, "an automobile carries passengers", 0.1,
               datetime.now(UTC)),
)


class TypedProvider:
    def __init__(self, *, error=None):
        self.calls = []
        self.legacy_calls = []
        self.error = error

    async def embed_for_retrieval(self, query, documents):
        self.calls.append((query, tuple(documents)))
        if self.error == "exception":
            raise RuntimeError("typed provider unavailable")
        if self.error == "count":
            return ((1.0, 0.0),)
        if self.error == "dimensions":
            return ((1.0, 0.0), (0.0,), (1.0, 0.0))
        return ((1.0, 0.0), (0.0, 1.0), (1.0, 0.0))

    async def embed(self, texts):
        self.legacy_calls.append(tuple(texts))
        raise AssertionError("typed providers must not silently retry through legacy input")


class LegacyProvider:
    def __init__(self):
        self.calls = []

    async def embed(self, texts):
        self.calls.append(tuple(texts))
        return ((1.0, 0.0), (0.0, 1.0), (1.0, 0.0))


def test_reranker_prefers_host_typed_capability_and_does_not_rewrite_text():
    async def exercise():
        provider = TypedProvider()
        ranked = await EmbeddingReranker(provider).rerank(QUERY, ITEMS)
        assert provider.calls == [(QUERY.text, tuple(item.text for item in ITEMS))]
        assert provider.legacy_calls == []
        assert ranked[0].id == "meaning"
        assert ranked[0].metadata["semantic_rank"] == 1

    asyncio.run(exercise())


def test_existing_embed_only_provider_keeps_legacy_batch_contract():
    async def exercise():
        provider = LegacyProvider()
        ranked = await EmbeddingReranker(provider).rerank(QUERY, ITEMS)
        assert provider.calls == [(QUERY.text, *(item.text for item in ITEMS))]
        assert ranked[0].id == "meaning"

    asyncio.run(exercise())


@pytest.mark.parametrize("error", ("count", "dimensions", "exception"))
@pytest.mark.parametrize("fail_open", (True, False))
def test_typed_invalid_batch_and_exception_follow_configured_failure_policy(error, fail_open):
    async def exercise():
        provider = TypedProvider(error=error)
        reranker = EmbeddingReranker(provider, fail_open=fail_open)
        if fail_open:
            baseline = await ReciprocalRankFusionReranker().rerank(QUERY, ITEMS)
            assert await reranker.rerank(QUERY, ITEMS) == baseline
        else:
            expected = RuntimeError if error == "exception" else ValueError
            with pytest.raises(expected):
                await reranker.rerank(QUERY, ITEMS)
        assert len(provider.calls) == 1
        assert provider.legacy_calls == []

    asyncio.run(exercise())


def load_ollama_provider():
    """Compile the actual example class without importing unrelated framework SDKs."""
    path = Path(__file__).resolve().parents[1] / "examples" / "ollama_memory_comparison.py"
    source = ast.parse(path.read_text())
    selected = [node for node in source.body if (
        isinstance(node, ast.ClassDef) and node.name == "OllamaEmbeddings"
    ) or (
        isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "EMBED_MODEL"
                for target in node.targets)
    )]
    namespace = {"ollama": SimpleNamespace(AsyncClient=object)}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["OllamaEmbeddings"]


class FakeOllamaClient:
    def __init__(self, *, wrong_count=False, dimensions=768):
        self.calls = []
        self.wrong_count = wrong_count
        self.dimensions = dimensions

    async def embed(self, **kwargs):
        self.calls.append(kwargs)
        count = len(kwargs["input"]) - int(self.wrong_count)
        return SimpleNamespace(embeddings=[
            [float(index + 1)] * self.dimensions for index in range(count)
        ])


def test_nomic_typed_provider_uses_one_role_specific_batch_without_truncation():
    async def exercise():
        client = FakeOllamaClient()
        provider = load_ollama_provider()(client)
        query = "search_document: find the records"
        documents = ("Where are the records?", "search_query: a literal document prefix", "")
        vectors = await provider.embed_for_retrieval(query, documents)
        assert client.calls == [{
            "model": "nomic-embed-text",
            "input": [f"search_query: {query}",
                      *(f"search_document: {document}" for document in documents)],
            "truncate": False,
        }]
        assert provider.retrieval_prefix_version == "nomic-search-prefixes/1"
        assert [vector[0] for vector in vectors] == [1.0, 2.0, 3.0, 4.0]
        assert all(len(vector) == provider.dimensions for vector in vectors)

    asyncio.run(exercise())


def test_nomic_legacy_embed_keeps_its_existing_unprefixed_behavior():
    async def exercise():
        client = FakeOllamaClient()
        provider = load_ollama_provider()(client)
        vectors = await provider.embed(("unchanged query", "unchanged document"))
        assert client.calls == [{"model": "nomic-embed-text",
                                 "input": ["unchanged query", "unchanged document"]}]
        assert len(vectors) == 2

    asyncio.run(exercise())


def test_real_reranker_calls_actual_ollama_typed_method_with_fake_client():
    async def exercise():
        client = FakeOllamaClient()
        provider = load_ollama_provider()(client)
        ranked = await EmbeddingReranker(provider, fail_open=False).rerank(QUERY, ITEMS)
        assert len(ranked) == len(ITEMS)
        assert len(client.calls) == 1
        assert client.calls[0]["input"] == [
            f"search_query: {QUERY.text}", *(f"search_document: {item.text}" for item in ITEMS),
        ]
        assert client.calls[0]["truncate"] is False
        assert all(item.metadata["semantic_dimensions"] == provider.dimensions for item in ranked)

    asyncio.run(exercise())


@pytest.mark.parametrize("options", ({"wrong_count": True}, {"dimensions": 3}))
def test_nomic_typed_provider_rejects_wrong_batch_shape(options):
    async def exercise():
        client = FakeOllamaClient(**options)
        provider = load_ollama_provider()(client)
        with pytest.raises(ValueError, match="unexpected Ollama embedding"):
            await provider.embed_for_retrieval("query", ("document",))
        assert len(client.calls) == 1

    asyncio.run(exercise())
