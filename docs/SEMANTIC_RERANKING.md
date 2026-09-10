# Optional Semantic Reranking

The core keeps no vector index and no embedding cache. Pass an `EmbeddingProvider` when building
the local or PostgreSQL provider to rerank only the bounded retrieval candidate set for a request.
This improves semantic matching without introducing a required model, network dependency, schema
migration, or resident vector memory.

```python
from agent_memory import AgentMemory


class MyEmbeddings:
    dimensions = 768

    async def embed(self, texts):
        return await external_embedding_service(texts)


memory = AgentMemory.local(
    "agent-memory.db",
    embedding_provider=MyEmbeddings(),
)
```

The reranker embeds the query and at most 48 candidates in one batch. It combines semantic cosine
similarity with the existing rank order. Provider errors, invalid vector dimensions, non-finite
values, and zero vectors fail open to the original ranking.
