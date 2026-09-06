from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from agent_memory import MemoryScope


@dataclass(frozen=True, slots=True)
class VectorHit:
    memory_id: str
    distance: float


class PgVectorIndex:
    """Optional semantic index; the relational store remains authoritative."""

    def __init__(self, pool: Any, dimensions: int) -> None:
        if not 8 <= dimensions <= 65535:
            raise ValueError("dimensions must be between 8 and 65535")
        self._pool = pool
        self._dimensions = dimensions

    @property
    def dimensions(self) -> int:
        return self._dimensions

    async def initialize(self, *, install_extension: bool = False) -> None:
        async with self._pool.connection() as connection:
            async with connection.transaction():
                if install_extension:
                    await connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
                await connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS agent_memory_vectors (
                        memory_id text PRIMARY KEY,
                        partition_key text NOT NULL,
                        tenant_id text NOT NULL,
                        namespace text NOT NULL,
                        user_id text,
                        agent_id text,
                        workspace_id text,
                        session_id text,
                        embedding vector({self._dimensions}) NOT NULL,
                        model text NOT NULL,
                        created_at timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS agent_memory_vectors_scope_idx
                    ON agent_memory_vectors(
                        tenant_id, namespace, user_id, agent_id, workspace_id, session_id
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS agent_memory_vectors_hnsw_idx
                    ON agent_memory_vectors USING hnsw (embedding vector_cosine_ops)
                    """
                )

    async def upsert(
        self,
        memory_id: str,
        scope: MemoryScope,
        embedding: Sequence[float],
        *,
        model: str,
    ) -> None:
        vector = self._validated_vector(embedding)
        async with self._pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO agent_memory_vectors (
                    memory_id, partition_key, tenant_id, namespace, user_id, agent_id,
                    workspace_id, session_id, embedding, model
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector, %s)
                ON CONFLICT (memory_id) DO UPDATE SET
                    partition_key = excluded.partition_key,
                    tenant_id = excluded.tenant_id,
                    namespace = excluded.namespace,
                    user_id = excluded.user_id,
                    agent_id = excluded.agent_id,
                    workspace_id = excluded.workspace_id,
                    session_id = excluded.session_id,
                    embedding = excluded.embedding,
                    model = excluded.model,
                    created_at = now()
                """,
                (
                    memory_id,
                    scope.partition_key(),
                    scope.tenant_id,
                    scope.namespace,
                    scope.user_id,
                    scope.agent_id,
                    scope.workspace_id,
                    scope.session_id,
                    self._literal(vector),
                    model,
                ),
            )

    async def search(
        self,
        scope: MemoryScope,
        embedding: Sequence[float],
        *,
        limit: int,
    ) -> tuple[VectorHit, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        vector = self._literal(self._validated_vector(embedding))
        clauses = ["tenant_id = %s", "namespace = %s"]
        params: list[Any] = [scope.tenant_id, scope.namespace]
        for column, value in (
            ("user_id", scope.user_id),
            ("agent_id", scope.agent_id),
            ("workspace_id", scope.workspace_id),
            ("session_id", scope.session_id),
        ):
            clauses.append(f"({column} IS NULL OR {column} = %s)")
            params.append(value)
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                f"""
                SELECT memory_id, embedding <=> %s::vector AS distance
                FROM agent_memory_vectors
                WHERE {" AND ".join(clauses)}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (vector, *params, vector, limit),
            )
            return tuple(
                VectorHit(memory_id=row["memory_id"], distance=float(row["distance"]))
                for row in await cursor.fetchall()
            )

    def _validated_vector(self, embedding: Sequence[float]) -> tuple[float, ...]:
        if len(embedding) != self._dimensions:
            raise ValueError(
                f"embedding has {len(embedding)} dimensions; expected {self._dimensions}"
            )
        vector = tuple(float(value) for value in embedding)
        if any(value != value or value in (float("inf"), float("-inf")) for value in vector):
            raise ValueError("embedding contains a non-finite value")
        return vector

    @staticmethod
    def _literal(vector: Sequence[float]) -> str:
        return "[" + ",".join(format(value, ".17g") for value in vector) + "]"
