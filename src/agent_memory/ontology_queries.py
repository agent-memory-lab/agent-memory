"""Exact authorization and bounded graph queries shared by SQL adapters."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime
import json


def _limit(value, maximum, name):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")


def _ids(values, maximum, name):
    if not isinstance(values, (tuple, list)) or not values or len(values) > maximum:
        raise ValueError(f"{name} must contain 1 to {maximum} IDs")
    if any(not isinstance(value, str) or not value.strip() or len(value) > 256 for value in values):
        raise ValueError(f"invalid {name}")
    return tuple(dict.fromkeys(values))


class SQLOntologyQueries:
    """Small SQL query mixin; adapter supplies a transactional _connect()."""

    def _validity_sql(self):
        return "julianday(a.valid_from)<=julianday(?) AND (a.valid_to IS NULL OR julianday(a.valid_to)>julianday(?))"

    async def get_assertions(self, scope, assertion_ids, *, ontology_id, ontology_version, at_time):
        ids = _ids(assertion_ids, 256, "assertion_ids")
        return await asyncio.to_thread(
            self._query_assertions, scope, ontology_id, ontology_version, at_time,
            "a.assertion_id IN (" + ",".join("?" for _ in ids) + ")", ids, len(ids),
        )

    async def neighbors(self, scope, entity_ids, *, ontology_id, ontology_version,
                        at_time, predicates=(), direction="outgoing", limit=100):
        ids = _ids(entity_ids, 256, "entity_ids")
        _limit(limit, 1000, "limit")
        if direction not in {"outgoing", "incoming", "both"}:
            raise ValueError("invalid graph direction")
        if not isinstance(predicates, (tuple, list)) or len(predicates) > 32:
            raise ValueError("at most 32 predicates are allowed")
        placeholders = ",".join("?" for _ in ids)
        column = "subject_entity_id" if direction != "incoming" else "object_entity_id"
        condition = f"a.{column} IN ({placeholders})"
        params = ids
        if direction == "both":
            condition = f"({condition} OR a.object_entity_id IN ({placeholders}))"
            params += ids
        if predicates:
            predicates = _ids(predicates, 32, "predicates")
            condition += " AND a.predicate_id IN (" + ",".join("?" for _ in predicates) + ")"
            params += predicates
        return await asyncio.to_thread(
            self._query_assertions, scope, ontology_id, ontology_version, at_time,
            condition, params, limit,
        )

    def _query_assertions(self, scope, ontology, version, at_time, condition, params, limit):
        from .domain import ScopeLevel
        from .ontology_memory import _assertion_from_row
        if not isinstance(at_time, datetime) or at_time.utcoffset() is None:
            raise ValueError("at_time must be timezone aware")
        scopes = {}
        for level in ScopeLevel:
            try:
                projected = scope.project(level)
                scopes[projected.partition_key()] = projected
            except ValueError:
                pass
        marks = ",".join("?" for _ in scopes)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"SELECT a.* FROM ontology_assertions a WHERE a.partition_key IN ({marks}) "
                "AND a.ontology_id=? AND a.ontology_version=? AND a.status='active' AND a.archived_at IS NULL "
                f"AND ({condition}) "
                "AND " + self._validity_sql() + " "
                "AND EXISTS (SELECT 1 FROM ontology_entities e WHERE e.partition_key=a.partition_key "
                "AND e.ontology_id=a.ontology_id AND e.ontology_version=a.ontology_version "
                "AND e.entity_id=a.subject_entity_id AND e.archived_at IS NULL) "
                "AND (a.object_entity_id IS NULL OR EXISTS (SELECT 1 FROM ontology_entities e "
                "WHERE e.partition_key=a.partition_key AND e.ontology_id=a.ontology_id "
                "AND e.ontology_version=a.ontology_version AND e.entity_id=a.object_entity_id AND e.archived_at IS NULL)) "
                "ORDER BY a.assertion_id LIMIT ?",
                (*scopes, ontology, version, *params, at_time.isoformat(), at_time.isoformat(), limit),
            ).fetchall()
            assertions = tuple(_assertion_from_row(row, scopes[row["partition_key"]]) for row in rows)
            return tuple(value for value in assertions if value.valid_from <= at_time
                         and (value.valid_to is None or at_time < value.valid_to))
        finally:
            connection.close()


@dataclass(frozen=True, slots=True)
class OntologyGraphResult:
    nodes: tuple[str, ...]
    edges: tuple
    paths: tuple[tuple[str, ...], ...]
    truncated: bool
    token_estimate: int
    token_count_kind: str = "estimate"
    tokenizer_id: str | None = None


def serialize_graph_content(nodes, edges, paths, truncated):
    """Canonical counted content; transport wrappers and accounting are excluded."""
    return json.dumps(dict(nodes=sorted(nodes), edges=[asdict(edge) for edge in edges],
                           paths=paths, truncated=truncated), default=str,
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))


async def traverse_ontology(store, scope, start_entity, *, ontology_id, ontology_version,
                            at_time, target_entity=None, predicates=(), direction="outgoing",
                            max_depth=2, max_nodes=32, max_edges=64, token_budget=1200,
                            token_counter=None):
    """Bounded BFS; paths contain assertion IDs and terminate at target_entity.

    Scope partitions never join into one path: repeated entity identifiers in
    different partitions remain separate traversal states. Only one shortest
    discovered path per partition/entity is retained.
    """
    from .domain import ScopeLevel
    _ids((start_entity,), 1, "start_entity")
    if target_entity is not None:
        _ids((target_entity,), 1, "target_entity")
    _limit(max_depth, 8, "max_depth")
    _limit(max_nodes, 256, "max_nodes")
    _limit(max_edges, 512, "max_edges")
    _limit(token_budget, 16000, "token_budget")
    partitions = {}
    for level in ScopeLevel:
        try:
            projected = scope.project(level)
            partitions[projected.partition_key()] = projected
        except ValueError:
            pass
    frontier = [(partition, start_entity, ()) for partition in partitions]
    seen = {(partition, start_entity) for partition in partitions}
    nodes, edges, paths, edge_ids = {start_entity}, [], [], set()
    tokens, truncated = max(1, (len(start_entity) + 7) // 4), False
    def measured(nodes, edges, paths):
        # Reserve both terminal flag values: BPE counts need not be additive.
        return max(token_counter.measure(serialize_graph_content(nodes, edges, paths, flag))
                   for flag in (True, False))

    if token_counter is not None:
        from .token_budget import TokenCounter
        if not isinstance(token_counter, TokenCounter):
            raise TypeError("token_counter must be a TokenCounter")
        tokens = measured(nodes, edges, paths)
    if tokens > token_budget:
        if token_counter is not None:
            tokens = token_counter.measure(serialize_graph_content((), (), (), True))
            if tokens > token_budget:
                raise ValueError("token budget cannot fit an empty graph payload")
            return OntologyGraphResult((), (), (), True, tokens, "exact", token_counter.identifier)
        return OntologyGraphResult((), (), (), True, 0)
    for depth in range(max_depth):
        next_frontier = []
        for partition, entity, path in frontier:
            candidates = await store.neighbors(
                partitions[partition], (entity,), ontology_id=ontology_id,
                ontology_version=ontology_version, at_time=at_time, predicates=predicates,
                direction=direction, limit=min(1000, max_edges + 1),
            )
            if len(candidates) >= max_edges + 1:
                truncated = True
            for edge in candidates:
                if edge.scope.partition_key() != partition or edge.assertion_id in edge_ids:
                    continue
                destinations = {edge.subject_entity_id}
                if edge.object_entity_id:
                    destinations.add(edge.object_entity_id)
                serialized = json.dumps(asdict(edge), default=str, ensure_ascii=False)
                cost = max(1, (len(serialized) + sum(len(v) + 4 for v in destinations - nodes) + 3) // 4)
                if target_entity is not None:
                    cost += sum(len(v) + 4 for v in (*path, edge.assertion_id)) // 4 + 1
                other = edge.object_entity_id if edge.subject_entity_id == entity else edge.subject_entity_id
                new_path = (*path, edge.assertion_id)
                proposed_paths = [*paths, new_path] if other is not None and target_entity is not None and other == target_entity else paths
                proposed_tokens = (measured(nodes | destinations, [*edges, edge], proposed_paths)
                                   if token_counter is not None else tokens + cost)
                if len(edges) >= max_edges or len(nodes | destinations) > max_nodes or proposed_tokens > token_budget:
                    truncated = True
                    continue
                edges.append(edge)
                edge_ids.add(edge.assertion_id)
                nodes.update(destinations)
                tokens = proposed_tokens
                other = edge.object_entity_id if edge.subject_entity_id == entity else edge.subject_entity_id
                if other is None:
                    continue
                new_path = (*path, edge.assertion_id)
                if target_entity is not None and other == target_entity:
                    paths.append(new_path)
                if (partition, other) not in seen:
                    seen.add((partition, other))
                    next_frontier.append((partition, other, new_path))
        if not next_frontier:
            break
        frontier = next_frontier
        if depth + 1 == max_depth:
            truncated = True
    if token_counter is not None:
        tokens = token_counter.measure(serialize_graph_content(nodes, edges, paths, truncated))
        return OntologyGraphResult(tuple(sorted(nodes)), tuple(edges), tuple(paths), truncated,
                                   tokens, "exact", token_counter.identifier)
    return OntologyGraphResult(tuple(sorted(nodes)), tuple(edges), tuple(paths), truncated, tokens)
