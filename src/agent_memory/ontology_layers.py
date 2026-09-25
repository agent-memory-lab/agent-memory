"""Explicit ancestor-layer composition without widening trusted scopes."""
from contextlib import AsyncExitStack, asynccontextmanager

from .domain import ScopeLevel
from .ontology_schema import ontology_schema_digest
from .ontology_runtime import open_active_ontology_memory


def _ancestors(scope):
    result = {scope.partition_key(): scope}
    for level in ScopeLevel:
        try:
            value = scope.project(level)
            result[value.partition_key()] = value
        except ValueError:
            pass
    return result


class LayeredOntologyMemory:
    """Compose explicitly opened managed runtimes from broad to narrow scope.

    Parent activation must have the same schema digest as the child. Missing or
    stale layers fail closed. Functional predicates are shadowed by narrower
    facts even when the narrower text does not match the search. Non-functional
    facts are additive. Current-state Claims remain the host provider's concern.
    """

    def __init__(self, context, catalog, ontology_id, layers):
        self.context, self.catalog, self.ontology_id = context, catalog, ontology_id
        self.layers = tuple(layers)
        if not 1 <= len(self.layers) <= 5:
            raise ValueError("configure between one and five scope layers")
        allowed = _ancestors(context.scope)
        keys = [runtime.context.scope.partition_key() for runtime in self.layers]
        if len(set(keys)) != len(keys) or any(key not in allowed for key in keys):
            raise ValueError("layers must be distinct trusted ancestor scopes")
        self.layers = tuple(sorted(self.layers, key=lambda runtime: sum(
            value is not None for value in (runtime.context.scope.user_id,
                runtime.context.scope.agent_id, runtime.context.scope.workspace_id,
                runtime.context.scope.session_id))))
        for broader, narrower in zip(self.layers, self.layers[1:]):
            if broader.context.scope.partition_key() not in _ancestors(narrower.context.scope):
                raise ValueError("scope layers must form an ancestor chain")
        if any(runtime.ontology_id != ontology_id for runtime in self.layers):
            raise ValueError("all layers must bind the same ontology")

    @asynccontextmanager
    async def borrow_store(self, scope, activation):
        if scope != self.context.scope or self.context.cancelled or self.context.expired:
            raise ValueError("layered runtime scope is invalid or inactive")
        schema = await self.catalog.get(scope, self.ontology_id, activation.version)
        if ontology_schema_digest(schema) != activation.digest:
            raise ValueError("child activation digest mismatch")
        async with AsyncExitStack() as stack:
            stores = []
            for runtime in self.layers:
                parent_scope = runtime.context.scope
                parent = await runtime.catalog.active(parent_scope, self.ontology_id)
                if parent is None or parent.digest != activation.digest:
                    raise ValueError("all configured layers require the same active schema")
                store = await stack.enter_async_context(runtime.borrow_store(parent_scope, parent))
                stores.append((parent_scope, store))
            yield _LayeredView(scope, schema, stores)
            if await self.catalog.active(scope, self.ontology_id) != activation:
                raise ValueError("child activation changed during layered query")

    async def retrieve(self, query, current_state):
        activation = await self.catalog.active(query.scope, self.ontology_id)
        if activation is None:
            raise LookupError("no active ontology")
        async with self.borrow_store(query.scope, activation) as view:
            async with open_active_ontology_memory(self.catalog, self.ontology_id,
                    self.context, view, _ReadOnlyEvidence()) as memory:
                return await memory.recall_pipeline.retrieve(query, current_state)


class _ReadOnlyEvidence:
    async def verify(self, scope, ids):
        return False  # This assembly must never accept projection writes.


class _LayeredView:
    def __init__(self, scope, schema, stores):
        self.scope, self.schema, self.stores = scope, schema, tuple(stores)

    async def initialize(self):
        pass

    async def register_schema(self, schema):
        if ontology_schema_digest(schema) != ontology_schema_digest(self.schema):
            raise ValueError("read-only view cannot register another schema")

    def _select(self, scope, options):
        if scope.partition_key() not in _ancestors(self.scope):
            raise ValueError("query is outside configured ancestry")
        if (options["ontology_id"], options["ontology_version"]) != (self.schema.ontology_id, self.schema.version):
            raise ValueError("query schema differs from pinned layers")
        allowed = _ancestors(scope)
        return [(s, store) for s, store in self.stores if s.partition_key() in allowed]

    async def _visible(self, scope, assertions, options):
        stores = self._select(scope, options)
        priorities = {s.partition_key(): number for number, (s, _) in enumerate(stores)}
        winners = {}
        functional = {p.property_id for p in self.schema.properties if p.functional}
        keys = {(a.subject_entity_id, a.predicate_id) for a in assertions if a.predicate_id in functional}
        # Exact predicate lookups prevent a nonmatching child value leaking a
        # shadowed parent. Fail closed if a bounded lookup might be incomplete.
        for number, (layer_scope, store) in enumerate(stores):
            conflicts = await store.list_conflicts(layer_scope, ontology_id=self.schema.ontology_id,
                ontology_version=self.schema.version, limit=1000)
            if len(conflicts) >= 1000:
                raise ValueError("layer conflict scan exceeded its bound")
            for subject, predicate in keys:
                facts = await store.neighbors(layer_scope, (subject,), predicates=(predicate,),
                    direction="outgoing", limit=1000, **options)
                if len(facts) >= 1000:
                    raise ValueError("layer override scan exceeded its bound")
                exact = [a for a in facts if a.scope == layer_scope]
                conflicting = any(a.scope == layer_scope and a.subject_entity_id == subject
                    and a.predicate_id == predicate and a.valid_from <= options["at_time"]
                    and (a.valid_to is None or options["at_time"] < a.valid_to) for a in conflicts)
                if exact or conflicting:
                    winners[subject, predicate] = (number, conflicting)
        return tuple(a for a in assertions if a.scope.partition_key() in priorities and (
            a.predicate_id not in functional or winners.get((a.subject_entity_id, a.predicate_id))
            == (priorities[a.scope.partition_key()], False)))

    async def get_assertions(self, scope, assertion_ids, **options):
        if not 1 <= len(assertion_ids) <= 256:
            raise ValueError("assertion ID count must be between 1 and 256")
        assertions = []
        for layer_scope, store in self._select(scope, options):
            assertions.extend(a for a in await store.get_assertions(layer_scope, assertion_ids, **options)
                              if a.scope == layer_scope)
        return await self._visible(scope, assertions, options)

    async def neighbors(self, scope, entity_ids, *, limit=100, **options):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("neighbor limit must be between 1 and 1000")
        query = {key: options[key] for key in ("ontology_id", "ontology_version", "at_time")}
        assertions = []
        for layer_scope, store in self._select(scope, query):
            assertions.extend(a for a in await store.neighbors(layer_scope, entity_ids,
                limit=limit, **options) if a.scope == layer_scope)
        return (await self._visible(scope, assertions, query))[:limit]

    async def search(self, text, scope, *, limit=8, max_scan=512, **options):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("search limit must be between 1 and 100")
        matches, assertions = {}, []
        for layer_scope, store in self._select(scope, options):
            values = await store.search(text, layer_scope, limit=limit, max_scan=max_scan, **options)
            if not values:
                continue
            exact = await store.get_assertions(layer_scope, tuple(v.item.id for v in values), **options)
            allowed = {a.assertion_id for a in exact if a.scope == layer_scope}
            assertions.extend(a for a in exact if a.scope == layer_scope)
            matches.update((v.item.id, v) for v in values if v.item.id in allowed)
        visible = await self._visible(scope, assertions, options)
        return tuple(sorted((matches[a.assertion_id] for a in visible),
                            key=lambda value: (-value.score, value.item.id))[:limit])
