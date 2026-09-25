"""Optional authenticated Ontology SDK/MCP contract."""
from __future__ import annotations

from datetime import UTC, datetime
from contextlib import asynccontextmanager

from .ontology_queries import traverse_ontology
from .serialization import to_jsonable


class OntologyAPI:
    """One host-configured ontology; scope always comes from MCP identity.

    Schema activation is exposed only when a trusted switch policy is supplied.
    Reads select the registry's active version. The configured store must contain
    that version's prepared index. Optional store_resolver(scope, activation)
    returns an async context manager that pins an index until the query finishes.
    """

    def __init__(self, store, catalog, ontology_id, *, switch_policy=None, store_resolver=None):
        self.store, self.catalog, self.ontology_id = store, catalog, ontology_id
        self.switch_policy, self.store_resolver = switch_policy, store_resolver

    def tools(self):
        definitions = [
            ("status", {}, ()),
            ("search", {"text": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}}, ("text",)),
            ("assertions", {"ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 256}}, ("ids",)),
            ("graph", {
                "start_entity": {"type": "string"}, "target_entity": {"type": "string"},
                "max_depth": {"type": "integer", "minimum": 1, "maximum": 8},
                "max_nodes": {"type": "integer", "minimum": 1, "maximum": 256},
                "max_edges": {"type": "integer", "minimum": 1, "maximum": 512},
                "token_budget": {"type": "integer", "minimum": 1, "maximum": 16000},
                "predicates": {"type": "array", "items": {"type": "string"}, "maxItems": 32},
                "direction": {"enum": ["outgoing", "incoming", "both"]},
            }, ("start_entity",)),
        ]
        if self.switch_policy is not None:
            definitions.append(("switch", {
                "version": {"type": "string"}, "reason": {"type": "string"},
                "expected_generation": {"type": "integer", "minimum": 0},
                "action": {"enum": ["activate", "rollback"]},
            }, ("version", "reason", "expected_generation", "action")))
        return tuple(dict(name="memory_ontology_" + name,
            description="Ontology " + name + "; scope is derived from authenticated identity.",
            inputSchema=dict(type="object", properties=properties, required=list(required), additionalProperties=False))
            for name, properties, required in definitions)

    async def call(self, name, arguments, context):
        from .mcp import MCPToolError
        definition = next((tool for tool in self.tools() if tool["name"] == name), None)
        if definition is None:
            raise MCPToolError("ontology operation is not enabled", code="ontology_unavailable")
        properties = definition["inputSchema"]["properties"]
        if not isinstance(arguments, dict) or set(arguments) - set(properties):
            raise MCPToolError("unknown ontology arguments; scope cannot be overridden")
        if set(definition["inputSchema"]["required"]) - set(arguments):
            raise MCPToolError("missing required ontology arguments")
        for key, value in arguments.items():
            rule = properties[key]
            if rule.get("type") == "string" and (not isinstance(value, str) or not value.strip() or len(value) > 4096):
                raise MCPToolError("invalid string", field=key)
            if rule.get("type") == "integer" and (type(value) is not int or value < rule.get("minimum", 0) or value > rule.get("maximum", 2**63-1)):
                raise MCPToolError("invalid integer", field=key)
            if rule.get("type") == "array" and (not isinstance(value, list) or not rule.get("minItems", 0) <= len(value) <= rule["maxItems"] or any(not isinstance(v, str) or not v.strip() or len(v) > 256 for v in value)):
                raise MCPToolError("invalid identifier array", field=key)
            if "enum" in rule and value not in rule["enum"]:
                raise MCPToolError("invalid enum value", field=key)
        try:
            return await self._call(name.removeprefix("memory_ontology_"), arguments, context)
        except (ValueError, KeyError, LookupError, PermissionError) as error:
            raise MCPToolError(str(error), code="ontology_rejected") from error
        except Exception as error:
            raise MCPToolError("ontology operation failed", code="ontology_unavailable") from error

    async def _call(self, operation, args, context):
        scope = context.scope
        activation = await self.catalog.active(scope, self.ontology_id)
        if operation == "status":
            return dict(active=to_jsonable(activation), versions=list(await self.catalog.versions(scope, self.ontology_id)))
        if operation == "switch":
            action = getattr(self.catalog, args["action"])
            result = await action(scope, self.ontology_id, args["version"],
                expected_generation=args["expected_generation"], reason=args["reason"], authorizer=self.switch_policy)
            return {"active": to_jsonable(result)}
        if activation is None:
            raise LookupError("no active ontology in this scope")
        async with self._store_context(scope, activation) as store:
            return await self._query(store, scope, activation, operation, args)

    @asynccontextmanager
    async def _store_context(self, scope, activation):
        if self.store_resolver is None:
            yield self.store
        else:
            async with self.store_resolver(scope, activation) as store:
                yield store

    async def _query(self, store, scope, activation, operation, args):
        options = dict(ontology_id=self.ontology_id, ontology_version=activation.version, at_time=datetime.now(UTC))
        if operation == "search":
            values = await store.search(args["text"], scope, limit=args.get("limit", 8), max_scan=512, **options)
            if values:
                valid = await store.get_assertions(scope, tuple(value.item.id for value in values), **options)
                allowed = {value.assertion_id for value in valid}
                values = tuple(value for value in values if value.item.id in allowed)
            result = {"matches": to_jsonable(values)}
        elif operation == "assertions":
            result = {"assertions": to_jsonable(await store.get_assertions(scope, tuple(args["ids"]), **options))}
        else:
            result = {"graph": to_jsonable(await traverse_ontology(store, scope, **options, **args))}
        if await self.catalog.active(scope, self.ontology_id) != activation:
            raise ValueError("activation changed during query; retry")
        return result
