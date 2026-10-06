"""Host-supplied query context and bounded, three-valued condition expressions.

No expression evaluation, imports, model assertions, or metadata-based authority.
Natural-language qualifiers stay on the candidate; the host binds their meaning.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .domain import MemoryScope, canonical_json
from .serialization import to_jsonable


def identifier(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError("expected a bounded identifier")


def instant(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("time must include a timezone")
    return value.astimezone(UTC)


def scalar(value):
    return type(value) in (str, int, float, bool) and len(canonical_json(value)) <= 1024


@dataclass(frozen=True, slots=True)
class ContextAttribute:
    name: str
    value: Any
    authority: str

    def __post_init__(self):
        identifier(self.name)
        identifier(self.authority)
        if not scalar(self.value):
            raise ValueError("context attribute must be a bounded JSON scalar")


@dataclass(frozen=True, slots=True)
class QueryContext:
    """Construct only from authenticated host routing, never a model tool payload."""

    principal: str
    scope: MemoryScope
    subject_id: str
    purpose: str
    valid_at: datetime
    known_at: datetime
    attributes: tuple[ContextAttribute, ...] = ()
    timezone: str | None = None
    snapshot_token: str = "host-request"

    def __post_init__(self):
        for value in (self.principal, self.subject_id, self.purpose, self.snapshot_token):
            identifier(value)
        if not isinstance(self.scope, MemoryScope):
            raise ValueError("context requires an exact authenticated scope")
        for name in ("valid_at", "known_at"):
            object.__setattr__(self, name, instant(getattr(self, name)))
        attrs = tuple(self.attributes)
        if len(attrs) > 32 or any(not isinstance(a, ContextAttribute) for a in attrs):
            raise ValueError("context requires at most 32 trusted attributes")
        if len({a.name for a in attrs}) != len(attrs):
            raise ValueError("duplicate context attribute")
        object.__setattr__(self, "attributes", tuple(sorted(attrs, key=lambda a: a.name)))
        if self.timezone is not None:
            identifier(self.timezone)
            try:
                ZoneInfo(self.timezone)
            except ZoneInfoNotFoundError as error:
                raise ValueError("unknown context timezone") from error

    @property
    def context_hash(self):
        return sha256(canonical_json(to_jsonable(self)).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Condition:
    op: str
    attribute: str | None = None
    value: Any = None
    children: tuple["Condition", ...] = ()

    def __post_init__(self):
        children = tuple(c if isinstance(c, Condition) else Condition(**c) for c in self.children)
        object.__setattr__(self, "children", children)
        if self.op in {"and", "or", "not"}:
            if self.attribute is not None or self.value is not None:
                raise ValueError("logical condition only accepts children")
            if not (len(children) == 1 if self.op == "not" else 1 <= len(children) <= 16):
                raise ValueError("invalid logical condition arity")
        elif self.op in {"eq", "ne", "lt", "le", "gt", "ge", "in"}:
            identifier(self.attribute)
            if children:
                raise ValueError("comparison cannot have children")
            if self.op == "in":
                if (
                    not isinstance(self.value, (tuple, list))
                    or not 1 <= len(self.value) <= 16
                    or not all(scalar(v) for v in self.value)
                ):
                    raise ValueError("membership requires bounded scalar values")
                object.__setattr__(self, "value", tuple(self.value))
            elif not scalar(self.value):
                raise ValueError("comparison requires a scalar")
        elif self.op == "weekday":
            if (
                self.attribute is not None
                or children
                or not isinstance(self.value, (tuple, list))
                or not 1 <= len(self.value) <= 7
                or any(type(v) is not int or not 0 <= v <= 6 for v in self.value)
            ):
                raise ValueError("weekday requires Monday=0 through Sunday=6")
            object.__setattr__(self, "value", tuple(sorted(set(self.value))))
        else:
            raise ValueError("unsupported condition operator")

        def size(node, depth=1):
            if depth > 8:
                raise ValueError("condition depth exceeded")
            return 1 + sum(size(c, depth + 1) for c in node.children)

        if size(self) > 64:
            raise ValueError("condition node budget exceeded")

    def evaluate(self, context):
        if self.op in {"and", "or", "not"}:
            values = [c.evaluate(context) for c in self.children]
            if self.op == "not":
                return None if values[0] is None else not values[0]
            if self.op == "and":
                return False if False in values else None if None in values else True
            return True if True in values else None if None in values else False
        if self.op == "weekday":
            return (
                None
                if context.timezone is None
                else context.valid_at.astimezone(ZoneInfo(context.timezone)).weekday() in self.value
            )
        attrs = {a.name: a.value for a in context.attributes}
        if self.attribute not in attrs:
            return None
        left, right = attrs[self.attribute], self.value
        if self.op == "in":
            return any(type(left) is type(v) and left == v for v in right)
        if type(left) is not type(right):
            return None  # bool/int and strings/numbers are never coerced.
        if self.op in {"lt", "le", "gt", "ge"} and type(left) not in (int, float):
            return None
        return {
            "eq": lambda: left == right,
            "ne": lambda: left != right,
            "lt": lambda: left < right,
            "le": lambda: left <= right,
            "gt": lambda: left > right,
            "ge": lambda: left >= right,
        }[self.op]()


def applicability(conditions, exceptions, context):
    values = [c.evaluate(context) for c in conditions]
    exemptions = [c.evaluate(context) for c in exceptions]
    if False in values or True in exemptions:
        return False
    return None if None in values or None in exemptions else True


@dataclass(frozen=True, slots=True)
class ProjectionPolicy:
    revision: str
    purpose: str
    mode: str = "single_exclusive"
    precedence: tuple[tuple[str, str], ...] = ()  # lower -> higher, explicitly approved
    require_domain_revision: bool = False
    require_independent_sources: bool = False

    def __post_init__(self):
        identifier(self.revision)
        identifier(self.purpose)
        if self.mode not in {"single_exclusive", "ordered_override"}:
            raise ValueError("unsupported composition mode")
        edges = tuple(tuple(e) for e in self.precedence)
        if len(edges) > 64 or (edges and self.mode != "ordered_override"):
            raise ValueError("invalid precedence policy")
        graph = {}
        for edge in edges:
            if len(edge) != 2:
                raise ValueError("precedence requires two applicability IDs")
            low, high = edge
            identifier(low)
            identifier(high)
            graph.setdefault(low, set()).add(high)
        visited = set()

        def visit(node, path):
            if node in path:
                raise ValueError("cyclic precedence policy")
            if node in visited:
                return
            for high in graph.get(node, ()):
                visit(high, path | {node})
            visited.add(node)

        for node in graph:
            visit(node, set())
        object.__setattr__(self, "precedence", tuple(sorted(set(edges))))
        if (
            type(self.require_domain_revision) is not bool
            or type(self.require_independent_sources) is not bool
        ):
            raise ValueError("evidence policies must be boolean")

    @property
    def fingerprint(self):
        return sha256(canonical_json(to_jsonable(self)).encode()).hexdigest()
