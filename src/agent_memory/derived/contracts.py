"""Host control contracts, independent of storage and Observation renderers."""

from dataclasses import dataclass
from datetime import datetime

from ..domain import MemoryScope
from ..serialization import to_jsonable
from .model import DerivedError, identity, timestamp


def audience(values):
    if not isinstance(values, tuple) or not 1 <= len(values) <= 16:
        raise DerivedError("invalid_derived_audience")
    for value in values:
        identity(value)
    if len(set(values)) != len(values):
        raise DerivedError("invalid_derived_audience")


@dataclass(frozen=True)
class QueryDefinition:
    """Complete current census of candidate slots, including rejected counterexamples.

    No acceptance, value, confidence, grant or top-k filter can reduce membership.
    Consumers must explicitly support every predicate in this definition.
    """

    id: str
    scope: MemoryScope
    subject_id: str
    predicates: tuple[str, ...]
    version: str = "1"
    schema: str = "derived-query/1"
    time_mode: str = "current"
    membership: str = "all_candidates"

    def __post_init__(self):
        for value in (self.id, self.subject_id, self.version):
            identity(value)
        if not isinstance(self.scope, MemoryScope) or self.subject_id != self.scope.user_id:
            raise DerivedError("derived_query_scope_mismatch")
        audience(self.predicates)
        if (
            self.schema != "derived-query/1"
            or self.time_mode != "current"
            or self.membership != "all_candidates"
        ):
            raise DerivedError("unsupported_derived_query")

    def payload(self):
        return to_jsonable(self)


@dataclass(frozen=True)
class HostGrantAuthority:
    """Expiring local host authority; every replacement needs CAS and fresh grants.

    The trusted host commits an ACL revision here before applying that revision.
    This contract does not attest to an unsynchronized remote ACL.
    """

    id: str
    readers: tuple[str, ...]
    expires_at: datetime
    purposes: tuple[str, ...] = ("agent_context",)
    revoked: bool = False
    schema: str = "host-grant-authority/1"

    def __post_init__(self):
        identity(self.id)
        audience(self.readers)
        audience(self.purposes)
        timestamp(self.expires_at)
        if type(self.revoked) is not bool or self.schema != "host-grant-authority/1":
            raise DerivedError("invalid_derived_authority")

    def payload(self):
        return to_jsonable(self)
