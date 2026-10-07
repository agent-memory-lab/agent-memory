"""Host-owned L2 organization contracts, separate from factual facet definitions."""

from dataclasses import dataclass

from ..domain import MemoryScope
from ..serialization import to_jsonable
from .model import DerivedError, digest, identity

PAGE_CONTRACT = "page-full-rebuild/1"
PAGE_TEMPLATE = "language-scenario/1"


@dataclass(frozen=True)
class ScenarioDefinition:
    id: str
    scope: MemoryScope
    subject_id: str
    title: str
    version: str = "1"
    status: str = "open"

    def __post_init__(self):
        for value in (self.id, self.subject_id, self.version):
            identity(value)
        if not isinstance(self.scope, MemoryScope) or self.subject_id != self.scope.user_id:
            raise DerivedError("page_scenario_scope_mismatch")
        if not isinstance(self.title, str) or not self.title.strip() or len(self.title) > 256:
            raise DerivedError("invalid_page_title")
        if self.status not in {"open", "closed"}:
            raise DerivedError("invalid_scenario_status")

    def payload(self):
        return to_jsonable(self)


@dataclass(frozen=True)
class PageDefinition:
    id: str
    scenario: ScenarioDefinition
    parent_facets: tuple[str, ...]
    version: str = "1"
    purpose: str = "agent_context"
    readers: tuple[str, ...] = ("alice",)
    template_version: str = PAGE_TEMPLATE
    authority_id: str | None = None

    def __post_init__(self):
        for value in (self.id, self.version, self.purpose):
            identity(value)
        if not isinstance(self.scenario, ScenarioDefinition):
            raise DerivedError("trusted_scenario_definition_required")
        if self.template_version != PAGE_TEMPLATE:
            raise DerivedError("page_template_unsupported")
        if not isinstance(self.parent_facets, tuple) or not 1 <= len(self.parent_facets) <= 4:
            raise DerivedError("invalid_page_parents")
        for value in self.parent_facets:
            identity(value)
        if len(set(self.parent_facets)) != len(self.parent_facets):
            raise DerivedError("invalid_page_parents")
        if not isinstance(self.readers, tuple) or not 1 <= len(self.readers) <= 16:
            raise DerivedError("invalid_page_readers")
        for value in self.readers:
            identity(value)
        if len(set(self.readers)) != len(self.readers):
            raise DerivedError("invalid_page_readers")
        if self.authority_id is not None:
            identity(self.authority_id)

    def payload(self):
        spec = to_jsonable(self)
        spec.update(resource_kind="page", subject_id=self.scenario.subject_id)
        if self.authority_id is None:
            spec.pop("authority_id")
        return spec


@dataclass(frozen=True)
class PageBlockRevision:
    """Version inherits this full rebuild's entire input manifest, not just a citation."""

    id: str
    block_id: str
    page_id: str
    body: dict
    body_sha256: str
    manifest: dict
    manifest_sha256: str

    def __post_init__(self):
        for value in (self.id, self.block_id, self.page_id):
            identity(value)
        if not isinstance(self.body, dict) or not isinstance(self.manifest, dict) or (
            digest(self.body) != self.body_sha256
            or digest(self.manifest) != self.manifest_sha256
            or self.id != "page-block-version:" + digest([
                self.block_id, self.body_sha256, self.manifest_sha256
            ])
        ):
            raise DerivedError("invalid_page_block_revision")

    def payload(self):
        spec = to_jsonable(self)
        spec.update(schema="page-block-revision/1", facet_id=self.page_id, state="ready")
        return spec
