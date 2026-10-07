"""Opt-in host-controlled derived memory. Ordinary L1 retrieval stays independent."""

from .contracts import HistoricalQuery, HostGrantAuthority, QueryDefinition
from .model import DerivedError, FacetContext, FacetDefinition, FacetRefreshUnit, ProcessingGrant
from .page_model import PageBlockRevision, PageDefinition, ScenarioDefinition
from .service import ObservationService

__all__ = [
    "DerivedError",
    "FacetDefinition",
    "FacetContext",
    "FacetRefreshUnit",
    "ProcessingGrant",
    "ObservationService",
    "HostGrantAuthority",
    "QueryDefinition",
    "HistoricalQuery",
    "ScenarioDefinition",
    "PageDefinition",
    "PageBlockRevision",
]
