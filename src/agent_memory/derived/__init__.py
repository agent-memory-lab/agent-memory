"""Opt-in host-controlled derived memory. Ordinary L1 retrieval stays independent."""

from .model import DerivedError, FacetContext, FacetDefinition, FacetRefreshUnit, ProcessingGrant
from .service import ObservationService

__all__ = [
    "DerivedError",
    "FacetDefinition",
    "FacetContext",
    "FacetRefreshUnit",
    "ProcessingGrant",
    "ObservationService",
]
