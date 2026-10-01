"""Compatibility exports for the former combined ontology implementation.

New subsystem code imports model, store or plugins directly.
"""
# ruff: noqa: F401 -- public and historical private re-exports

from .model import (
    CallableOntologyEvidenceVerifier,
    OntologyAssertion,
    OntologyAssertionStatus,
    OntologyClass,
    OntologyConflictResolution,
    OntologyEntity,
    OntologyEvidenceVerifier,
    OntologyMatch,
    OntologyProjection,
    OntologyProperty,
    OntologySchema,
    OntologyStore,
    OntologyValidationError,
    _entity_from_mapping,
    _evidence,
    _identifier,
    _non_empty,
    _schema_payload,
    _validate_projection,
    project_claim_to_ontology,
)
from .plugins import (
    OntologyProjectionConsolidatorPlugin,
    OntologyRetrieverPlugin,
    _OntologyPlugin,
)
from .queries import (
    _assertion_from_row,
)
from .store import (
    SQLiteOntologyStore,
    _intervals_overlap,
    _stored_object,
    _visible_scope_partitions,
)
