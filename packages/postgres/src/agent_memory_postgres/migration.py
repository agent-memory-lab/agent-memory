"""Shared transaction-scoped startup lock for core and optional schema writers."""

SCHEMA_LOCK_SQL = (
    "SELECT pg_advisory_xact_lock(hashtextextended("
    "%s || current_database() || ':' || current_schema(), 0))"
)
SCHEMA_LOCK_PARAMS = ("agent-memory:migration:",)
