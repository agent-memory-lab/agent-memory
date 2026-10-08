from pathlib import Path


def test_core_migration_contains_v3_safety_contracts():
    migration = Path(__file__).parents[1] / "migrations" / "001_core.sql"
    sql = migration.read_text(encoding="utf-8").lower()

    assert "agent_memory_events_idempotency_idx" in sql
    assert "agent_memory_claims_one_active_idx" in sql
    assert "on delete cascade" in sql
    assert "for update skip locked" not in sql
    assert "agent_memory_consolidation_jobs" in sql
    assert "lease_expires_at" in sql
    assert "provenance_json jsonb" in sql


def test_pgvector_is_not_required_by_the_core_migration():
    migrations = Path(__file__).parents[1] / "migrations"
    core = (migrations / "001_core.sql").read_text(encoding="utf-8").lower()
    optional = (migrations / "002_pgvector.sql").read_text(encoding="utf-8").lower()

    assert "create extension" not in core
    assert "create extension if not exists vector" in optional


def test_feedback_migration_is_additive_and_indexed():
    migration = Path(__file__).parents[1] / "migrations" / "003_feedback.sql"
    sql = migration.read_text(encoding="utf-8").lower()

    assert "add column if not exists feedback_status" in sql
    assert "add column if not exists parent_id" in sql
    assert "add column if not exists idempotency_key" in sql
    assert "agent_memory_evolution_idempotency_idx" in sql
    assert "agent_memory_evolution_parent_idx" in sql
    assert "values (2)" in sql


def test_derived_migration_is_additive_and_reverse_indexed():
    sql = (
        (Path(__file__).parents[1] / "migrations" / "015_derived_observations.sql")
        .read_text()
        .lower()
    )
    assert "agent_memory_derived_reverse_idx" in sql
    assert "edge_kind in ('support','processing','query')" in sql
    assert "agent_memory_derived_atom_slot_idx" in sql
    assert "alter table agent_memory_claims" not in sql
    assert "drop table" not in sql


def test_artifact_erasure_migration_is_additive_and_identity_only():
    sql = (Path(__file__).parents[1] / "migrations" / "016_memory_erasure.sql").read_text().lower()
    assert "create table if not exists agent_memory_memory_tombstones" in sql
    assert "primary key (partition_key, memory_table, id)" in sql
    assert "agent_memory_memory_tombstones_scope_idx" in sql
    assert "payload_json" not in sql and "content" not in sql
    assert "drop table" not in sql and "alter table" not in sql
