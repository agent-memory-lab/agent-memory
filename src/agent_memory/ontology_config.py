"""Validated opt-in storage configuration without importing optional drivers."""
from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class OntologyStoreConfig:
    backend: str = "sqlite"
    database_path: str = "ontology.db"
    dsn_env: str = "AGENT_MEMORY_ONTOLOGY_DSN"
    namespace: str = "agent_memory_ontology"

    def __post_init__(self):
        if self.backend not in {"sqlite", "postgres"}:
            raise ValueError("ontology backend must be sqlite or postgres")
        for name in ("database_path", "dsn_env", "namespace"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")

    def create_store(self):
        if self.backend == "sqlite":
            from .ontology_memory import SQLiteOntologyStore
            return SQLiteOntologyStore(self.database_path)
        from importlib import import_module
        try:
            adapter = import_module("agent_memory_postgres").PostgresOntologyStore
        except ImportError as error:
            raise RuntimeError("install agent-memory-postgres to use the postgres backend") from error
        dsn = os.environ.get(self.dsn_env)
        if not dsn:
            raise ValueError("configured ontology DSN environment variable is missing")
        return adapter(dsn, namespace=self.namespace)
