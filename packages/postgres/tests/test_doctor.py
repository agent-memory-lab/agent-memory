import asyncio
import os

import pytest

from agent_memory import MemoryDoctorProvider, MemoryScope
from agent_memory_postgres import PostgresMemoryDoctor, PostgresMemoryRepository


def test_postgres_doctor_implements_optional_protocol() -> None:
    class Pool:
        def connection(self):
            raise AssertionError("not used")

    assert isinstance(PostgresMemoryDoctor(Pool()), MemoryDoctorProvider)


def test_live_postgres_doctor_is_scoped_and_read_only() -> None:
    dsn = os.getenv("AGENT_MEMORY_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("AGENT_MEMORY_TEST_POSTGRES_DSN is not configured")

    async def scenario() -> None:
        repository = PostgresMemoryRepository.from_dsn(dsn, min_size=1, max_size=2)
        await repository.initialize()
        try:
            scope = MemoryScope(tenant_id="doctor-contract", session_id="session")
            report = await PostgresMemoryDoctor(repository.pool).inspect(scope)
            assert report.scope_fingerprint is not None
            assert report.schema_version is not None
            assert report.database_bytes > 0
            assert "events" in report.counts
            assert all(len(sample) == 12 for item in report.findings for sample in item.sample_fingerprints)
        finally:
            await repository.close()

    asyncio.run(scenario())
