"""Real authenticated HTTP and SDK block provenance contract."""

import asyncio

import httpx
import pytest
from agent_memory_sdk import MCPMemoryClient, MemoryClientError
from test_recovery_http_acceptance import headers, server


@pytest.mark.parametrize("mode", ["archive", "erase"])
def test_http_sdk_updates_keep_claim_dependencies_until_withdrawal(tmp_path, mode):
    async def scenario(url):
        async with httpx.AsyncClient(headers=headers(), trust_env=False) as http:
            async with MCPMemoryClient.from_http(url, http_client=http) as client:
                receipt = await client.ingest("fact", "Mira owns the project", metadata={
                    "claims": [{"key": "owner", "value": "Mira", "text": "Mira owns project"}]
                })
                source = receipt["event_id"]
                claims = receipt["claim_ids"]
                created = (await client.write_block(
                    title="Project owner", content="Mira owns the project", event_ids=[source],
                    source_claim_ids=claims,
                ))["block"]
                assert created["source_claim_ids"] == claims
                updated = (await client.write_block(
                    block_id=created["id"], expected_version=1, title="Renamed project owner",
                    content=created["content"], event_ids=[source],
                ))["block"]
                assert updated["source_claim_ids"] == claims
                with pytest.raises(MemoryClientError):
                    await client.write_block(
                        block_id=created["id"], expected_version=2, title=updated["title"],
                        content=updated["content"], event_ids=[source], source_claim_ids=[],
                    )
                unchanged = (await client.read_block(created["id"]))["block"]
                assert unchanged["version"] == 2
                assert unchanged["source_claim_ids"] == claims
                await client.forget(memory_ids=claims, mode=mode)
                assert (await client.read_block(created["id"]))["block"] is None
                assert not (await client.search_blocks("owner"))["blocks"]

    with server(tmp_path, "blocks") as url:
        asyncio.run(scenario(url))
