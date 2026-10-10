import asyncio
import json
import os
import threading
from pathlib import Path

import pytest
from agent_memory_local_models.models import LocalQwenReranker, artifact_manifest

from agent_memory.retrieval.model_contracts import digest
from agent_memory.retrieval.pair_reranker import FinalTokenPairScorer, PairCandidate


def test_incomplete_and_mutated_artifacts_refuse_before_model_load(tmp_path):
    for name in ("config.json", "tokenizer_config.json", "tokenizer.json"):
        (tmp_path / name).write_text("{}")
    with pytest.raises(ValueError, match="complete local"):
        artifact_manifest(tmp_path)
    (tmp_path / "model.safetensors").write_bytes(b"authored-invalid-weight-file")
    old = digest(artifact_manifest(tmp_path))
    (tmp_path / "tokenizer.json").write_text('{"changed":true}')
    with pytest.raises(ValueError, match="frozen manifest"):
        LocalQwenReranker(tmp_path, expected_manifest_sha256=old)


def test_cancelled_inference_cannot_start_another_worker_before_drain():
    async def run():
        adapter = object.__new__(LocalQwenReranker)
        adapter.spec = "test"
        adapter._lock = asyncio.Lock()
        adapter._inflight = None
        started, finish = threading.Event(), threading.Event()

        def blocked(*_):
            started.set()
            finish.wait(2)
            return ()

        adapter._infer = blocked
        task = asyncio.create_task(adapter.infer("test", "q", ()))
        await asyncio.to_thread(started.wait, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(ValueError, match="busy_after_cancelled"):
            await adapter.infer("test", "q", ())
        finish.set()
        flight = adapter._inflight
        await flight
        await asyncio.sleep(0)
        assert adapter._inflight is None

    asyncio.run(run())


@pytest.fixture(scope="module")
def installed():
    root = os.environ.get("AGENT_MEMORY_LOCAL_RERANKER_DIR")
    if not root:
        pytest.skip("explicit installed model directory required for actual inference")
    import torch

    torch.set_num_threads(4)
    root = Path(root)
    return LocalQwenReranker(
        root, expected_manifest_sha256=digest(artifact_manifest(root)), max_tokens=256
    )


def test_actual_final_token_logits_and_no_silent_truncation(installed):
    async def run():
        scorer = FinalTokenPairScorer(installed.spec, installed.infer)
        candidates = (
            PairCandidate("relevant", "China has its capital in Beijing.", ("authored:1",)),
            PairCandidate("irrelevant", "Bananas are yellow.", ("authored:2",)),
        )
        scores = await scorer.score("What is the capital of China?", candidates)
        assert scores[0].score > scores[1].score
        with pytest.raises(ValueError, match="token_budget_exceeded"):
            await scorer.score(
                "capital", (PairCandidate("huge", "banana " * 1024, ("authored:3",)),)
            )

    asyncio.run(run())


def test_exact_local_rendering_matches_actual_generation_receipt(installed, monkeypatch):
    from hashlib import sha256

    from agent_memory_local_models import models

    from agent_memory.context.model_input_budget import ModelInputBudget, TokenBudgetPort
    from agent_memory.retrieval.model_contracts import (
        ModelConfiguration,
        ModelCoordinates,
        SealedModelInput,
        canonical,
    )

    # Reuse the genuine loaded model, not a simulated tokenizer or provider.
    monkeypatch.setattr(models, "LocalQwenReranker", lambda *_args, **_kwargs: installed)
    spec = ModelInputBudget(
        installed.spec.model_sha256,
        installed.spec.tokenizer_sha256,
        digest([models.LocalTransformerChat.RENDERER, installed.tokenizer.chat_template]),
        256,
        8,
    )
    prompt = "Reply yes or no: Is Beijing the capital of China?"
    cfg = ModelConfiguration(
        provider="transformers-local",
        endpoint="http://127.0.0.1:1",
        account="test",
        region="local",
        processing_policy="authored/1",
        adapter_revision="local-transformers/1",
        model=installed.spec.model,
        model_revision=installed.spec.model_sha256,
        tokenizer_revision=installed.spec.tokenizer_sha256,
        runtime_manifest_sha256=installed.spec.runtime_sha256,
        overflow_guard_sha256=spec.fingerprint,
        options_json=canonical(dict(num_ctx=256, num_predict=8, temperature=0)),
        output_schema_json="{}",
        prompt_revision="1",
        template_sha256=digest(prompt),
        output_revision="1",
        language="en",
        max_input_bytes=4096,
        max_output_bytes=4096,
        timeout_seconds=30,
    )
    port = models.LocalTransformerChat(
        installed.directory, cfg, expected_manifest_sha256=installed.spec.model_sha256
    )
    guarded = TokenBudgetPort(
        port, port.budget, port.count_payload, verify_binding=port.verify_binding
    )
    coords = ModelCoordinates(
        "scope",
        "reader",
        "project",
        "test",
        "reader",
        "q",
        "1",
        "{}",
        "answer",
        digest({}),
        digest({}),
        digest({}),
        "2026-10-01T00:00:00+00:00",
        "2026-10-01T00:00:00+00:00",
    )
    payload = canonical(
        dict(
            model=cfg.model,
            options=json.loads(cfg.options_json),
            think=False,
            stream=False,
            truncate=False,
            shift=False,
            keep_alive=cfg.keep_alive,
            messages=[dict(role="system", content=prompt), dict(role="user", content="yes or no?")],
        )
    )
    manifest = canonical(
        dict(
            schema="model-input-manifest/1",
            sources={},
            payload_sha256=sha256(payload.encode()).hexdigest(),
            configuration_sha256=cfg.fingerprint,
            coordinates_sha256=digest(coords.payload()),
        )
    )
    request = SealedModelInput(cfg, coords, payload, manifest)
    response = asyncio.run(guarded.generate(request))
    assert response.input_tokens == port.count_payload(request.payload_json)
    assert 0 < response.output_tokens <= 8
