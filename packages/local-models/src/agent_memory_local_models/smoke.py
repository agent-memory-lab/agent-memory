"""Explicit local inference probe over authored examples, not a promotion evaluation."""

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from time import perf_counter

from agent_memory.retrieval.model_contracts import digest
from agent_memory.retrieval.pair_reranker import FinalTokenPairScorer, PairCandidate

from .models import LocalQwenReranker, artifact_manifest


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--device", choices=("cpu", "mps"), default="cpu")
    args = parser.parse_args()
    import torch

    torch.set_num_threads(4)
    start = perf_counter()
    adapter = LocalQwenReranker(
        args.directory,
        expected_manifest_sha256=args.manifest_sha256,
        max_tokens=512,
        device=args.device,
    )
    scorer = FinalTokenPairScorer(adapter.spec, adapter.infer)
    pairs = (
        PairCandidate("relevant", "The capital of China is Beijing.", ("authored:1",)),
        PairCandidate("irrelevant", "Bananas are yellow fruit.", ("authored:2",)),
    )
    scores = asyncio.run(scorer.score("What is the capital of China?", pairs))
    result = dict(
        schema="local-reranker-smoke/1",
        evidence_class="authored_synthetic",
        promotion="not_evaluated",
        costs="unknown",
        spec=asdict(adapter.spec),
        artifact_manifest=artifact_manifest(args.directory),
        scores=[asdict(score) for score in scores],
        elapsed_seconds=perf_counter() - start,
    )
    assert scores[0].score > scores[1].score, "authored probe failed"
    result["sha256"] = digest(result)
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            dict(
                status="passed",
                evidence_class=result["evidence_class"],
                elapsed_seconds=result["elapsed_seconds"],
                scores=result["scores"],
            )
        )
    )


if __name__ == "__main__":
    main()
