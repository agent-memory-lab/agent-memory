"""Durable LongMemEval-S worker with a frozen full plan or explicit regression subset."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import tempfile
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import longmemeval_ollama_comparison as lme
import memory_evidence_reader as evidence_reader
import ollama_memory_comparison as native
from longmemeval_claim_extraction import BenchmarkClaimGenerator, record_extraction
from longmemeval_full_reporting import build_report, render_markdown
from longmemeval_retrieval_diagnostics import (
    evidence_diagnostic,
    load_snapshot,
    restore_snapshot,
    retain_snapshot,
    snapshot_key,
)
from longmemeval_run_ledger import RunLedger
from longmemeval_worker_support import (
    load_evaluation_selection,
    package_digest,
    recover_call_log_tails,
    worker_lock,
)
from memory_benchmark_protocol import bounded_evidence, canonical_digest, checked_call
from memory_benchmark_support import ModelCallLedger, OllamaAccountingProxy
from unified_memory_benchmark import (
    ARMS,
    SETTINGS,
    CommonAgentAdapter,
    ControlledClient,
    close_trial_resources,
    write_json,
)

import agent_memory
from agent_memory import MemoryScope
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.composition import build_local_kernel
from agent_memory.retrieval.packing_policy import packing_contract
from agent_memory.evaluation.comparison import GraphitiComparisonAdapter, Mem0ComparisonAdapter
from agent_memory.providers import GeneratedTrajectoryClaimExtractor
from agent_memory.unified_memory import SQLiteDeletionJournal, UnifiedMemory

DATA_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"
ANSWER_PROMPT = (
    "Answer the question using only the retrieved memory. Do not use outside knowledge. "
    "If the memory does not provide enough information, say you do not know. Answer concisely.\n"
)


def parse_judge(text):
    match = re.fullmatch(r"\s*(yes|no)[.!]?\s*", text, re.IGNORECASE)
    return match.group(1).casefold() == "yes" if match else None


async def generate_answer(
    client,
    ledger,
    row,
    *,
    question,
    question_date,
    evidence,
    timeout,
    reader_mode,
    allow_contextual_inference=False,
    reader_thinking=False,
    reader_output_tokens=None,
    evidence_records=None,
):
    """One answer attempt; the interface cannot receive benchmark gold or labels."""
    if reader_mode != "structured" and allow_contextual_inference:
        raise ValueError("contextual inference requires the structured reader")
    if type(reader_thinking) is not bool:
        raise ValueError("reader_thinking must be a boolean")
    if reader_mode not in ("structured", "structured-v3") and (reader_thinking or reader_output_tokens is not None):
        raise ValueError("reader generation options require the structured reader")
    if reader_mode == "structured-v3":
        if evidence_records is None:
            raise ValueError("v3 requires delivered evidence records, not a flattened string")
        result = await checked_call(ledger, row, "answer.question",
            evidence_reader.read_evidence_v3(client, model=native.MODEL, question=question,
                question_date=question_date, records=evidence_records, thinking=reader_thinking,
                max_output_tokens=reader_output_tokens), timeout)
        row["answer_reader"] = result.to_dict()
        return evidence_reader.render_answer(result)
    if reader_mode == "structured":
        result = await checked_call(
            ledger,
            row,
            "answer.question",
            evidence_reader.read_evidence(
                client,
                model=native.MODEL,
                question=question,
                question_date=question_date,
                evidence=evidence,
                allow_contextual_inference=allow_contextual_inference,
                thinking=reader_thinking,
                max_output_tokens=reader_output_tokens,
            ),
            timeout,
        )
        row["answer_reader"] = result.to_dict()
        return evidence_reader.render_answer(result)
    if reader_mode != "direct":
        raise ValueError("unsupported reader mode")
    prompt = ANSWER_PROMPT + (
        f"Question date: {question_date}\nQuestion: {question}\n"
        f"Retrieved memory:\n{evidence or '(no memory retrieved)'}"
    )
    response = await checked_call(
        ledger,
        row,
        "answer.question",
        client.chat(
            model=native.MODEL,
            messages=[{"role": "user", "content": prompt}],
            think=False,
            options={"num_predict": 384},
        ),
        timeout,
    )
    return response.message.content.strip()


def answer_reader_manifest(args):
    """Validate and bind reader settings before any model or filesystem work."""
    mode = getattr(args, "reader_mode", "direct")
    allow_inference = getattr(args, "allow_contextual_inference", False)
    thinking = getattr(args, "reader_thinking", False)
    output_tokens = getattr(args, "reader_output_tokens", None)
    if mode == 'structured-v3':
        if allow_inference:
            raise ValueError('v3 requires explicit support; contextual inference policy belongs to v2')
        policy = evidence_reader.reader_policy_manifest_v3(thinking=thinking, max_output_tokens=output_tokens)
        return {'mode':mode, **policy, 'schema_sha256':canonical_digest(evidence_reader.RESPONSE_SCHEMA_V3),
                'thinking':policy['requested_thinking'], 'max_output_tokens':policy['requested_max_output_tokens']}
    policy = evidence_reader.reader_policy_manifest(
        allow_contextual_inference=allow_inference,
        thinking=thinking,
        max_output_tokens=output_tokens,
    )
    if mode not in ("direct", "structured"):
        raise ValueError("unsupported reader mode")
    if mode != "structured" and (allow_inference or thinking or output_tokens is not None):
        raise ValueError("reader generation options require --reader-mode structured")
    if mode == "direct":
        return {"mode": mode}
    return {
        "mode": mode,
        **policy,
        "schema_sha256": canonical_digest(evidence_reader.RESPONSE_SCHEMA),
        "thinking": policy["requested_thinking"],
        "max_output_tokens": policy["requested_max_output_tokens"],
    }


def source_hashes():
    root = Path(__file__).parent
    names = (
        "longmemeval_full_comparison.py",
        "longmemeval_full_reporting.py",
        "longmemeval_run_ledger.py",
        "longmemeval_ollama_comparison.py",
        "longmemeval_claim_extraction.py",
        "longmemeval_retrieval_diagnostics.py",
        "ollama_memory_comparison.py",
        "unified_memory_benchmark.py",
        "memory_benchmark_protocol.py",
        "memory_benchmark_support.py",
        "longmemeval_worker_support.py",
        "memory_evidence_reader.py",
    )
    return {name: sha256((root / name).read_bytes()).hexdigest() for name in names}


def all_records(output):
    for path in sorted((output / "calls").glob("*.jsonl")):
        with path.open() as stream:
            for line in stream:
                yield json.loads(line)


def publish_progress(output, manifest, tasks, status, started):
    task_summary = tasks.summary()
    report = build_report(
        manifest,
        task_summary,
        all_records(output),
        (datetime.now(UTC) - started).total_seconds(),
        status,
    )
    gaps = (
        json.loads((output / "accounting_gaps.json").read_text())
        if (output / "accounting_gaps.json").exists()
        else []
    )
    persistence_errors = sorted(
        path.name for path in (output / "calls").glob("*.accounting-error.json")
    )
    abandoned = task_summary["abandoned_attempts"]
    incomplete = task_summary["incomplete_accounting_attempts"]
    running = task_summary["running"]
    report["accounting_integrity"] = {
        "quarantined_log_tails": gaps,
        "persistence_error_logs": persistence_errors,
        "abandoned_attempts": abandoned,
        "incomplete_accounting_attempts": incomplete,
        "running_attempts": running,
        "interrupted_usage_may_be_unknown": bool(
            gaps or persistence_errors or abandoned or incomplete or running
        ),
    }
    report["generated_at"] = datetime.now(UTC).isoformat()
    if manifest.get("ingestion_mode") == "snapshot_replay":
        report["limitations"].append(
            "This run replays retained public-fixture SQLite snapshots. Its measured cost "
            "excludes original ingestion; compare recall/answer changes, not total memory cost."
        )
    if report["accounting_integrity"]["interrupted_usage_may_be_unknown"]:
        report["limitations"].append(
            "Unsettled/interrupted attempts, persistence errors or quarantined log tails "
            "may have unrecorded model usage; "
            "recorded totals cannot be treated as the complete experiment cost."
        )
    write_json(output / "progress.json", report)
    (output / "REPORT.md").write_text(render_markdown(report))
    for arm in manifest["arms"]:
        temporary = output / (arm + ".jsonl.tmp")
        with temporary.open("w") as stream:
            for row in tasks.results(arm):
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
        temporary.replace(output / (arm + ".jsonl"))


async def execute_task(item, task, ordinal, output, args):
    row = {
        "trial": ordinal,
        "name": task["arm"],
        "operations": [],
        "question_id": item["question_id"],
        "attempt": task["attempt"],
        "question_type": item["question_type"],
        "question": item["question"],
        "ingested_events": 0,
        "extraction": {} if task["arm"] == "agent-memory" else None,
        "sessions_completed": 0,
        "judge_label": None,
        "cleanup_errors": [],
    }
    ledger = ModelCallLedger(
        output
        / "calls"
        / (
            sha256((item["question_id"] + task["arm"]).encode()).hexdigest()[:24]
            + f"-attempt{task['attempt']}.jsonl"
        )
    )
    started = perf_counter()
    with OllamaAccountingProxy(
        args.upstream, ledger, timeout=args.timeout, max_requests=args.request_budget
    ) as proxy:
        native.OLLAMA_URL = proxy.url
        client = ControlledClient(
            host=proxy.url,
            timeout=args.timeout,
            allow_thinking=getattr(args, "reader_thinking", False),
        )
        local = mem0 = graph = lite = adapter = None
        pid = None
        # Keep the sandbox alive until every created client and DB has closed.
        with tempfile.TemporaryDirectory(prefix="longmemeval-full-") as sandbox:
            try:
                root = Path(sandbox)
                ledger.start_phase(ordinal, task["arm"], "setup")
                calls = {arm: 0 for arm in ARMS}
                mem0, graph, lite = await native._make_arm_clients(
                    root,
                    client,
                    calls,
                    graphiti_max_extraction_items=8,
                    selected_arm=task["arm"],
                )
                pid = getattr(getattr(lite, "client", None), "pid", None)
                if mem0 is not None:
                    original = mem0.llm.client.chat

                    def controlled_mem0_chat(**kwargs):
                        kwargs["options"] = {**kwargs.get("options", {}), **SETTINGS}
                        return original(**kwargs)

                    mem0.llm.client.chat = controlled_mem0_chat
                    mem0.llm.client._client.timeout = args.timeout
                    mem0.embedding_model.client._client.timeout = args.timeout
                    adapter = Mem0ComparisonAdapter(mem0)
                elif graph is not None:
                    graph.embedder.client.timeout = args.timeout
                    adapter = GraphitiComparisonAdapter(
                        graph,
                        previous_episode_context=2,
                        extraction_instructions=(
                            "Extract only facts explicitly stated in this conversation segment. "
                            "Prefer user and assistant details, named people, plans, dates and "
                            "corrections. Skip general world knowledge and advice. Return at "
                            "most eight entities and eight factual relationships."
                        ),
                    )
                else:
                    path = root / "agent.sqlite3"
                    snapshot_source = getattr(args, "snapshot_sources", {}).get(item["question_id"])
                    if snapshot_source is not None:
                        restored = restore_snapshot(
                            snapshot_source,
                            path,
                            item,
                            dataset_sha256=DATA_SHA256,
                            expected_manifest_sha256=args.snapshot_inputs[item["question_id"]][
                                "manifest_sha256"
                            ],
                        )
                        scope = MemoryScope(**restored["scope"])
                        row["snapshot_replay"] = {
                            "source": str(snapshot_source),
                            "manifest_sha256": sha256(
                                (snapshot_source / "snapshot.json").read_bytes()
                            ).hexdigest(),
                            "inventory": restored["inventory"],
                            "source_ingestion": restored["ingestion"],
                        }
                        row["sessions_completed"] = len(item["haystack_sessions"])
                    else:
                        scope = MemoryScope(
                            "longmemeval-full", user_id=uuid4().hex, session_id=uuid4().hex
                        )
                    local = UnifiedMemory(
                        build_local_kernel(
                            path,
                            packing_version=getattr(args, "packing_version", "v2"),
                            extractor=GeneratedTrajectoryClaimExtractor(
                                BenchmarkClaimGenerator(client, calls, model=native.MODEL),
                                provider="ollama",
                                model=native.MODEL,
                                event_types=(
                                    "user.message",
                                    "agent.model.completed",
                                    "agent.tool.completed",
                                ),
                                fail_open=False,
                            ),
                            embedding_provider=native.OllamaEmbeddings(client),
                        ),
                        scope,
                        journal=SQLiteDeletionJournal(str(path) + ".deletions.db"),
                        sanitizer=CaptureSanitizer(redact_builtin=False),
                    )
                    await local.initialize()
                    adapter = CommonAgentAdapter(local)
                for index, (session, timestamp) in enumerate(
                    zip(
                        item["haystack_sessions"],
                        item["haystack_dates"],
                        strict=True,
                    )
                ):
                    if "snapshot_replay" in row:
                        break
                    for event in lme._session_event(item["question_id"], index, session, timestamp):
                        submission = await checked_call(
                            ledger, row, "add." + event.event_id, adapter.add(event), args.timeout
                        )
                        if row["extraction"] is not None:
                            record_extraction(row["extraction"], submission)
                        if getattr(submission, 'provider_event_id', None):
                            row.setdefault('reader_source_metadata', {})[submission.provider_event_id] = {
                                'source_id':f'session-{index:04d}', 'occurred_at':event.occurred_at.isoformat(),
                                'roles':sorted({turn['role'] for turn in session}), 'speaker_scope':'mixed_transcript'}
                        row["ingested_events"] += 1
                    row["sessions_completed"] = index + 1
                    write_json(
                        output / "current_task.json",
                        {
                            **task,
                            "session_completed": index + 1,
                            "session_total": len(item["haystack_sessions"]),
                            "ingested_events": row["ingested_events"],
                            "extraction": row["extraction"],
                            "updated_at": datetime.now(UTC).isoformat(),
                            "model_usage": ledger.summary(),
                        },
                    )
                    print(
                        f"question={item['question_id']} arm={task['arm']} "
                        f"attempt={task['attempt']} session={index + 1}/"
                        f"{len(item['haystack_sessions'])}",
                        flush=True,
                    )
                if local is not None and getattr(args, "retain_agent_snapshots", False):
                    artifact = (
                        output
                        / "agent_snapshots"
                        / snapshot_key(item["question_id"])
                        / f"attempt-{task['attempt']}"
                    )
                    # A separate public-fixture diagnostic copy; normal trial clear/close
                    # still runs in finally. Never expose this as a production export API.
                    retained = retain_snapshot(
                        artifact,
                        path,
                        local.scope,
                        item,
                        dataset_sha256=DATA_SHA256,
                        provenance=args.snapshot_provenance,
                        ingestion={
                            "ingested_events": row["ingested_events"],
                            "sessions_completed": row["sessions_completed"],
                            "extraction": row["extraction"],
                            "model_usage": ledger.summary(),
                        },
                    )
                    row["retained_snapshot"] = {
                        "path": str(artifact.relative_to(output)),
                        "inventory": retained["inventory"],
                    }
                memories = await checked_call(
                    ledger,
                    row,
                    "search.question",
                    adapter.search(item["question"], limit=8),
                    args.timeout,
                )
                if local is not None:
                    row["retrieval_diagnostics"] = adapter.last_retrieval_diagnostics
                evidence_records = None
                if getattr(args, 'reader_mode', 'direct') == 'structured-v3':
                    evidence_records = evidence_reader.records_from_memories_v3(memories,
                        source_metadata=row.get('reader_source_metadata', {}))
                    evidence, _, row['reader_delivery'] = evidence_reader.labeled_evidence_v3(evidence_records)
                    row['answer_evidence_records'] = evidence_records
                else:
                    evidence = bounded_evidence(memories)
                if local is not None and evidence_records is None:
                    row["retrieval_diagnostics"]["answer_evidence"] = evidence_diagnostic(
                        memories, evidence
                    )
                # Gold, question_type, answer_session_ids and has_answer never
                # enter extraction, retrieval or shared answer generation.
                row["retrieved"] = memories
                row["answer_evidence"] = evidence
                row["answer"] = await generate_answer(
                    client,
                    ledger,
                    row,
                    question=item["question"],
                    question_date=item["question_date"],
                    evidence=evidence,
                    timeout=args.timeout,
                    reader_mode=getattr(args, "reader_mode", "direct"),
                    allow_contextual_inference=getattr(args, "allow_contextual_inference", False),
                    reader_thinking=getattr(args, "reader_thinking", False),
                    reader_output_tokens=getattr(args, "reader_output_tokens", None),
                    evidence_records=evidence_records,
                )
                judge_prompt = lme._judge_prompt(
                    item["question_type"],
                    item["question"],
                    item["answer"],
                    row["answer"],
                    item["question_id"].endswith("_abs"),
                )
                judged = await checked_call(
                    ledger,
                    row,
                    "judge.question",
                    client.chat(
                        model=native.MODEL,
                        think=False,
                        messages=[{"role": "user", "content": judge_prompt}],
                        options={"num_predict": 16},
                    ),
                    args.timeout,
                )
                row["judge_output"] = judged.message.content.strip()
                row["judge_label"] = parse_judge(row["judge_output"])
                row["gold_answer"] = item["answer"]
                row["status"] = "completed"
            except (Exception, asyncio.CancelledError) as error:
                row["cleanup_errors"].extend(getattr(error, "comparison_cleanup_errors", ()))
                if row["extraction"] is not None and getattr(error, "extraction", None) is not None:
                    record_extraction(row["extraction"], error)
                row["status"] = "aborted" if isinstance(error, asyncio.CancelledError) else "failed"
                row["error_type"] = type(error).__name__
                if isinstance(error, asyncio.CancelledError):
                    raise
            finally:
                cleanup_cancelled = None
                if adapter is not None:
                    try:
                        await checked_call(ledger, row, "cleanup", adapter.clear(), args.timeout)
                    except (Exception, asyncio.CancelledError) as error:
                        if isinstance(error, asyncio.CancelledError):
                            cleanup_cancelled = error
                        row["cleanup_errors"].append(
                            {"resource": "adapter-clear", "error_type": type(error).__name__}
                        )
                row["accounting_complete"] = await asyncio.to_thread(proxy.drain, args.timeout)
                row["cleanup_errors"].extend(
                    await close_trial_resources(local, mem0, graph, lite, pid)
                )
                await client._client.aclose()
                row["accounting_complete"] &= await asyncio.to_thread(proxy.drain, args.timeout)
                row["pending_requests"] = proxy.pending_requests
                row["model_usage"] = ledger.summary()
                row["seconds_including_setup_cleanup"] = perf_counter() - started
                if row["cleanup_errors"]:
                    row["original_error_type"] = row.get("error_type")
                    row["status"] = "failed"
                    row["error_type"] = "ResourceCleanupFailure"
                if cleanup_cancelled is not None:
                    raise cleanup_cancelled
    return row


async def run(args):
    reader_manifest = answer_reader_manifest(args)
    packing_manifest = packing_contract(getattr(args, "packing_version", "v2"))
    selection = load_evaluation_selection(
        args.data,
        DATA_SHA256,
        ARMS,
        arms=getattr(args, "arms", None),
        question_ids=getattr(args, "question_ids", None),
    )
    dataset = selection["items"]
    retain = getattr(args, "retain_agent_snapshots", False)
    replay = getattr(args, "replay_agent_snapshots", None)
    if (retain or replay) and selection["arms"] != ["agent-memory"]:
        raise ValueError("diagnostic snapshots require --arms agent-memory only")
    if retain and replay:
        raise ValueError("retaining and replaying snapshots are separate experiments")
    args.snapshot_sources = {}
    snapshot_inputs = {}
    if replay:
        replay = Path(replay).resolve()
        if args.output_dir.resolve() == replay.parent or args.output_dir.resolve().is_relative_to(
            replay
        ):
            raise ValueError("snapshot replay requires a separate output directory")
        for item in dataset:
            choices = [
                path.parent
                for path in (replay / snapshot_key(item["question_id"])).glob(
                    "attempt-*/snapshot.json"
                )
                if re.fullmatch(r"attempt-[1-9][0-9]*", path.parent.name)
            ]
            if not choices:
                raise ValueError("a selected question has no complete ingestion snapshot")
            source = max(choices, key=lambda path: int(path.name.removeprefix("attempt-")))
            load_snapshot(source, item, dataset_sha256=DATA_SHA256)
            args.snapshot_sources[item["question_id"]] = source
            snapshot_inputs[item["question_id"]] = {
                "path": str(source),
                "manifest_sha256": sha256((source / "snapshot.json").read_bytes()).hexdigest(),
            }
    args.snapshot_inputs = snapshot_inputs
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "calls").mkdir(exist_ok=True)
    metadata = ModelCallLedger(args.output_dir / "calls" / "metadata.jsonl")
    with OllamaAccountingProxy(args.upstream, metadata, timeout=args.timeout) as proxy:
        metadata.start_phase(0, "unassigned", "metadata")
        probe = ControlledClient(host=proxy.url, timeout=args.timeout)
        try:
            tags = await probe.list()
            expected = {native.MODEL, native.EMBED_MODEL + ":latest"}
            models = [
                item.model_dump(mode="json") for item in tags.models if item.model in expected
            ]
            if {model["model"] for model in models} != expected:
                raise ValueError("both frozen generation and embedding models must be cached")
            version = (await probe._client.get("/api/version")).json()
        finally:
            await probe._client.aclose()
    manifest = {
        "schema_version": 1,
        "dataset": "official-cleaned-LongMemEval-S",
        "dataset_sha256": DATA_SHA256,
        **{key: value for key, value in selection.items() if key != "items"},
        "source_commit": args.source_commit
        or subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "core_source_sha256": package_digest(Path(agent_memory.__file__).parent),
        "runner_sha256": source_hashes(),
        "settings": {**SETTINGS, "thinking": False},
        "settings_scope": (
            "Extraction and judge defaults; answer_reader overrides thinking and "
            "max_output_tokens for answer generation."
        ),
        "generation_model": native.MODEL,
        "embedding_model": native.EMBED_MODEL,
        "judge_model": native.MODEL,
        "models": models,
        "ollama_version": version,
        "dependency_versions": {
            d.metadata["Name"]: d.version for d in importlib.metadata.distributions()
        },
        "python": platform.python_version(),
        "python_hash_seed": os.environ.get("PYTHONHASHSEED"),
        "platform": platform.platform(),
        "answer_prompt_sha256": canonical_digest(
            evidence_reader.READER_PROMPT_V3 if getattr(args, "reader_mode", "direct") == "structured-v3" else
            evidence_reader.policy_prompt(getattr(args, "allow_contextual_inference", False))
            if getattr(args, "reader_mode", "direct") == "structured"
            else ANSWER_PROMPT
        ),
        "answer_reader": reader_manifest,
        "packing_contract": packing_manifest,
        "top_k": 8,
        "evidence_utf8_byte_budget": 8192,
        "agent_internal_estimated_token_budget": CommonAgentAdapter.packing_token_budget(),
        "agent_embedding_input_contract": {
            "version": native.OllamaEmbeddings.retrieval_prefix_version,
            "roles": ["query", "document"],
            "truncate": False,
        },
        "raw_chunk_utf8_bytes": 9000,
        "graphiti_max_items": 8,
        "graphiti_previous_episodes": 2,
        "graphiti_generation_limits": [8192, 16384],
        "operation_timeout_seconds": args.timeout,
        "request_budget_per_task": args.request_budget,
        "max_attempts": args.max_attempts,
        "judge_protocol": (
            "Official yes/no templates with strict local Qwen decoding; not official GPT-4o scores"
        ),
        "execution_order": "original dataset order, selected arms in manifest order per question",
        "raw_session_order": "original array order; no gold-selected evidence",
        "adapter_path": "generic UnifiedMemory, not V7.2 MemoryHost business acceptance",
        "ingestion_mode": "snapshot_replay" if replay else "fresh_extraction",
        "retain_agent_snapshots": bool(retain),
        "snapshot_inputs": snapshot_inputs,
    }
    args.snapshot_provenance = {
        key: manifest[key]
        for key in ("core_source_sha256", "runner_sha256", "source_commit", "settings", "models")
    }
    with RunLedger(
        args.output_dir / "run.sqlite3",
        manifest,
        [item["question_id"] for item in dataset],
        manifest["arms"],
        max_attempts=args.max_attempts,
    ) as tasks:
        tasks.recover()
        write_json(args.output_dir / "manifest.json", manifest)
        snapshot = args.output_dir / "runner_snapshot"
        snapshot.mkdir(exist_ok=True)
        for name in manifest["runner_sha256"]:
            (snapshot / name).write_bytes((Path(__file__).parent / name).read_bytes())
        lookup = {item["question_id"]: (index + 1, item) for index, item in enumerate(dataset)}
        execution_path = args.output_dir / "execution.json"
        if execution_path.exists():
            execution = json.loads(execution_path.read_text())
            started = datetime.fromisoformat(execution["started_at"])
        else:
            started = datetime.now(UTC)
            execution = {
                "started_at": started.isoformat(),
                "elapsed_semantics": "wall time including interruptions",
            }
        # A display snapshot only; reports always derive this from SQLite. A
        # crash after recover commits cannot erase abandoned attempt history.
        execution["abandoned_attempts"] = tasks.summary()["abandoned_attempts"]
        write_json(execution_path, execution)
        publish_progress(args.output_dir, manifest, tasks, "running", started)
        try:
            while task := tasks.next_task():
                index, item = lookup[task["question_id"]]
                print(
                    f"TASK question={task['question_id']} arm={task['arm']} "
                    f"attempt={task['attempt']}",
                    flush=True,
                )
                publish_progress(args.output_dir, manifest, tasks, "running", started)
                write_json(
                    args.output_dir / "current_task.json",
                    {
                        **task,
                        "status": "running",
                        "session_completed": 0,
                        "session_total": len(item["haystack_sessions"]),
                        "updated_at": datetime.now(UTC).isoformat(),
                    },
                )
                result = await execute_task(item, task, index, args.output_dir, args)
                write_json(
                    args.output_dir / "current_task.json",
                    {
                        **task,
                        "status": result["status"],
                        "session_completed": result["sessions_completed"],
                        "session_total": len(item["haystack_sessions"]),
                        "updated_at": datetime.now(UTC).isoformat(),
                        "model_usage": result["model_usage"],
                    },
                )
                if not result["accounting_complete"]:
                    tasks.fail(
                        task["question_id"], task["arm"], "IncompleteModelAccounting", result
                    )
                    raise RuntimeError("cannot continue with incomplete model accounting")
                if result["status"] == "completed":
                    tasks.complete(task["question_id"], task["arm"], result)
                else:
                    tasks.fail(task["question_id"], task["arm"], result["error_type"], result)
                if result["cleanup_errors"]:
                    raise RuntimeError("resource cleanup failed; stop before the next task")
                publish_progress(args.output_dir, manifest, tasks, "running", started)
                print(json.dumps(tasks.summary(), ensure_ascii=False), flush=True)
        except (Exception, asyncio.CancelledError):
            publish_progress(args.output_dir, manifest, tasks, "interrupted", started)
            raise
        summary = tasks.summary()
        status = "completed_with_failures" if summary["failed"] else "completed"
        publish_progress(args.output_dir, manifest, tasks, status, started)
        print(f"FINISHED status={status}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--upstream", default="http://127.0.0.1:11434")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--request-budget", type=int, default=10000)
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--packing-version", choices=("v2", "v3", "v4", "v5"), default="v2",
                        help="Stable v2 or experimental v3/v4/v5, independent of --reader-mode")
    parser.add_argument(
        "--reader-mode",
        choices=("direct", "structured", "structured-v3"),
        default="direct",
        help="direct/structured preserve v2; structured-v3 binds quotes to source-labelled blocks",
    )
    parser.add_argument(
        "--allow-contextual-inference",
        action="store_true",
        help="Structured reader may return explicitly labelled, quoted contextual inferences",
    )
    parser.add_argument(
        "--reader-thinking",
        action="store_true",
        help="Enable thinking only for the structured answer reader; extraction and judge stay off",
    )
    parser.add_argument(
        "--reader-output-tokens",
        type=int,
        help="Structured reader output budget 1..32768; default 384, or 2048 with thinking",
    )
    parser.add_argument("--source-commit", help="Baseline Git commit for a frozen worker snapshot")
    parser.add_argument(
        "--arms",
        nargs="+",
        choices=ARMS,
        help="Selected framework arms; default runs all three frameworks",
    )
    parser.add_argument(
        "--question-ids",
        nargs="+",
        help="Explicit regression question IDs from the frozen 500-question source",
    )
    diagnostic = parser.add_mutually_exclusive_group()
    diagnostic.add_argument(
        "--retain-agent-snapshots",
        action="store_true",
        help="Retain public benchmark SQLite fixtures after ingestion for local diagnostics",
    )
    diagnostic.add_argument(
        "--replay-agent-snapshots",
        type=Path,
        help="Replay latest complete attempts from another run's agent_snapshots",
    )
    args = parser.parse_args()
    if args.allow_contextual_inference and args.reader_mode != "structured":
        parser.error("contextual inference requires --reader-mode structured")
    try:
        answer_reader_manifest(args)
    except ValueError as error:
        parser.error(str(error))
    if not isfinite(args.timeout) or args.timeout <= 0 or not 1 <= args.request_budget <= 10000:
        parser.error("positive timeout and request budget 1..10000 required")
    with worker_lock(args.output_dir):
        recover_call_log_tails(args.output_dir)
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
