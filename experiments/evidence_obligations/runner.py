"""Bounded research only. No production registration, defaults or automatic promotion."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import hashlib
import json
import os
import platform
import random
import re
import sys
import time
import warnings
from dataclasses import asdict
from pathlib import Path

from .core_bridge import QuoteStore
from .dataset import Query, SourceEvent, build_dataset

BASELINE_SHA = "2cd3a74bf0e1c103f934a82028696890976e78b1"
BASELINE_TREE = "4a3bd9530371973a07943cea1ab44049aa7d13a3"
ARMS = (
    "baseline",
    "wider_recall",
    "per_query_gap",
    "repaired_atoms",
    "persistent_obligations",
    "no_cross_query_sharing",
    "no_source_invalidation",
    "ordinary_cache",
)
GAP_ARMS = set(ARMS[2:])
PERSIST_ATOMS = set(ARMS[3:])
PERSIST_GAPS = set(ARMS[4:])
SEED = 17
LIMITS = dict(
    sequence_tokens=40000,
    sequence_calls=80,
    source_reads_per_query=8,
    context_chars=12000,
    atom_candidates=4,
    raw_candidates=4,
)
PROMPTS = {
    ("extract"): (
        "Select at most eight useful verbatim quotes from these documents for"
        " future questions. Preserve complete conditions and exceptions. "
        'Return JSON {"quotes":[{"source_id":"...","quote":"exact '
        'substring"}]}. No invented facts.'
    ),
    ("gap"): (
        "Identify evidence still missing to answer the question faithfully, "
        "including conditions, exceptions and time. Only use supplied "
        'evidence. Return JSON {"missing":["short unresolved '
        'requirement"],"search_terms":"short retrieval query"}. missing=[] '
        "only if sufficient. Do not answer. Previous questions are context, "
        "not evidence."
    ),
    ("answer"): (
        "Answer only using supplied evidence. Preserve all applicable "
        "conditions, exceptions and time. If evidence is insufficient, "
        'abstain. Return JSON {"answer":"...","abstain":true or '
        'false,"quotes":[{"source_id":"...","quote":"exact supporting '
        'substring"}]}. No unsupported claim. Cite verbatim quotes that '
        "support the answer, including qualifiers."
    ),
}


def canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf8") as stream:
        stream.write(canonical(value) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def tokens(text):
    return set(re.findall(r"[a-z0-9]+", text.lower())) - {
        "the",
        "a",
        "an",
        "is",
        "for",
        "to",
        "of",
        "and",
        "what",
        "which",
        "can",
        "it",
        "does",
    }


def similarity(left, right):
    a, b = tokens(left), tokens(right)
    return len(a & b) / max(1, len(a | b))


def ranked_sources(sources, query, limit):
    return sorted(
        sources.values(), key=lambda s: (-len(tokens(s.text) & tokens(query)), s.source_id)
    )[:limit]


def source_payload(source):
    return dict(source_id=source.source_id, revision=source.revision, text=source.text)


def extract_json(content):
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("model output must be an object")
    return value


class SequenceModel:
    """Arm-local budget; global client independently reserves durable hard ceilings."""

    def __init__(self, client):
        self.client, self.receipts = client, []
        self.used_tokens = 0

    def call(self, phase, payload, maximum=512):
        messages = [
            {"role": "system", "content": PROMPTS[phase]},
            {"role": "user", "content": canonical(payload)},
        ]
        from .model_client import MODEL, PER_MESSAGE_OVERHEAD_TOKENS, PROTOCOL_OVERHEAD_TOKENS

        payload = dict(
            model=MODEL,
            messages=messages,
            temperature=0,
            enable_thinking=False,
            stream=False,
            max_tokens=maximum,
        )
        bound = (
            len(canonical(payload).encode())
            + PROTOCOL_OVERHEAD_TOKENS
            + PER_MESSAGE_OVERHEAD_TOKENS * len(messages)
            + maximum
        )
        if (
            len(self.receipts) >= LIMITS["sequence_calls"]
            or self.used_tokens + bound > LIMITS["sequence_tokens"]
        ):
            raise RuntimeError("sequence_budget_exhausted")
        receipt = self.client.complete(messages, max_output_tokens=maximum, phase=phase)
        self.receipts.append(
            dict(phase=phase, **{k: v for k, v in receipt.items() if k not in {"content", "phase"}})
        )
        self.used_tokens += receipt["input_tokens"] + receipt["output_tokens"]
        return extract_json(receipt["content"])


class ObligationState:
    """Unresolved requirements have explicit frontier-bound lifecycle; never evidence."""

    def __init__(self, arm):
        self.arm, self.records, self.transitions = arm, [], []
        self.frontier = None

    def source_changed(self, frontier, *, purge=False):
        self.frontier = frontier
        if purge:
            self.transitions.append("purged_after_deletion_or_revocation")
            self.records.clear()
        elif self.arm != "no_source_invalidation":
            for record in self.records:
                record["state"] = "reopened"
                record.pop("answer", None)
            self.transitions.append("reopened_after_source_change")

    def match(self, query):
        for record in reversed(self.records):
            match = (
                record["query"] == query
                if self.arm == "no_cross_query_sharing"
                else similarity(record["query"], query) >= 0.3
            )
            if match and record["state"] != "reopened":
                return record
        return None

    def get(self, query):
        record = self.match(query)
        if record is None:
            return None
        return (
            dict(missing=[], search_terms="")
            if record["state"] == "resolved"
            else dict(record["gap"])
        )

    def save(self, query, gap):
        self.records = [r for r in self.records if r["query"] != query]
        self.records.append(
            dict(
                query=query,
                gap=gap,
                state="unresolved" if gap["missing"] else "resolved",
                frontier=self.frontier,
            )
        )
        self.records = self.records[-16:]

    def resolve(self, query, output, valid):
        record = next((r for r in reversed(self.records) if r["query"] == query), None)
        if record is not None:
            record["state"] = "resolved" if valid and not output["abstain"] else "unresolved"
            self.transitions.append(
                record["state"] + ":nonabstention_and_valid_quote_operational_signal_only"
            )
            if self.arm == "ordinary_cache" and valid:
                record["answer"] = output


class EpisodeFailure(RuntimeError):
    def __init__(self, record):
        super().__init__("episode_failed_or_censored")
        self.record = record


async def run_episode(episode, arm, client, work, checkpoint_dir=None):
    """Runtime receives Episode only; no gold, task slots or oracle relevance."""
    model, hints = SequenceModel(client), ObligationState(arm)
    sources = {source.source_id: source for source in episode.initial_sources}
    store = QuoteStore(work / "memory.sqlite", namespace=episode.episode_id + ":" + arm)
    await store.initialize()
    history, rows = [], []
    hints.frontier = digest([source_payload(s) for s in sources.values()])
    revoked = set()
    start = time.perf_counter()
    counts = dict(
        initial_quotes=0,
        refresh_quotes=0,
        repair_quotes=0,
        raw_source_reads=0,
        gap_calls=0,
        gap_reuses=0,
        invalid_quotes=0,
    )

    async def save_quotes(proposals, allowed):
        saved = 0
        if not isinstance(proposals, list) or len(proposals) > 8:
            raise ValueError("invalid quote collection")
        for proposal in proposals:
            if not isinstance(proposal, dict) or set(proposal) != {"source_id", "quote"}:
                raise ValueError("invalid quote proposal")
            source = allowed.get(proposal["source_id"])
            quote = proposal["quote"]
            if (
                source is None
                or not isinstance(quote, str)
                or not quote
                or len(quote) > 1000
                or quote not in source.text
            ):
                counts["invalid_quotes"] += 1
                continue
            saved += await store.admit(source.source_id, str(source.revision), source.text, [quote])
        return saved

    try:
        initial = model.call(
            "extract", {"documents": [source_payload(s) for s in sources.values()]}, 700
        )
        counts["initial_quotes"] += await save_quotes(initial.get("quotes"), sources)
        for step in episode.steps:
            if isinstance(step, SourceEvent):
                await store.revoke(step.source_id)
                sources.pop(step.source_id, None)
                if step.kind == "upsert":
                    sources[step.source_id] = step.source
                    refreshed = model.call(
                        "extract", {"documents": [source_payload(step.source)]}, 700
                    )
                    counts["refresh_quotes"] += await save_quotes(
                        refreshed.get("quotes"), {step.source_id: step.source}
                    )
                if step.kind in {"delete", "revoke"}:
                    revoked.add(step.source_id)
                else:
                    revoked.discard(step.source_id)
                hints.source_changed(
                    digest([source_payload(s) for s in sources.values()]),
                    purge=step.kind in {"delete", "revoke"},
                )
                continue
            if not isinstance(step, Query):
                raise TypeError("unsupported episode step")
            if arm == "no_cross_query_sharing":
                hints.records.clear()  # No state reuse across any query event, including repeats.
            before_tokens, before_calls = model.used_tokens, len(model.receipts)
            query_start = time.perf_counter()
            observation_path = (
                checkpoint_dir / ("observation-" + step.query_id + ".json")
                if checkpoint_dir is not None
                else None
            )
            observation_binding = digest(dict(query=step.text, frontier=hints.frontier))
            if observation_path is not None and observation_path.exists():
                observation = json.loads(observation_path.read_text())
                checksum = observation.pop("checksum", None)
                if checksum != digest(observation) or observation["binding"] != observation_binding:
                    raise ValueError("query observation checkpoint changed")
                atoms = observation["atoms"]
            else:
                atoms = await store.search(step.text, limit=LIMITS["atom_candidates"])
                atoms = [{**a, "revision": int(a["revision"])} for a in atoms]
                if observation_path is not None:
                    observation = dict(binding=observation_binding, atoms=atoms)
                    observation["checksum"] = digest(observation)
                    atomic_json(observation_path, observation)
            evidence = [
                dict(source_id=a["source_id"], revision=a["revision"], text=a["quote"])
                for a in atoms
                if a["source_id"] in sources and sources[a["source_id"]].revision == a["revision"]
            ]
            gap = None
            reused = False
            cached_record = hints.match(step.text) if arm == "ordinary_cache" else None
            cached_answer = (
                cached_record.get("answer")
                if cached_record and cached_record["query"] == step.text
                else None
            )
            if arm in GAP_ARMS:
                gap = hints.get(step.text) if arm in PERSIST_GAPS else None
                if gap is None:
                    gap = model.call(
                        "gap",
                        {
                            "question": step.text,
                            "evidence": evidence,
                            "previous_questions": history[-4:],
                        },
                        220,
                    )
                    if (
                        set(gap) != {"missing", "search_terms"}
                        or not isinstance(gap["missing"], list)
                        or len(gap["missing"]) > 8
                        or any(not isinstance(x, str) or len(x) > 300 for x in gap["missing"])
                        or not isinstance(gap["search_terms"], str)
                        or len(gap["search_terms"]) > 500
                    ):
                        raise ValueError("invalid gap schema")
                    counts["gap_calls"] += 1
                else:
                    counts["gap_reuses"] += 1
                    reused = True
                if arm in PERSIST_GAPS:
                    hints.save(step.text, gap)
            raw = []
            if arm == "wider_recall":
                raw = ranked_sources(sources, step.text, LIMITS["source_reads_per_query"])
            elif gap and gap["missing"]:
                raw = ranked_sources(
                    sources, step.text + " " + gap["search_terms"], LIMITS["raw_candidates"]
                )
            counts["raw_source_reads"] += len(raw)
            evidence += [source_payload(source) for source in raw]
            packed, chars = [], 0
            for item in evidence:
                size = len(canonical(item))
                if chars + size <= LIMITS["context_chars"]:
                    packed.append(item)
                    chars += size
            if cached_answer is not None:
                # Revalidate cached quote dependencies against the same live sources.
                cached_ids = {q["source_id"] for q in cached_answer["quotes"]}
                for key in sorted(cached_ids):
                    if key not in sources:
                        continue
                    required = [q["quote"] for q in cached_answer["quotes"]
                                if q["source_id"] == key]
                    if all(any(p["source_id"] == key and quote in p["text"]
                               for p in packed) for quote in required):
                        continue
                    item = source_payload(sources[key])
                    size = len(canonical(item))
                    if chars + size <= LIMITS["context_chars"]:
                        packed.append(item)
                        chars += size
                # Cached answers have the same evidence ceiling as fresh ones.
                # Fall back to generation if their dependencies cannot fit.
                if not all(any(p["source_id"] == q["source_id"]
                               and q["quote"] in p["text"] for p in packed)
                           for q in cached_answer["quotes"]):
                    cached_answer = None
            if cached_answer is not None:
                output = cached_answer
            else:
                output = model.call("answer", {"question": step.text, "evidence": packed}, 700)
            if (
                set(output) != {"answer", "abstain", "quotes"}
                or not isinstance(output["answer"], str)
                or type(output["abstain"]) is not bool
                or not isinstance(output["quotes"], list)
                or len(output["quotes"]) > 8
            ):
                raise ValueError("invalid answer schema")
            # Revalidate current live dependencies and exact quote delivery. The ablation
            # cannot bypass this guard; it measures stale routing work, not unsafe delivery.
            citations, invalid = [], 0
            for quote in output["quotes"]:
                source = sources.get(quote.get("source_id")) if isinstance(quote, dict) else None
                text = quote.get("quote") if isinstance(quote, dict) else None
                available = (
                    any(
                        p["source_id"] == quote.get("source_id")
                        and isinstance(text, str)
                        and text in p["text"]
                        for p in packed
                    )
                    if isinstance(quote, dict)
                    else False
                )
                if (
                    source is None
                    or not isinstance(text, str)
                    or not text
                    or text not in source.text
                    or not available
                ):
                    invalid += 1
                else:
                    citations.append(
                        dict(source_id=source.source_id, revision=source.revision, quote=text)
                    )
            delivery_blocked = invalid > 0 or (not output["abstain"] and not citations)
            if arm in PERSIST_GAPS:
                hints.resolve(step.text, output, not delivery_blocked)
            if arm in PERSIST_ATOMS:
                counts["repair_quotes"] += await save_quotes(
                    [{"source_id": q["source_id"], "quote": q["quote"]} for q in citations], sources
                )
            rows.append(
                dict(
                    query_id=step.query_id,
                    group_id=episode.source_group,
                    arm=arm,
                    question=step.text,
                    raw_output=output,
                    delivered_answer="" if delivery_blocked else output["answer"],
                    abstain=True if delivery_blocked else output["abstain"],
                    citations=[] if delivery_blocked else citations,
                    delivery_blocked=delivery_blocked,
                    invalid_citations=invalid,
                    gap=gap,
                    gap_reused=reused,
                    retrieved_atoms=atoms,
                    raw_source_ids=[s.source_id for s in raw],
                    raw_evidence=[source_payload(s) for s in raw],
                    live_sources=[source_payload(s) for s in sources.values()],
                    revoked_source_ids=sorted(revoked),
                    packed_evidence=packed,
                    answer_cache_reused=cached_answer is not None,
                    model_tokens=model.used_tokens - before_tokens,
                    model_calls=len(model.receipts) - before_calls,
                    latency_seconds=time.perf_counter() - query_start,
                )
            )
            if checkpoint_dir is not None:
                checkpoint = dict(
                    row=rows[-1],
                    receipts=model.receipts,
                    counts=counts,
                    total_tokens=model.used_tokens,
                )
                checkpoint["checkpoint_sha256"] = digest(checkpoint)
                atomic_json(checkpoint_dir / ("query-" + step.query_id + ".json"), checkpoint)
            history.append(step.text)
        return dict(
            episode_id=episode.episode_id,
            group_id=episode.source_group,
            arm=arm,
            rows=rows,
            expected_query_ids=[q.query_id for q in episode.steps if isinstance(q, Query)],
            counts=counts,
            receipts=model.receipts,
            total_tokens=model.used_tokens,
            total_calls=len(model.receipts),
            whole_seconds=time.perf_counter() - start,
            retained_obligation_bytes=len(canonical(hints.records).encode()),
            obligation_transitions=hints.transitions,
            status="complete",
            evidence_class="synthetic-diagnostic-not-public-benchmark",
        )
    except Exception as error:
        raise EpisodeFailure(
            dict(
                episode_id=episode.episode_id,
                arm=arm,
                group_id=episode.source_group,
                rows=rows,
                counts=counts,
                receipts=model.receipts,
                total_tokens=model.used_tokens,
                total_calls=len(model.receipts),
                status="failed_or_budget_censored",
                error_type=type(error).__name__,
                ranking_eligible=False,
            )
        ) from None
    finally:
        await store.close()


def source_identity():
    root = Path(__file__).resolve().parents[2]
    paths = sorted(
        p for p in (root / "src/agent_memory").rglob("*") if p.suffix in {".py", ".sql"}
    ) + sorted(Path(__file__).parent.glob("*.py"))
    return digest(
        [(str(p.relative_to(root)), hashlib.sha256(p.read_bytes()).hexdigest()) for p in paths]
    )


def freeze(
    output, dataset, split, groups, arms, execution="real-api", provider_binding=None,
    *, max_calls=220,
):
    from .evaluate import BOOTSTRAP_PROTOCOL

    episodes = getattr(dataset, split)[:groups] if groups else getattr(dataset, split)
    runtime = [asdict(episode) for episode in episodes]
    manifest = dict(
        schema="evidence-obligations/2-checkpointed",
        provider_binding=provider_binding,
        baseline_sha=BASELINE_SHA,
        baseline_tree=BASELINE_TREE,
        runtime_python=platform.python_version(),
        runtime_platform=platform.platform(),
        dependency_policy=(
            "stdlib-only experimental runtime; core runtime no third-party dependency"
        ),
        statistics_protocol=asdict(BOOTSTRAP_PROTOCOL),
        statistics_protocol_sha256=BOOTSTRAP_PROTOCOL.fingerprint,
        setup_accounting=(
            "shared frozen extraction response; full logical extraction cost "
            "charged to each arm; actual provider receipt counted once globally"
        ),
        seed=dataset.seed,
        model_execution=execution,
        model="qwen3.8-flash" if execution == "real-api" else "scripted-test",
        model_revision="provider-alias-not-immutable",
        endpoint_region="operator-configured-unverified",
        model_parameters=dict(temperature=0, enable_thinking=False, stream=False),
        query_agent_semantics=(
            "all synthetic agents share one authorized episode scope; no cross-agent causal claim"
        ),
        split=split,
        groups=[e.source_group for e in episodes],
        arms=list(arms),
        limits=LIMITS,
        runtime_corpus_sha256=digest(runtime),
        gold_sha256=digest({k: asdict(v) for k, v in dataset.gold.items()}),
        source_sha256=source_identity(),
        prompts=PROMPTS,
        equal_access="same chronological authorized sources",
        budget_comparison=(
            "matched sequence ceilings; report measured total cost, not equal actual expenditure"
        ),
        ordinary_cache=(
            "similarity-keyed gap-response cache plus exact-query validated "
            "answer reuse; paraphrases always regenerate answer; same source "
            "frontier invalidation"
        ),
        no_invalidation_scope="stale routing hints only; delivery validity guards remain on",
        baseline_scope=(
            "controlled quote-atom prototype; real unchanged core retrieval; not "
            "full production pipeline"
        ),
        run_call_cap=max_calls,
        promotion="forbidden; synthetic diagnostic only",
        stopping=(
            "prefer cache/simple fix when equivalent; do not claim novel benefit "
            "from gap reuse alone"
        ),
    )
    envelope = dict(manifest=manifest, manifest_sha256=digest(manifest))
    path = output / "manifest.json"
    if path.exists():
        if json.loads(path.read_text()) != envelope:
            raise ValueError(
                "frozen manifest mismatch; use new output directory and same cumulative ledger"
            )
    else:
        atomic_json(path, envelope)
    return episodes, envelope


class SharedExtractionClient:
    def __init__(self, client, output):
        self.client, self.output = client, output

    def complete(self, messages, *, max_output_tokens, phase):
        if phase != "extract":
            return self.client.complete(messages, max_output_tokens=max_output_tokens, phase=phase)
        key = digest({"messages": messages, "maximum": max_output_tokens})
        path = self.output / "shared-extraction" / (key + ".json")
        if path.exists():
            artifact = json.loads(path.read_text())
            receipt = artifact["receipt"]
            if artifact["payload_sha256"] != key or artifact["receipt_sha256"] != digest(receipt):
                raise ValueError("corrupt shared extraction artifact")
            return {**receipt, "shared_extraction_reused": True}
        receipt = self.client.complete(messages, max_output_tokens=max_output_tokens, phase=phase)
        atomic_json(path, dict(payload_sha256=key, receipt_sha256=digest(receipt), receipt=receipt))
        return {**receipt, "shared_extraction_reused": False}


class RunClient:
    def __init__(self, client, limit, output):
        self.client, self.limit = client, limit
        self.path = output / "dispatch-count.json"
        self.calls = 0
        if self.path.exists():
            state = json.loads(self.path.read_text())
            if (
                state.get("limit") != limit
                or type(state.get("calls")) is not int
                or state["calls"] < 0
            ):
                raise ValueError("invalid persistent dispatch counter")
            self.calls = state["calls"]

    def complete(self, messages, *, max_output_tokens, phase):
        if self.calls >= self.limit:
            raise RuntimeError("run_call_limit_reached")
        self.calls += 1
        atomic_json(self.path, dict(limit=self.limit, calls=self.calls))
        print(canonical({"call": self.calls, "phase": phase, "status": "dispatch"}), flush=True)
        receipt = self.client.complete(messages, max_output_tokens=max_output_tokens, phase=phase)
        print(
            canonical(
                {
                    "call": self.calls,
                    "phase": phase,
                    "status": "settled",
                    "input_tokens": receipt["input_tokens"],
                    "output_tokens": receipt["output_tokens"],
                }
            ),
            flush=True,
        )
        return receipt


async def _run_unlocked(
    output, work, client, *, split="dev", groups=1, arms=ARMS, max_calls=220, execution="real-api"
):
    output, work = Path(output), Path(work)
    provider_binding = None
    if execution == "real-api":
        from .model_client import ENDPOINT

        ledger = client.ledger
        ledger_state = ledger.snapshot()
        provider_binding = dict(
            endpoint_sha256=digest(ENDPOINT),
            ledger_id=ledger_state["ledger_id"],
            ledger_path_sha256=digest(str(ledger.path.resolve())),
        )
    client = SharedExtractionClient(RunClient(client, max_calls, output), output)
    output.mkdir(parents=True, exist_ok=True)
    dataset = build_dataset(seed=SEED)
    episodes, manifest = freeze(
        output, dataset, split, groups, arms, execution, provider_binding, max_calls=max_calls
    )
    if split == "heldout" and not (output / "HOLDOUT_APPROVED").exists():
        raise RuntimeError(
            "heldout requires explicit reviewed pilot gate marker in frozen output directory"
        )
    results = []
    for episode in episodes:
        ordered = list(arms)
        random.Random(SEED + sum(map(ord, episode.episode_id))).shuffle(ordered)
        for arm in ordered:
            target = output / (episode.episode_id + "--" + arm + ".json")
            if target.exists():
                record = json.loads(target.read_text())
                checksum = record.pop("result_sha256", None)
                if (
                    checksum != digest(record)
                    or record.get("manifest_sha256") != manifest["manifest_sha256"]
                    or record.get("status") != "complete"
                    or record.get("arm") != arm
                    or record.get("episode_id") != episode.episode_id
                ):
                    raise ValueError("unusable prior result")
                results.append(record)
                continue
            taskwork = work / (episode.episode_id + "--" + arm)
            # Replay from a fresh isolated core using immutable completed-call responses.
            # Never reuse possibly half-mutated core state or re-dispatch known calls.
            import uuid

            from .checkpoints import DurableCallCache

            taskwork = taskwork / ("replay-" + uuid.uuid4().hex)
            taskwork.mkdir(parents=True)
            checkpoint_dir = output / "checkpoints" / (episode.episode_id + "--" + arm)
            arm_client = DurableCallCache(client, checkpoint_dir / "calls")
            try:
                record = await run_episode(episode, arm, arm_client, taskwork, checkpoint_dir)
                record["manifest_sha256"] = manifest["manifest_sha256"]
                record["result_sha256"] = digest(record)
                atomic_json(target, record)
                results.append(record)
                print(
                    canonical(
                        {
                            "episode": episode.episode_id,
                            "arm": arm,
                            "status": "complete",
                            "tokens": record["total_tokens"],
                        }
                    ),
                    flush=True,
                )
            except Exception as error:
                failure = dict(
                    episode=episode.episode_id,
                    arm=arm,
                    error_type=type(error).__name__,
                    manifest_sha256=manifest["manifest_sha256"],
                )
                if isinstance(error, EpisodeFailure):
                    failure["partial"] = error.record
                atomic_json(output / "failure.json", failure)
                raise RuntimeError(
                    "experiment stopped; inspect sanitized failure and ledger"
                ) from None
    # Gold is supplied only to the offline evaluator, after all runtime work.
    from .evaluate import evaluate

    report = evaluate(results, dataset.gold)
    report["manifest_sha256"] = manifest["manifest_sha256"]
    report["model_execution"] = execution
    atomic_json(output / "report.json", report)
    return report


async def run(
    output, work, client, *, split="dev", groups=1, arms=ARMS, max_calls=220, execution="real-api"
):
    """One process owns a run, including shared extraction and its dispatch counter."""
    import fcntl

    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with (output / ".run.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another process already owns this run directory") from None
        try:
            return await _run_unlocked(
                output,
                work,
                client,
                split=split,
                groups=groups,
                arms=arms,
                max_calls=max_calls,
                execution=execution,
            )
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _prompt_api_key():
    """Never accept getpass's fallback to an echoed terminal prompt."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            return getpass.getpass("API key: ")
        except getpass.GetPassWarning:
            raise RuntimeError(
                "A non-echoing terminal is required; use --key-stdin with a private input pipe."
            ) from None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--split", choices=["dev", "heldout"], default="dev")
    parser.add_argument("--groups", type=int, default=1)
    parser.add_argument("--key-stdin", action="store_true")
    args = parser.parse_args()
    from urllib.parse import urlsplit

    from .model_client import ENDPOINT, BudgetLedger, OpenAICompatibleClient

    endpoint = urlsplit(ENDPOINT)
    if (
        endpoint.scheme != "https"
        or not endpoint.hostname
        or endpoint.hostname.endswith(".invalid")
    ):
        raise RuntimeError(
            "Set EVIDENCE_MODEL_ENDPOINT to your approved HTTPS endpoint before running."
        )
    ledger = BudgetLedger(Path(args.ledger))
    key = sys.stdin.readline().rstrip("\r\n") if args.key_stdin else _prompt_api_key()
    client = OpenAICompatibleClient(key, ledger=ledger, timeout_seconds=60)
    key = None
    asyncio.run(run(args.output, args.work, client, split=args.split, groups=args.groups))


if __name__ == "__main__":
    main()
