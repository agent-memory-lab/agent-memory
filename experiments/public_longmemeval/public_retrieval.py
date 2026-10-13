"""Pinned LongMemEval preparation and gold-blind, dependency-free retrieval baselines.

This is an all-role SESSION retrieval diagnostic, not an agent-memory QA score
and not a reproduction of upstream's user-only retrieval preprocessing.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from time import perf_counter

DATASET_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"
DATASET_REVISION = "98d7416c24c778c2fee6e6f3006e7a073259d48f"
REPO_REVISION = "current-checkout; inspect runner core_identity"
UPSTREAM_REVISION = "9e0b455f4ef0e2ab8f2e582289761153549043fc"
SEED = "agent-memory-public-longmemeval-2026-10-10-v1"
PROTOCOL = "longmemeval-s-cleaned/all-role-session-retrieval/1"


def sha(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1048576), b""):
            value.update(block)
    return value.hexdigest()


def stable(value):
    return hashlib.sha256((SEED + ":" + value).encode()).hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def timestamp(value):
    cleaned = re.sub(r"\s*\([^)]*\)", "", value).strip()
    return datetime.strptime(cleaned, "%Y/%m/%d %H:%M").replace(tzinfo=timezone.utc).isoformat()


def make_split(data):
    """Freeze 50 development cases; use metadata only, never gold answer content."""
    strata = defaultdict(list)
    for row in data:
        strata[(row["question_type"], "_abs" in row["question_id"])].append(row["question_id"])
    strata = {key: sorted(ids, key=stable) for key, ids in sorted(strata.items())}
    # At least one case from each native-type/abstention stratum, then
    # proportional allocation using largest remainders for the remaining 40.
    quotas = {key: 1 for key in strata}
    remaining = 50 - len(strata)
    denominator = sum(len(ids) - 1 for ids in strata.values())
    ideal = {key: remaining * (len(ids) - 1) / denominator for key, ids in strata.items()}
    for key in quotas:
        quotas[key] += int(ideal[key])
    for key in sorted(strata, key=lambda k: (-(ideal[k] % 1), k))[:50 - sum(quotas.values())]:
        quotas[key] += 1
    development = sorted(qid for key, ids in strata.items() for qid in ids[:quotas[key]])
    smoke = [ids[0] for ids in strata.values()]
    holdout = sorted(row["question_id"] for row in data if row["question_id"] not in development)
    return {
        "seed": SEED, "selection": "sha256 order within native type and abstention strata",
        "development": development, "smoke": smoke, "holdout": holdout,
        "strata": [{"question_type": key[0], "abstention": key[1], "all": len(ids),
                    "development": quotas[key]} for key, ids in strata.items()],
    }


def prepare(args):
    if sha(args.data) != DATASET_SHA256:
        raise ValueError("Dataset bytes do not match the verified official cleaned revision")
    data = json.loads(args.data.read_text(encoding='utf-8'))
    if len(data) != 500 or len({row["question_id"] for row in data}) != 500:
        raise ValueError("Expected 500 unique official questions")
    args.output.mkdir(parents=True, exist_ok=False)
    split = make_split(data)
    dump(args.output / "split.json", split)
    statistics = Counter()
    with (args.output / "corpus.jsonl").open("w", encoding="utf-8") as corpus, \
            (args.output / "queries.jsonl").open("w", encoding="utf-8") as queries, \
            (args.output / "gold.evaluator-only.jsonl").open("w", encoding="utf-8") as gold:
        for row in data:
            case_id = "case-" + stable(row["question_id"])[:24]
            aligned = list(zip(row["haystack_session_ids"], row["haystack_dates"],
                               row["haystack_sessions"], strict=True))
            statistics["duplicate_session_id_occurrences"] += len(aligned) - len({item[0] for item in aligned})
            # Neutral IDs and actual dates are the only metadata given to retrieval.
            # Original session IDs can reveal answer/noans markers and stay evaluator-only.
            source_map = defaultdict(list)
            sessions = []
            for index, (original_id, date, turns) in enumerate(aligned):
                source_id = f"session-{index:04d}"
                source_map[original_id].append(source_id)
                blind_turns = [{"role": turn["role"], "content": turn["content"]}
                               for turn in turns if turn["role"] in {"user", "assistant"}]
                sessions.append({"source_id": source_id, "occurred_at": timestamp(date),
                                 "turns": blind_turns})
                statistics["turns"] += len(blind_turns)
                statistics["utf8_content_bytes"] += sum(len(t["content"].encode()) for t in blind_turns)
            sessions.sort(key=lambda item: (item["occurred_at"], item["source_id"]))
            statistics["sessions"] += len(sessions)
            queries.write(json.dumps({"case_id": case_id, "question": row["question"],
                                      "question_date": timestamp(row["question_date"])}) + "\n")
            corpus.write(json.dumps({"case_id": case_id, "sessions": sessions}, ensure_ascii=False) + "\n")
            if any(len(source_map[sid]) != 1 for sid in row["answer_session_ids"]):
                raise ValueError("A gold session is missing or duplicated; explicit grouped-evidence scoring required")
            gold.write(json.dumps({
                "case_id": case_id, "question_id": row["question_id"],
                "question_type": row["question_type"], "abstention": "_abs" in row["question_id"],
                "answer": row["answer"],
                "answer_source_ids": [source_map[sid][0] for sid in row["answer_session_ids"]],
                "original_source_map": source_map,
                "split": "development" if row["question_id"] in split["development"] else "holdout",
                "smoke": row["question_id"] in split["smoke"],
            }, ensure_ascii=False) + "\n")
    # Machine-readable case lists let the retrieval process select a cohort without opening gold.
    for group in ("development", "smoke", "holdout"):
        dump(args.output / f"{group}.case-ids.json", ["case-" + stable(qid)[:24] for qid in split[group]])
    manifest = {
        "protocol": PROTOCOL, "evidence_class": "public_benchmark", "official_score_equivalent": False,
        "dataset": "LongMemEval-S cleaned", "dataset_revision": DATASET_REVISION,
        "dataset_sha256": DATASET_SHA256, "dataset_bytes": args.data.stat().st_size,
        "dataset_license": "MIT", "dataset_license_source":
        f"https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/blob/{DATASET_REVISION}/README.md",
        "agent_memory_revision": REPO_REVISION, "upstream_metrics_revision": UPSTREAM_REVISION,
        "adapter_sha256": sha(Path(__file__)), "cases": 500, "development_cases": 50,
        "held_out_cases": 450, "smoke_cases": len(split["smoke"]), "statistics": dict(statistics),
        "artifacts": {p.name: sha(p) for p in sorted(args.output.iterdir()) if p.is_file()},
        "notes": ["Gold answer, type, answer flags and original IDs are evaluator-only.",
                  "All-role histories differ from upstream user-only BM25 preprocessing.",
                  "Official timestamps have no timezone; UTC is a serialization assumption only.",
                  "Official histories are preserved without a question-date cutoff; this is not a causal/as-of-time test.",
                  "Development/holdout partition is not an upstream official split.",
                  "No QA accuracy, production acceptance or model cost conclusion is implied."],
    }
    dump(args.output / "manifest.json", manifest)
    print(json.dumps({"prepared": str(args.output), "statistics": dict(statistics), "smoke_cases": len(split["smoke"])}))


@dataclass(frozen=True)
class BlindSession:
    source_id: str
    occurred_at: str
    text: str


def blind_sessions(record):
    if set(record) != {"case_id", "sessions"}:
        raise ValueError("Unexpected corpus metadata")
    output = []
    for session in record["sessions"]:
        if set(session) != {"source_id", "occurred_at", "turns"}:
            raise ValueError("Unexpected session metadata")
        if any(set(turn) != {"role", "content"} for turn in session["turns"]):
            raise ValueError("Unexpected turn metadata, possible answer leakage")
        output.append(BlindSession(session["source_id"], session["occurred_at"],
                                   "\n".join(t["role"] + ": " + t["content"] for t in session["turns"])))
    return output


def terms(text):
    return re.findall(r"[a-z0-9]+", text.casefold())


def bm25(question, sessions, k):
    """Positive-IDF BM25, k1=1.2, b=.75, casefolded ASCII words; no model calls."""
    frequencies = [Counter(terms(session.text)) for session in sessions]
    lengths = [sum(row.values()) for row in frequencies]
    average = sum(lengths) / len(lengths) if lengths else 0
    query = set(terms(question))
    df = Counter(term for row in frequencies for term in query.intersection(row))
    results = []
    for session, row, length in zip(sessions, frequencies, lengths, strict=True):
        score = sum(math.log(1 + (len(sessions) - df[term] + .5) / (df[term] + .5))
                    * row[term] * 2.2 / (row[term] + 1.2 * (.25 + .75 * length / average))
                    for term in sorted(query.intersection(row))) if average else 0
        if score > 0:
            results.append((score, session.source_id))
    return [source_id for _, source_id in sorted(results, key=lambda v: (-v[0], v[1]))[:k]]


def verify_artifact(directory, name, manifest):
    if sha(directory / name) != manifest["artifacts"][name]:
        raise ValueError(f"Changed artifact: {name}")


def retrieve(args):
    if type(args.k) is not int or args.k < 10 or args.k > 100:
        raise ValueError("k must be between 10 and 100 for the declared @5/@10 protocol")
    manifest = json.loads((args.prepared / "manifest.json").read_text(encoding='utf-8'))
    for name in ("corpus.jsonl", "queries.jsonl", f"{args.split}.case-ids.json"):
        verify_artifact(args.prepared, name, manifest)
    selected = set(json.loads((args.prepared / f"{args.split}.case-ids.json").read_text(encoding='utf-8')))
    queries = {r["case_id"]: r for line in (args.prepared / "queries.jsonl").open(encoding='utf-8') if (r := json.loads(line))["case_id"] in selected}
    if args.output.exists():
        raise ValueError("Use a fresh result path to avoid mixed protocol outputs")
    started = perf_counter()
    with (args.prepared / "corpus.jsonl").open(encoding='utf-8') as source, args.output.open("w", encoding="utf-8") as output:
        for line in source:
            row = json.loads(line)
            if row["case_id"] not in selected:
                continue
            if set(queries[row["case_id"]]) != {"case_id", "question", "question_date"}:
                raise ValueError("Unexpected query metadata")
            sessions = blind_sessions(row)
            for arm in ("bm25-session", "recent-session"):
                start = perf_counter()
                ranking = bm25(queries[row["case_id"]]["question"], sessions, args.k) if arm == "bm25-session" else [s.source_id for s in sorted(sessions, key=lambda s: (s.occurred_at, s.source_id), reverse=True)[:args.k]]
                output.write(json.dumps({"case_id": row["case_id"], "arm": arm, "ranking": ranking,
                                         "seconds": perf_counter() - start, "sessions": len(sessions),
                                         "model_calls": 0}) + "\n")
    dump(args.output.with_suffix(".run.json"), {
        "protocol": PROTOCOL, "split": args.split, "cases": len(queries), "k": args.k,
        "prepared_manifest_sha256": sha(args.prepared / "manifest.json"),
        "runner_sha256": sha(Path(__file__)), "results_sha256": sha(args.output),
        "seconds_including_corpus_read": perf_counter() - started, "model_calls": 0,
        "bm25": {"k1": 1.2, "b": .75, "idf": "log(1+(N-df+.5)/(df+.5))",
                 "tokenizer": "casefolded ASCII alphanumeric runs", "unit": "all-role session"},
    })
    print(json.dumps({"retrieved_cases": len(queries), "output": str(args.output)}))


def metrics(ranking, relevant, k):
    if not relevant:
        raise ValueError("Evidence metrics require nonempty gold; do not score abstention as perfect recall")
    hits = [int(value in relevant) for value in ranking[:k]]
    def dcg(values):
        return sum(value if index == 0 else value / math.log2(index + 1) for index, value in enumerate(values))
    return {f"recall_any@{k}": float(any(hits)),
            f"recall_all@{k}": float(set(relevant).issubset(ranking[:k])),
            f"upstream_ndcg@{k}": dcg(hits) / dcg([1] * min(len(relevant), k))}


def score(args):
    manifest = json.loads((args.prepared / "manifest.json").read_text(encoding='utf-8'))
    verify_artifact(args.prepared, "gold.evaluator-only.jsonl", manifest)
    run = json.loads(args.results.with_suffix(".run.json").read_text(encoding='utf-8'))
    if run["protocol"] != PROTOCOL or run["k"] < 10:
        raise ValueError("Unsupported retrieval protocol or cutoff")
    if sha(args.results) != run["results_sha256"] or sha(args.prepared / "manifest.json") != run["prepared_manifest_sha256"]:
        raise ValueError("Run inputs or results changed")
    gold = {r["case_id"]: r for line in (args.prepared / "gold.evaluator-only.jsonl").open(encoding='utf-8') if (r := json.loads(line))}
    cohort_file = f"{run['split']}.case-ids.json"
    verify_artifact(args.prepared, cohort_file, manifest)
    cohort = set(json.loads((args.prepared / cohort_file).read_text(encoding='utf-8')))
    arms = {"bm25-session", "recent-session"}
    expected = {(case_id, arm) for case_id in cohort for arm in arms}
    seen = set()
    scores, skipped, latencies, type_scores = defaultdict(list), Counter(), defaultdict(list), defaultdict(list)
    for line in args.results.open(encoding='utf-8'):
        row = json.loads(line)
        pair = (row["case_id"], row["arm"])
        if pair not in expected or pair in seen:
            raise ValueError("Duplicate result or case/arm outside the frozen cohort")
        seen.add(pair)
        target = gold[row["case_id"]]
        known_sources = {sid for group in target["original_source_map"].values() for sid in group}
        if (len(row["ranking"]) > run["k"] or len(set(row["ranking"])) != len(row["ranking"])
                or not set(row["ranking"]).issubset(known_sources)):
            raise ValueError("Invalid or repeated source IDs in retrieval result")
        latencies[row["arm"]].append(row["seconds"])
        if target["abstention"]:
            skipped[row["arm"]] += 1
            continue
        result = {key: value for k in (5, 10) for key, value in metrics(row["ranking"], target["answer_source_ids"], k).items()}
        scores[row["arm"]].append(result)
        type_scores[(row["arm"], target["question_type"])].append(result)
    if seen != expected or run["cases"] != len(cohort):
        raise ValueError("Incomplete retrieval cohort; no aggregate score emitted")
    def aggregate(rows):
        return {"scored": len(rows), **{key: sum(row[key] for row in rows) / len(rows) for key in rows[0]}}
    summary = {
        "protocol": PROTOCOL, "split": run["split"], "official_score_equivalent": False,
        "evidence_class": "public_benchmark", "answer_accuracy": None,
        "run": run, "arms": {arm: {**aggregate(rows), "abstention_not_scored": skipped[arm],
                                  "mean_index_and_query_seconds": sum(latencies[arm]) / len(latencies[arm]),
                                  "by_type": {kind: aggregate(values) for (name, kind), values in type_scores.items() if name == arm}}
                              for arm, rows in scores.items()},
        "limitations": ["Retrieval-only reference baselines; no agent-memory, Mem0, Graphiti or Hindsight QA result.",
                        "Abstention handling needs generated answers and is untested here.",
                        "This uses all-role sessions and its own BM25 tokenizer; upstream user-only baseline differs.",
                        "Upstream DCG convention weights ranks 1 and 2 equally.",
                        "Development subset is stratified and not population-weighted; no tuning on holdout."]
    }
    dump(args.output, summary)
    print(json.dumps({arm: {key: value for key, value in values.items() if key != "by_type"} for arm, values in summary["arms"].items()}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--data", type=Path, required=True)
    prep.add_argument("--output", type=Path, required=True)
    retrieval = commands.add_parser("retrieve")
    retrieval.add_argument("--prepared", type=Path, required=True)
    retrieval.add_argument("--split", choices=("smoke", "development", "holdout"), default="smoke")
    retrieval.add_argument("--k", type=int, default=10)
    retrieval.add_argument("--output", type=Path, required=True)
    scoring = commands.add_parser("score")
    scoring.add_argument("--prepared", type=Path, required=True)
    scoring.add_argument("--results", type=Path, required=True)
    scoring.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    {"prepare": prepare, "retrieve": retrieve, "score": score}[args.command](args)


if __name__ == "__main__":
    main()
