"""Offline, deterministic scoring of the bounded synthetic episode experiment.

The evaluator never imports the runtime, reads model credentials, or makes model
calls. Gold requirements are checked independently of the runtime's delivery guard.
This is a deliberately strict lexical diagnostic, not a semantic truth judge or
public benchmark. Unsupported additional prose and uncited obsolete propositions
cannot in general be detected by substring checks; the report states that limit.
"""

from __future__ import annotations

import math
import random
import re
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from hashlib import sha256
from typing import Any

from agent_memory.evaluation.evidence import _digest
from agent_memory.evaluation.question_statistics import BootstrapProtocol

BOOTSTRAP_PROTOCOL = BootstrapProtocol(17, 2000, 0.95, 2, 2)
SCHEMA = "evidence-obligations-evaluation/1"
INPUT_CNY_PER_MILLION = 0.8
OUTPUT_CNY_PER_MILLION = 2.7
COMPARATORS = ("repaired_atoms", "ordinary_cache")
CANDIDATE = "persistent_obligations"


def normalize(text: str) -> str:
    """NFKC/case/whitespace only; punctuation and negation remain significant."""
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def _contains(text: str, requirement: str) -> bool:
    # Word boundaries avoid giving "30" credit for "3", or "disallowed" for "allowed".
    term = normalize(requirement)
    left = r"(?<!\w)" if term[0].isalnum() else ""
    right = r"(?!\w)" if term[-1].isalnum() else ""
    return re.search(left + re.escape(term) + right, normalize(text)) is not None


def _missing(texts: list[str], requirements: list[str]) -> list[str]:
    return [term for term in requirements if not any(_contains(text, term) for text in texts)]


def _integer(value: Any) -> bool:
    return type(value) is int and value >= 0


def _number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _gold(value: Any, query_id: str) -> dict[str, Any]:
    value = asdict(value) if is_dataclass(value) else value
    if not isinstance(value, Mapping) or value.get("query_id") != query_id:
        raise ValueError(f"invalid gold identity: {query_id}")
    result = dict(value)
    if not isinstance(result.get("category"), str) or not result["category"]:
        raise ValueError(f"invalid gold category: {query_id}")
    if type(result.get("answerable")) is not bool:
        raise ValueError(f"invalid gold answerability: {query_id}")
    for name in ("required_values", "required_qualifiers", "supporting_source_ids"):
        items = result.get(name)
        if (
            not isinstance(items, (list, tuple))
            or any(not isinstance(item, str) or not item.strip() for item in items)
            or len(set(items)) != len(items)
        ):
            raise ValueError(f"invalid gold {name}: {query_id}")
        result[name] = list(items)
    revisions = result.get("supporting_source_revisions")
    if not isinstance(revisions, (list, tuple)):
        raise ValueError(f"missing gold source revisions: {query_id}")
    expected = {}
    for entry in revisions:
        if (
            not isinstance(entry, (list, tuple))
            or len(entry) != 2
            or not isinstance(entry[0], str)
            or not _integer(entry[1])
            or entry[0] in expected
        ):
            raise ValueError(f"invalid gold source revisions: {query_id}")
        expected[entry[0]] = entry[1]
    if set(expected) != set(result["supporting_source_ids"]):
        raise ValueError(f"gold support/revision mismatch: {query_id}")
    if result["answerable"] and (not result["required_values"] or not expected):
        raise ValueError(f"answerable gold needs values and versioned support: {query_id}")
    result["expected_revisions"] = expected
    return result


def _documents(value: Any) -> tuple[list[dict[str, Any]], bool]:
    """Accept runtime source payloads and retrieved quote records, never guess text."""
    if not isinstance(value, list):
        return [], False
    documents, complete = [], True
    for entry in value:
        if not isinstance(entry, Mapping):
            complete = False
            continue
        text = entry.get("text", entry.get("quote"))
        if (
            not isinstance(entry.get("source_id"), str)
            or not _integer(entry.get("revision"))
            or not isinstance(text, str)
        ):
            complete = False
            continue
        documents.append(dict(source_id=entry["source_id"], revision=entry["revision"], text=text))
    return documents, complete


def _stage(documents: list[dict], complete: bool, gold: dict) -> dict:
    eligible = [
        item
        for item in documents
        if gold["expected_revisions"].get(item["source_id"]) == item["revision"]
    ]
    missing_sources = sorted(
        set(gold["supporting_source_ids"]) - {item["source_id"] for item in eligible}
    )
    texts = [item["text"] for item in eligible]
    missing_values = _missing(texts, gold["required_values"])
    missing_qualifiers = _missing(texts, gold["required_qualifiers"])
    if not gold["answerable"]:
        state = "not_applicable_unanswerable"
    elif not missing_sources and not missing_values and not missing_qualifiers:
        state = "present"
    else:
        state = "absent" if complete else "unknown"
    return dict(
        state=state,
        snapshot_complete=complete,
        missing_source_ids=missing_sources,
        missing_values=missing_values,
        missing_qualifiers=missing_qualifiers,
    )


def _citation_issues(
    citation: Any,
    packed: list[dict],
    live: list[dict],
    live_known: bool,
    gold: dict,
    revoked: set[str],
) -> list[str]:
    if not isinstance(citation, Mapping) or (
        not isinstance(citation.get("source_id"), str)
        or not _integer(citation.get("revision"))
        or not isinstance(citation.get("quote"), str)
        or not citation["quote"]
    ):
        return ["invalid_citation_schema"]
    source_id, revision, quote = citation["source_id"], citation["revision"], citation["quote"]
    reasons = []
    if source_id in revoked:
        reasons.append("revoked_source")
    expected = gold["expected_revisions"].get(source_id)
    if expected is not None and revision != expected:
        reasons.append("stale_or_wrong_revision")
    current = [entry for entry in live if entry["source_id"] == source_id]
    if live_known:
        if not current:
            reasons.append("not_in_current_authorized_sources")
        elif not any(entry["revision"] == revision for entry in current):
            reasons.append("stale_or_wrong_revision")
        elif not any(entry["revision"] == revision and quote in entry["text"] for entry in current):
            reasons.append("quote_not_in_current_source")
    if not any(
        entry["source_id"] == source_id and entry["revision"] == revision and quote in entry["text"]
        for entry in packed
    ):
        reasons.append("quote_not_in_packed_evidence")
    return sorted(set(reasons))


def _score(row: Any, gold: dict, query_id: str, group_id: str, arm: str) -> dict:
    row = row if isinstance(row, Mapping) else {}
    packed, packed_known = _documents(row.get("packed_evidence"))
    live, live_known = _documents(row.get("live_sources"))
    atoms, atoms_known = _documents(row.get("retrieved_atoms"))
    raw, raw_known = _documents(row.get("raw_evidence"))
    stages = dict(
        raw=_stage(live, live_known, gold),
        retrieved=_stage(atoms + raw, atoms_known and raw_known, gold),
        packed=_stage(packed, packed_known, gold),
        raw_reads=_stage(raw, raw_known, gold),
        retrieved_atoms=_stage(atoms, atoms_known, gold),
    )
    revoked_list = row.get("revoked_source_ids", [])
    revoked = (
        set(revoked_list)
        if isinstance(revoked_list, list) and all(isinstance(value, str) for value in revoked_list)
        else set()
    )
    raw_output = row.get("raw_output")
    output_valid = (
        isinstance(raw_output, Mapping)
        and set(raw_output) == {"answer", "abstain", "quotes"}
        and isinstance(raw_output.get("answer"), str)
        and type(raw_output.get("abstain")) is bool
    )
    if output_valid:
        proposals = raw_output.get("quotes")
        output_valid = (
            isinstance(proposals, list)
            and len(proposals) <= 8
            and all(
                isinstance(item, Mapping)
                and set(item) == {"source_id", "quote"}
                and isinstance(item["source_id"], str)
                and bool(item["source_id"])
                and isinstance(item["quote"], str)
                and bool(item["quote"])
                for item in proposals
            )
        )
    delivered = row.get("delivered_answer")
    citations = row.get("citations")
    delivery_valid = (
        isinstance(delivered, str)
        and type(row.get("abstain")) is bool
        and type(row.get("delivery_blocked")) is bool
        and isinstance(citations, list)
    )
    citations = citations if isinstance(citations, list) else []
    citation_checks = []
    support_quotes, support_sources = [], set()
    for citation in citations:
        issues = _citation_issues(citation, packed, live, live_known, gold, revoked)
        citation_checks.append(dict(citation=citation, issues=issues))
        if (
            not issues
            and gold["expected_revisions"].get(citation["source_id"]) == citation["revision"]
        ):
            support_quotes.append(citation["quote"])
            support_sources.add(citation["source_id"])
    missing_values = _missing(
        [delivered] if isinstance(delivered, str) else [], gold["required_values"]
    )
    missing_qualifiers = _missing(
        [delivered] if isinstance(delivered, str) else [], gold["required_qualifiers"]
    )
    unsupported_values = _missing(support_quotes, gold["required_values"])
    unsupported_qualifiers = _missing(support_quotes, gold["required_qualifiers"])
    missing_sources = sorted(set(gold["supporting_source_ids"]) - support_sources)
    abstain = row.get("abstain") is True
    blocked = row.get("delivery_blocked") is True
    valid_answer = (
        output_valid and delivery_valid and not blocked and not abstain and bool(delivered.strip())
    )
    # A delivery cannot silently differ from the model output outside the explicit guard.
    consistent = (
        output_valid
        and delivery_valid
        and (blocked or (delivered == raw_output["answer"] and abstain == raw_output["abstain"]))
    )
    if consistent and not blocked:
        proposed = Counter((item["source_id"], item["quote"]) for item in raw_output["quotes"])
        delivered_quotes = Counter(
            (item.get("source_id"), item.get("quote"))
            for item in citations
            if isinstance(item, Mapping)
            and isinstance(item.get("source_id"), str)
            and isinstance(item.get("quote"), str)
        )
        consistent = proposed == delivered_quotes and len(citations) == sum(
            delivered_quotes.values()
        )
    valid_answer = bool(valid_answer and consistent)
    answer_correct = bool(gold["answerable"] and valid_answer and not missing_values)
    qualifier_fidelity = bool(gold["answerable"] and valid_answer and not missing_qualifiers)
    citation_coverage = bool(gold["answerable"] and valid_answer and not missing_sources)
    citation_support = bool(
        gold["answerable"]
        and valid_answer
        and citations
        and not unsupported_values
        and not unsupported_qualifiers
        and all(not item["issues"] for item in citation_checks)
    )
    stale = any("stale_or_wrong_revision" in item["issues"] for item in citation_checks)
    revoked_leak = any("revoked_source" in item["issues"] for item in citation_checks)
    unauthorized = any(
        "not_in_current_authorized_sources" in item["issues"] for item in citation_checks
    )
    effective = answer_correct and qualifier_fidelity and citation_coverage and citation_support
    correct_abstention = bool(
        not gold["answerable"]
        and output_valid
        and delivery_valid
        and consistent
        and abstain
        and not blocked
        and not citations
        and not raw_output["quotes"]
    )
    problems = []
    if not row:
        problems.append("missing_answer_row")
    if not output_valid or not delivery_valid or not consistent:
        problems.append("invalid_answer_schema_or_delivery")
    if blocked:
        problems.append("delivery_blocked")
    if gold["answerable"] and abstain:
        problems.append("incorrect_abstention")
    if gold["answerable"] and not effective:
        problems.extend(
            name
            for name, ok in (
                ("answer_values_failed", answer_correct),
                ("qualifier_fidelity_failed", qualifier_fidelity),
                ("citation_coverage_failed", citation_coverage),
                ("citation_support_failed", citation_support),
            )
            if not ok
        )
    if not gold["answerable"] and not correct_abstention:
        problems.append("unanswerable_query_not_correctly_abstained")
    if effective or correct_abstention:
        diagnostic = "success_under_lexical_contract"
    elif stages["raw"]["state"] == "absent":
        diagnostic = "required_evidence_absent_from_current_raw_snapshot"
    elif stages["raw"]["state"] == "present" and stages["retrieved"]["state"] == "absent":
        diagnostic = "required_evidence_lost_before_retrieved_stage"
    elif stages["retrieved"]["state"] == "present" and stages["packed"]["state"] == "absent":
        diagnostic = "required_evidence_lost_before_packed_stage"
    elif stages["packed"]["state"] == "present":
        diagnostic = "required_evidence_present_but_answer_or_citation_failed"
    else:
        diagnostic = "unknown_stage_attribution"
    return dict(
        query_id=query_id,
        group_id=group_id,
        arm=arm,
        category=gold.get("category", "unspecified"),
        answerable=gold["answerable"],
        answer_schema_valid=bool(output_valid and delivery_valid and consistent),
        answer_correctness=answer_correct,
        qualifier_fidelity=qualifier_fidelity,
        citation_coverage=citation_coverage,
        citation_support=citation_support,
        effective_correct_answer=bool(effective),
        correct_abstention=correct_abstention,
        incorrect_abstention=bool(gold["answerable"] and abstain),
        delivery_blocked=blocked,
        missing_values=missing_values,
        missing_qualifiers=missing_qualifiers,
        unsupported_values=unsupported_values,
        unsupported_qualifiers=unsupported_qualifiers,
        missing_citation_source_ids=missing_sources,
        citation_checks=citation_checks,
        stale_revision_leakage=stale,
        revoked_source_leakage=revoked_leak,
        unauthorized_source_leakage=unauthorized,
        leakage_detection_scope=(
            "delivered citation identities and exact quotes; uncited semantic leakage unknown"
        ),
        stages=stages,
        diagnostic=diagnostic,
        causal_attribution="unknown; stage observations are not causal proof",
        failures=problems,
    )


def _cost(result: dict) -> dict:
    receipts = result.get("receipts")
    issues = []
    if not isinstance(receipts, list) or not receipts:
        receipts = []
        issues.append("missing_whole_episode_receipts")
    valid = all(
        isinstance(item, Mapping)
        and _integer(item.get("input_tokens"))
        and _integer(item.get("output_tokens"))
        for item in receipts
    )
    if not valid:
        issues.append("invalid_usage_receipt")
    input_tokens = sum(item["input_tokens"] for item in receipts) if valid else None
    output_tokens = sum(item["output_tokens"] for item in receipts) if valid else None
    total = input_tokens + output_tokens if valid else None
    if total != result.get("total_tokens"):
        issues.append("receipt_total_tokens_mismatch")
    if len(receipts) != result.get("total_calls"):
        issues.append("receipt_total_calls_mismatch")
    if not any(isinstance(item, Mapping) and item.get("phase") == "extract" for item in receipts):
        issues.append("extraction_phase_not_verified")
    counts = result.get("counts", {})
    raw_reads = counts.get("raw_source_reads") if isinstance(counts, Mapping) else None
    if not _integer(raw_reads):
        issues.append("invalid_raw_source_read_count")
        raw_reads = None
    latency = result.get("whole_seconds")
    if not _number(latency):
        issues.append("invalid_whole_episode_latency")
        latency = None
    complete = not issues
    units = input_tokens * 8 + output_tokens * 27 if valid else None
    return dict(
        accounting_complete=complete,
        issues=issues,
        receipt_input_tokens=input_tokens,
        receipt_output_tokens=output_tokens,
        receipt_total_tokens=total,
        receipt_total_calls=len(receipts),
        input_tokens=input_tokens if complete else None,
        output_tokens=output_tokens if complete else None,
        total_tokens=total if complete else None,
        total_calls=len(receipts) if complete else None,
        raw_source_reads=raw_reads,
        whole_seconds=latency,
        tariff_estimated_cny=units / 10_000_000 if complete else None,
        tariff_cost_units=units if complete else None,
    )


QUALITY_FIELDS = (
    "answer_correctness",
    "qualifier_fidelity",
    "citation_coverage",
    "citation_support",
    "effective_correct_answer",
    "correct_abstention",
    "incorrect_abstention",
    "delivery_blocked",
    "stale_revision_leakage",
    "revoked_source_leakage",
    "unauthorized_source_leakage",
)
COST_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "total_calls",
    "raw_source_reads",
    "whole_seconds",
    "tariff_estimated_cny",
)


def _sum_known(values: list[Any]) -> int | float | None:
    return sum(values) if values and all(value is not None for value in values) else None


def _provider_usage(results: list[dict]) -> dict:
    """Deduplicate frozen extraction receipts without claiming cumulative billing."""
    unique, issues, references = {}, [], 0
    for result in results:
        receipts = result.get("receipts")
        if not isinstance(receipts, list):
            issues.append("missing_receipts")
            continue
        for receipt in receipts:
            references += 1
            if not isinstance(receipt, Mapping) or not isinstance(receipt.get("receipt_id"), str):
                issues.append("missing_provider_receipt_identity")
                continue
            receipt_id = receipt["receipt_id"]
            if (
                not receipt_id
                or not _integer(receipt.get("input_tokens"))
                or not _integer(receipt.get("output_tokens"))
            ):
                issues.append("invalid_provider_usage_receipt")
                continue
            identity = (receipt.get("phase"), receipt["input_tokens"], receipt["output_tokens"])
            if receipt_id in unique:
                if unique[receipt_id] != identity:
                    issues.append("conflicting_duplicate_provider_receipt")
                elif receipt.get("phase") != "extract":
                    issues.append("unexpected_non_extraction_receipt_reuse")
            else:
                unique[receipt_id] = identity
    complete = not issues and bool(unique)
    inputs = sum(value[1] for value in unique.values())
    outputs = sum(value[2] for value in unique.values())
    return dict(
        classification="deduplicated observed usage with tariff estimate, not provider bill",
        scope=(
            "supplied complete-cell receipts only; excludes earlier runs and failed reservations"
        ),
        cumulative_spend_authority="durable provider-call ledger, not this report",
        accounting_complete=complete,
        issues=sorted(set(issues)),
        logical_receipt_references=references,
        shared_receipt_references=references - len(unique) if complete else None,
        provider_calls=len(unique) if complete else None,
        input_tokens=inputs if complete else None,
        output_tokens=outputs if complete else None,
        total_tokens=inputs + outputs if complete else None,
        tariff_estimated_cny=(inputs * 8 + outputs * 27) / 10_000_000 if complete else None,
    )


def _summarize(rows: list[dict], costs: list[dict]) -> dict:
    result = dict(requests=len(rows), answerable=sum(row["answerable"] for row in rows))
    result["unanswerable"] = len(rows) - result["answerable"]
    result.update({field: sum(row[field] for row in rows) for field in QUALITY_FIELDS})
    result["effective_correct_answers"] = result.pop("effective_correct_answer")
    result["effective_rate"] = (
        result["effective_correct_answers"] / result["answerable"] if result["answerable"] else None
    )
    result["correct_abstention_rate"] = (
        result["correct_abstention"] / result["unanswerable"] if result["unanswerable"] else None
    )
    result["failed_queries"] = sum(bool(row["failures"]) for row in rows)
    result["accounting_complete"] = bool(costs) and all(
        cost["accounting_complete"] for cost in costs
    )
    result.update({field: _sum_known([cost[field] for cost in costs]) for field in COST_FIELDS})
    result["diagnostics"] = dict(sorted(Counter(row["diagnostic"] for row in rows).items()))
    return result


def _quantiles(values: list[float]) -> list[float] | None:
    if not values or any(value is None for value in values):
        return None
    values = sorted(values)
    bounds = []
    for probability in (
        (1 - BOOTSTRAP_PROTOCOL.confidence_level) / 2,
        (1 + BOOTSTRAP_PROTOCOL.confidence_level) / 2,
    ):
        position = (len(values) - 1) * probability
        low, high = math.floor(position), math.ceil(position)
        bounds.append(values[low] + (values[high] - values[low]) * (position - low))
    return bounds


def _metrics(summaries: list[dict]) -> dict:
    values = {
        field: _sum_known([row[field] for row in summaries])
        for field in ("requests", "answerable", "effective_correct_answers", *COST_FIELDS)
    }
    denominator = values["effective_correct_answers"]
    values["effective_rate"] = denominator / values["answerable"] if values["answerable"] else None
    for name in (
        "total_tokens",
        "total_calls",
        "raw_source_reads",
        "whole_seconds",
        "tariff_estimated_cny",
    ):
        values[name + "_per_effective_answer"] = (
            values[name] / denominator if denominator and values[name] is not None else None
        )
    return {key: value for key, value in values.items() if key not in {"requests", "answerable"}}


def _compare(grouped: dict, comparator: str) -> dict:
    if CANDIDATE not in grouped or comparator not in grouped:
        return dict(candidate=CANDIDATE, baseline=comparator, status="not_run", metrics={})
    groups = sorted(grouped[CANDIDATE])
    if set(groups) != set(grouped[comparator]):
        raise ValueError("unpaired source groups")

    def difference(sample: list[str]) -> dict:
        left = _metrics([grouped[CANDIDATE][group] for group in sample])
        right = _metrics([grouped[comparator][group] for group in sample])
        return {
            key: left[key] - right[key]
            if left[key] is not None and right[key] is not None
            else None
            for key in left
        }

    point = difference(groups)
    per_group = [dict(group_id=group, differences=difference([group])) for group in groups]
    draws, draw_hash = [], sha256()
    enough = len(groups) >= BOOTSTRAP_PROTOCOL.minimum_groups
    if enough:
        rng = random.Random(BOOTSTRAP_PROTOCOL.seed)
        for _ in range(BOOTSTRAP_PROTOCOL.resamples):
            indices = [rng.randrange(len(groups)) for _ in groups]
            draw_hash.update((_digest(indices) + "\n").encode())
            draws.append(difference([groups[index] for index in indices]))
    metrics = {}
    reasons = [] if enough else ["fewer_than_two_source_groups_no_interval"]
    for name, value in point.items():
        values = [draw[name] for draw in draws]
        undefined = sum(item is None for item in values)
        metrics[name] = dict(
            difference=value,
            interval=_quantiles(values),
            undefined_resamples=undefined,
        )
        if undefined:
            reasons.append("undefined_resamples:" + name)
    return dict(
        candidate=CANDIDATE,
        baseline=comparator,
        status="descriptive_only",
        direction="candidate_minus_baseline",
        group_count=len(groups),
        request_count=sum(grouped[CANDIDATE][group]["requests"] for group in groups),
        resampling_unit="whole_source_group",
        resamples=len(draws),
        protocol_sha256=BOOTSTRAP_PROTOCOL.fingerprint,
        confidence_level=BOOTSTRAP_PROTOCOL.confidence_level,
        draws_sha256=draw_hash.hexdigest() if enough else None,
        small_n_warning="descriptive and unstable; repeated queries do not add independent groups",
        production_promotion=False,
        reasons=reasons,
        per_group=per_group,
        metrics=metrics,
    )


def evaluate(results: list[dict], gold: dict) -> dict:
    """Evaluate complete episode/arm cells; invalid/missing answers count as failures.

    ``gold`` may contain frozen dataclass instances or their JSON dictionaries.
    Runtime failure records or absent episode/arm cells reject report construction.
    The runner owns durable failure retention and knows the selected full manifest.
    """
    if not isinstance(results, list) or not results or not isinstance(gold, Mapping):
        raise ValueError("nonempty result cells and independent gold are required")
    cells, by_episode, episode_groups, arms = {}, defaultdict(list), {}, set()
    gold_cache = {}
    for result in results:
        if not isinstance(result, Mapping) or result.get("status") != "complete":
            raise ValueError("incomplete episode cell; retain failure artifact before evaluation")
        episode, arm, group = (result.get(name) for name in ("episode_id", "arm", "group_id"))
        if any(not isinstance(value, str) or not value for value in (episode, arm, group)):
            raise ValueError("invalid episode/arm/group identity")
        if (episode, arm) in cells:
            raise ValueError("duplicate episode/arm cell")
        if episode in episode_groups and episode_groups[episode] != group:
            raise ValueError("episode changes source group across arms")
        if not isinstance(result.get("rows"), list):
            raise ValueError("missing episode rows")
        cells[episode, arm] = result
        episode_groups[episode] = group
        by_episode[episode].append(result)
        arms.add(arm)
    if len(cells) != len(by_episode) * len(arms):
        raise ValueError("missing episode/arm cell; no complete report")
    expectations, completeness = {}, True
    for episode, records in by_episode.items():
        declared = [record.get("expected_query_ids") for record in records]
        has_declarations = all(isinstance(value, list) for value in declared)
        if has_declarations:
            expected = declared[0]
            if not expected or any(value != expected for value in declared):
                raise ValueError("inconsistent expected query manifest")
            if any(not isinstance(value, str) or not value for value in expected) or len(
                set(expected)
            ) != len(expected):
                raise ValueError("invalid expected query manifest")
        else:
            completeness = False
            expected = sorted(
                {
                    row.get("query_id")
                    for record in records
                    for row in record["rows"]
                    if isinstance(row, Mapping) and isinstance(row.get("query_id"), str)
                }
            )
        if not expected:
            raise ValueError("episode has no declared or observed queries")
        expectations[episode] = expected
        for query_id in expected:
            if query_id not in gold:
                raise ValueError(f"missing independent gold: {query_id}")
            gold_cache[query_id] = _gold(gold[query_id], query_id)
    scored, costs = [], []
    rows_by_arm, costs_by_arm = defaultdict(list), defaultdict(list)
    group_rows, group_costs = (
        defaultdict(lambda: defaultdict(list)),
        defaultdict(lambda: defaultdict(list)),
    )
    for (episode, arm), result in sorted(cells.items()):
        row_map = {}
        for row in result["rows"]:
            if not isinstance(row, Mapping) or row.get("query_id") not in expectations[episode]:
                raise ValueError("unknown or unidentifiable query row")
            query_id = row["query_id"]
            if query_id in row_map:
                raise ValueError("duplicate query row")
            if (
                row.get("arm", arm) != arm
                or row.get("group_id", result["group_id"]) != result["group_id"]
            ):
                raise ValueError("query row identity conflicts with episode cell")
            row_map[query_id] = row
        group = result["group_id"]
        for query_id in expectations[episode]:
            score = _score(row_map.get(query_id), gold_cache[query_id], query_id, group, arm)
            score["episode_id"] = episode
            scored.append(score)
            rows_by_arm[arm].append(score)
            group_rows[arm][group].append(score)
        cost = dict(episode_id=episode, group_id=group, arm=arm, **_cost(result))
        costs.append(cost)
        costs_by_arm[arm].append(cost)
        group_costs[arm][group].append(cost)
    summaries = {arm: _summarize(rows_by_arm[arm], costs_by_arm[arm]) for arm in sorted(arms)}
    grouped = {
        arm: {
            group: _summarize(rows, group_costs[arm][group])
            for group, rows in sorted(group_rows[arm].items())
        }
        for arm in sorted(arms)
    }
    categories = {
        arm: {
            category: _summarize(
                [row for row in rows_by_arm[arm] if row["category"] == category], []
            )
            for category in sorted({row["category"] for row in rows_by_arm[arm]})
        }
        for arm in sorted(arms)
    }
    return dict(
        schema=SCHEMA,
        evidence_class="synthetic-diagnostic-not-public-benchmark",
        evaluator="offline deterministic normalized-substring and versioned exact-quote checks",
        production_promotion=False,
        novelty_claim="none; comparison cannot establish innovation",
        completeness=dict(
            observed_episode_arm_matrix_complete=True,
            expected_query_manifests_verified=completeness,
            selected_episode_manifest_authority=(
                "runner; evaluator cannot infer wholly missing selected episodes"
            ),
        ),
        limitations=[
            "Strict lexical matching may reject valid paraphrases; it is not semantic entailment.",
            "Required values and qualifiers are checked separately in current cited quotes.",
            "Unsupported extra prose and uncited stale/revoked facts are not fully detectable.",
            "Missing raw/retrieval snapshots are unknown, never evidence of absence.",
            "Stage localization is observational; causal mechanism attribution remains unknown.",
            "Correct abstention uses a valid abstain declaration, separate from effective answers.",
            "Small group samples yield descriptive, unstable intervals; no production promotion.",
            "Observed wall time can include different shared-extraction wait times across arms.",
            "Wall time is attempt-local; checkpoint replay does not recover prior provider latency.",
        ],
        bootstrap_protocol=asdict(BOOTSTRAP_PROTOCOL),
        bootstrap_protocol_sha256=BOOTSTRAP_PROTOCOL.fingerprint,
        cost_basis=dict(
            currency="CNY",
            input_per_million_tokens=INPUT_CNY_PER_MILLION,
            output_per_million_tokens=OUTPUT_CNY_PER_MILLION,
            classification="tariff estimate, not provider bill",
            scope=(
                "logical treatment cost: whole-episode receipts including shared extraction, "
                "gap and answer calls"
            ),
            latency="attempt-local observed wall time; not cumulative provider or end-to-end latency",
            raw_reads="online raw-source reads; initial extraction source enumeration excluded",
            categories="costs at whole-episode/source-group level; no query-category allocation",
        ),
        observed_provider_usage=_provider_usage(results),
        arms=summaries,
        groups=grouped,
        categories=categories,
        episode_costs=costs,
        comparisons={f"{CANDIDATE}_minus_{arm}": _compare(grouped, arm) for arm in COMPARATORS},
        rows=scored,
    )
