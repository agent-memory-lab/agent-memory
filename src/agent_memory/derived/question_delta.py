"""Bounded deterministic, keyed project maintenance with complete group state.

The census/security scan is intentionally retained. Delta saves semantic group
work, not admission or permission checks. No model patch or approximate frontier.
"""

import json
from collections import Counter
from copy import deepcopy
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from ..conditions import applicability, instant
from ..serialization import to_jsonable
from .model import DerivedError, digest
from .project_questions import (
    ProjectQuestionResult,
    ProjectRowResult,
    _deadline,
    _field,
    _row_status,
    _validate_values,
    full_project_question,
)
from .question_model import AnswerStatus

SCHEMA = "project-keyed-delta/1"
OPERATOR = "project-complete-groups/2"
LOG_SCHEMA = "project-change-window/1"
LOG_LIMIT = 128


async def record_change(uow, scope, key, generation, *, before=None, after=None, reason):
    """Opaque bounded commit log on the same transaction as its route barrier.

    Store no source/candidate names or bodies. Missing history after rollover is
    a full-compute reason. A coordinated old-writer upgrade remains required.
    """
    old = await uow.derived_get(scope, "question_change_log", key)
    valid_old = (
        old
        and old.get("schema") == LOG_SCHEMA
        and old.get("sha256") == digest({k: v for k, v in old.items() if k != "sha256"})
        and old.get("through") in {generation - 1, generation}
    )
    entries = old.get("entries", []) if valid_old else []
    if entries and entries[-1]["sequence"] == generation:
        entries = entries[:-1]  # Enrich the same transaction's generic barrier record.
    if entries and entries[-1]["sequence"] != generation - 1:
        entries = []
    entry = dict(sequence=generation, before=digest(before), after=digest(after), reason=reason)
    entries = [*entries, entry][-LOG_LIMIT:]
    row = dict(schema=LOG_SCHEMA, entries=entries, through=generation)
    await uow.derived_put(scope, "question_change_log", key, {**row, "sha256": digest(row)})


def continuous(old, new, logs):
    """Canonicalize duplicates/order, rejecting conflicts and every missing step."""
    if set(old) != set(new):
        return False
    for key, finish in new.items():
        start = old[key]
        if type(start) is not int or type(finish) is not int or finish < start:
            return False
        if start == finish:
            continue
        if finish - start > LOG_LIMIT:
            return False
        row = logs.get(key)
        if (
            not row
            or row.get("schema") != LOG_SCHEMA
            or row.get("sha256") != digest({k: v for k, v in row.items() if k != "sha256"})
            or row.get("through") != finish
        ):
            return False
        entries = {}
        if len(row.get("entries", [])) > LOG_LIMIT * 2:
            return False
        for entry in row.get("entries", []):
            sequence = entry.get("sequence")
            if type(sequence) is not int or sequence < 1:
                return False
            if sequence in entries and entries[sequence] != entry:
                return False
            entries[sequence] = entry
        if set(range(start + 1, finish + 1)) - set(entries):
            return False
    return True


class KeyedAggregate:
    """Old-remove/new-add set, count, distinct, sum/min/max for exact integer units.

    Unknown and missing values have independent counts and never become zero.
    Retaining every keyed contribution makes deletion of extrema and duplicate
    values correct. Arbitrary numeric coercion and mixed-unit arithmetic fail.
    """

    def __init__(self, unit, contributions=()):
        if type(unit) is not str or not unit or len(unit) > 128:
            raise DerivedError("delta_unit_required")
        self.unit, self.members = unit, {}
        for key, value in contributions:
            self.replace(key, value, unit=unit)

    def replace(self, key, value, *, unit, expected=None):
        if unit != self.unit or type(key) is not str or not key:
            raise DerivedError("delta_unit_mismatch")
        if not (type(value) is str and value in {"unknown", "missing"}) and type(value) is not int:
            raise DerivedError("delta_aggregate_unsupported")
        if expected is not None and self.members.get(key) != expected:
            raise DerivedError("delta_old_contribution_mismatch")
        self.members.pop(key, None)
        self.members[key] = value

    def remove(self, key, *, expected=None):
        if key not in self.members or (expected is not None and self.members[key] != expected):
            raise DerivedError("delta_old_contribution_mismatch")
        del self.members[key]

    def result(self):
        refs = Counter(v for v in self.members.values() if type(v) is int)
        return dict(
            unit=self.unit,
            count=len(self.members),
            known_count=sum(refs.values()),
            unknown_count=sum(v == "unknown" for v in self.members.values()),
            missing_count=sum(v == "missing" for v in self.members.values()),
            distinct=sorted(refs),
            reference_counts=[[k, refs[k]] for k in sorted(refs)],
            sum=sum(k * n for k, n in refs.items()) if refs else None,
            min=min(refs) if refs else None,
            max=max(refs) if refs else None,
        )


def compatibility(contract, snapshot):
    from .question_dependencies import OPERATOR as READSET_OPERATOR, READSET_SCHEMA

    return digest(
        dict(
            operator=OPERATOR,
            contract=contract.fingerprint,
            instance=snapshot["instance"],
            question=snapshot["question"],
            overdue=snapshot["overdue_only"],
            readset_schema=READSET_SCHEMA,
            readset_operator=READSET_OPERATOR,
            readset=snapshot["definition"].get("spec", {}).get("semantic_readset"),
        )
    )


def _groups(contract, census, question):
    facts = census.facts
    if question in {"owner", "status"}:
        from .question_dependencies import predicates

        names = set(predicates(contract, question))
        return {
            census.project_id: tuple(f for f in facts if f.fact.predicate in names)
        }
    prefix = "commitment." if question == "commitments" else "risk."
    groups = {}
    for item in facts:
        if item.fact.predicate.startswith(prefix):
            groups.setdefault(item.fact.entity_id, []).append(item)
    if question == "risks":
        for rule in contract.risk_rules:
            groups["rule:" + rule.id] = [f for f in facts if f.fact.predicate == rule.predicate]
    return {key: tuple(values) for key, values in groups.items()}


def _signature(items, context, rule, overdue):
    return digest(
        [
            to_jsonable(items),
            [
                [
                    i.possibly_active(context.valid_at, context.known_at),
                    applicability(i.fact.conditions, i.fact.exceptions, context),
                    context.valid_at >= _deadline(i.fact.value)
                    if overdue and i.fact.predicate == "commitment.deadline"
                    else None,
                ]
                for i in items
            ],
            applicability(rule.impact_conditions, (), context) if rule else None,
        ]
    )


def _group(contract, census, question, key, items, overdue):
    context = census.context
    active = [i for i in items if i.possibly_active(context.valid_at, context.known_at)]

    def field(name, multiple=False):
        return _field(
            name, [i for i in active if i.fact.predicate == name], context, multiple=multiple
        )

    if question in {"owner", "status"}:
        names = ["project.owner"] if question == "owner" else ["project.status"]
        if question == "status" and contract.require_phase:
            names.append("project.phase")
        fields = tuple(
            field(n, question == "owner" and contract.owner_cardinality == "multiple")
            for n in names
        )
        state = _row_status(fields)
        return ProjectRowResult(
            key, state, True if state == AnswerStatus.RESOLVED else None, fields
        )
    if question == "risks" and key.startswith("rule:"):
        rule = next(r for r in contract.risk_rules if key == "rule:" + r.id)
        source = field(rule.predicate)
        condition = applicability(rule.impact_conditions, (), context)
        state, matches = source.status, None
        if condition is False:
            matches, state = False, AnswerStatus.RESOLVED
        elif condition is None:
            state = AnswerStatus.UNKNOWN
        elif source.status == AnswerStatus.RESOLVED:
            matches = source.value == rule.equals
        return ProjectRowResult(key, state, matches, (source,), "inferred", rule)
    if not active:
        return None
    if question == "commitments":
        fields = tuple(
            field(n)
            for n in (
                "commitment.promisor",
                "commitment.action",
                "commitment.state",
                "commitment.deadline",
            )
        )
        state, matches = _row_status(fields if overdue else fields[:3]), None
        observed = fields[2].value
        if observed in contract.commitment_terminal_states:
            matches = False
        elif observed in contract.commitment_open_states and state == AnswerStatus.RESOLVED:
            matches = not overdue or context.valid_at >= _deadline(fields[3].value)
        elif observed == "unknown":
            state = AnswerStatus.UNKNOWN
    else:
        fields = (field("risk.label"), field("risk.state"))
        state, matches = _row_status(fields), None
        if fields[1].value == "closed":
            matches = False
        elif fields[1].value == "open" and state == AnswerStatus.RESOLVED:
            matches = True
    return ProjectRowResult(key, state, matches, fields)


def _metadata(contract, census, question, rows, overdue):
    """Cheap global proof/time fold; no full semantic field evaluation."""
    context = census.context
    active = [i for i in census.facts if i.possibly_active(context.valid_at, context.known_at)]
    transitions = set()
    for item in census.facts:
        f = item.fact
        if f.known_from <= context.known_at and (
            f.known_to is None or context.known_at < f.known_to
        ):
            transitions.update(t for t in (f.valid_from, f.valid_to) if t and t > context.valid_at)
    if any(i.fact.conditions or i.fact.exceptions for i in active) or any(
        r.impact_conditions for r in contract.risk_rules
    ):
        local = context.valid_at.astimezone(ZoneInfo(contract.timezone))
        transitions.add(
            instant(datetime.combine(local.date() + timedelta(days=1), time(), local.tzinfo))
        )
    if overdue:
        for row in rows:
            fields = row["fields"]
            if (
                row["status"] == "resolved"
                and fields[2]["known_values"]
                and fields[2]["known_values"][0] in contract.commitment_open_states
            ):
                deadline = _deadline(fields[3]["known_values"][0])
                if deadline > context.valid_at:
                    transitions.add(deadline)
    reasons = set(census.coverage.incomplete_reasons)
    if any({"subject_id", "predicate"} & set(i.missing_support) for i in active):
        reasons.add("candidate_membership_unproved")
    relevant = [r for r in rows if r["matches"] is not False]
    if reasons:
        status = "incomplete"
    elif any(r["status"] == "contested" for r in relevant):
        status = "contested"
    elif any(r["matches"] is None for r in relevant):
        status = "unknown"
    elif any(r["matches"] is True for r in relevant):
        status = "resolved"
    else:
        status = "empty" if question in {"commitments", "risks"} else "unknown"
    if status == "empty":
        reasons.add("no_matches_in_complete_known_scope")
    refs = sorted(
        {s for i in census.facts for s in i.source_references},
        key=lambda s: (s.source_event_id, s.start, s.end, s.quote),
    )
    # Same public oracle schema, with a separately truthful computation trace.
    result = ProjectQuestionResult(
        question,
        contract.fingerprint,
        census.id,
        digest(to_jsonable(census)),
        tuple(refs),
        context.valid_at,
        context.known_at,
        AnswerStatus(status),
        (),
        census.coverage,
        tuple(sorted(reasons)),
        min(transitions, default=None),
        overdue,
    ).payload()
    result = json.loads(json.dumps(result))
    result["rows"] = rows
    result["matched_ids"] = [r["id"] for r in rows if r["matches"] is True]
    return result


def evaluate(contract, snapshot):
    census, question, overdue = (
        snapshot["census"].snapshot,
        snapshot["question"],
        snapshot["overdue_only"],
    )
    old = snapshot.get("delta_state")
    reason = None
    compatible = compatibility(contract, snapshot)
    if not old:
        reason = "missing_baseline"
    elif (
        old.get("schema") != SCHEMA
        or old.get("operator") != OPERATOR
        or old.get("compatibility") != compatible
        or old.get("sha256") != digest({k: v for k, v in old.items() if k != "sha256"})
    ):
        reason = "incompatible_baseline"
    elif not census.coverage.candidate_census_complete or census.coverage.truncation_reasons:
        reason = "incomplete_census"
    elif not continuous(
        old["query_generations"],
        snapshot["proof"]["query_generations"],
        snapshot.get("change_logs", {}),
    ):
        reason = "change_log_gap"
    elif not snapshot.get("generation_safe", False):
        reason = "original_generation_unsafe"
    groups = _groups(contract, census, question)
    rules = {"rule:" + r.id: r for r in contract.risk_rules}
    signatures = {
        key: _signature(items, census.context, rules.get(key), overdue)
        for key, items in groups.items()
    }
    if reason:
        result = json.loads(
            json.dumps(
                full_project_question(contract, census, question, overdue_only=overdue).payload()
            )
        )
        rows = {r["id"]: r for r in result["rows"]}
        evaluated = len(groups)
        mode = "full"
    else:
        _validate_values(contract, census)
        rows = deepcopy(old["rows"])
        changed = {
            key
            for key in set(old["signatures"]) | set(signatures)
            if old["signatures"].get(key) != signatures.get(key)
        }
        # Remove the complete old contribution before installing the new group.
        for key in changed:
            rows.pop(key, None)
            if key in groups:
                row = _group(contract, census, question, key, groups[key], overdue)
                if row:
                    rows[key] = json.loads(json.dumps(to_jsonable(row)))
        evaluated = len(changed & set(groups))
        result = _metadata(
            contract,
            census,
            question,
            sorted(rows.values(), key=lambda r: (r["id"], r["origin"])),
            overdue,
        )
        mode = "delta"
    contributions = {
        key: "unknown" if row["matches"] is None else int(row["matches"])
        for key, row in rows.items()
    }
    previous = old["contributions"] if not reason else {}
    counts = KeyedAggregate("matched-row", previous.items())
    for key in set(previous) | set(contributions):
        if previous.get(key) == contributions.get(key):
            continue
        if key in previous:
            counts.remove(key, expected=previous[key])
        if key in contributions:
            counts.replace(key, contributions[key], unit="matched-row")
    state = dict(
        schema=SCHEMA,
        operator=OPERATOR,
        compatibility=compatible,
        instance_id=snapshot["definition"]["facet_id"],
        signatures=signatures,
        rows=rows,
        query_generations=snapshot["proof"]["query_generations"],
        contributions=contributions,
        aggregate=counts.result(),
        matched_ids=result["matched_ids"],
    )
    state["sha256"] = digest(state)
    return (
        result,
        state,
        dict(
            compute_mode=mode,
            fallback_reason=reason,
            groups_evaluated=evaluated,
            groups_total=len(groups),
            semantic_dirty_count=snapshot["definition"].get("semantic_dirty_count", 0),
            proof_dirty_count=snapshot["definition"].get("proof_dirty_count", 0),
            predicate_disjoint_count=snapshot["definition"].get("predicate_disjoint_count", 0),
            last_invalidation=deepcopy(snapshot["definition"].get("last_invalidation")),
        ),
    )
