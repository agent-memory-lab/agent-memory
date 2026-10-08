"""Q7-17/18/19: replay every delta against the independent B3 full oracle."""

import json
import random
from dataclasses import replace
from types import SimpleNamespace

import pytest
import test_project_questions_v7 as p

from agent_memory.conditions import Condition
from agent_memory.derived.model import DerivedError, digest
from agent_memory.derived.project_questions import ProjectRiskRule, full_project_question
from agent_memory.derived.question_delta import (
    LOG_SCHEMA,
    KeyedAggregate,
    continuous,
    evaluate,
)

contract = p.contract


def work(contract, facts, question, *, old=None, sequence=0, at=10, overdue=False, pending=()):
    census = p.snapshot(contract, facts, valid_at=p.at(at), known_at=p.at(at), pending=pending)
    entries = [
        dict(sequence=n, before=digest(n - 1), after=digest(n), reason="test")
        for n in range(1, sequence + 1)
    ]
    log = dict(schema=LOG_SCHEMA, through=sequence, entries=entries)
    return dict(
        instance={"definition": contract.fingerprint},
        definition={"facet_id": "instance"},
        census=SimpleNamespace(snapshot=census),
        question=question,
        overdue_only=overdue,
        proof={"query_generations": {"route": sequence}},
        delta_state=old,
        change_logs={"route": {**log, "sha256": digest(log)}},
        generation_safe=True,
    )


def oracle(contract, data):
    return json.loads(
        json.dumps(
            full_project_question(
                contract,
                data["census"].snapshot,
                data["question"],
                overdue_only=data["overdue_only"],
            ).payload()
        )
    )


@pytest.mark.parametrize(
    "question,overdue",
    [
        ("owner", False),
        ("status", False),
        ("commitments", False),
        ("commitments", True),
        ("risks", False),
    ],
)
def test_randomized_add_modify_remove_withdraw_qualification_and_time_replay(
    contract, question, overdue
):
    contract = replace(
        contract,
        require_phase=True,
        risk_rules=(ProjectRiskRule("blocked", "1", "project.status", "blocked", "Blocked"),),
    )
    rng = random.Random(717)
    values = {
        "project.owner": ("alice", "bob"),
        "project.status": ("active", "blocked"),
        "project.phase": ("design", "build"),
        "commitment.promisor": ("alice", "bob"),
        "commitment.action": ("ship", "review"),
        "commitment.state": ("open", "completed", "cancelled", "unknown"),
        "commitment.deadline": (p.at(15).isoformat(), p.at(50).isoformat()),
        "risk.label": ("Delay", "Dependency"),
        "risk.state": ("open", "closed", "unknown"),
    }
    facts, old = {}, None
    saw_delta = False
    for step in range(110):
        name = rng.choice(tuple(values))
        entity = "P" if name.startswith("project.") else rng.choice(("A", "B", "C"))
        key = entity + ":" + name + ":" + str(rng.randrange(2))
        if rng.random() < 0.22:
            facts.pop(key, None)
        else:
            facts[key] = p.qualified(
                name,
                rng.choice(values[name]),
                fact_id=key,
                entity=entity,
                valid_from=p.at(rng.choice((0, 12, 35))),
                valid_to=p.at(70) if rng.random() < 0.2 else None,
                known_from=p.at(rng.choice((0, 20))),
                conditions=(Condition("eq", "region", "US"),) if rng.random() < 0.15 else (),
                support=("subject_id", "predicate", "valid_from") if rng.random() < 0.1 else None,
            )
        data = work(
            contract,
            facts.values(),
            question,
            old=old,
            sequence=step,
            at=10 + step,
            overdue=overdue,
            pending=("pending",) if step % 9 == 0 else (),
        )
        result, old, trace = evaluate(contract, data)
        assert result == oracle(contract, data), (question, step)
        assert old["aggregate"]["sum"] == (
            sum(r["matches"] is True for r in result["rows"])
            if any(r["matches"] is not None for r in result["rows"])
            else None
        )
        saw_delta |= trace["compute_mode"] == "delta"
    assert saw_delta


def test_only_affected_complete_group_recomputed_without_full_oracle(contract, monkeypatch):
    import agent_memory.derived.question_delta as delta

    facts = p.commitment(entity="A") + p.commitment(entity="B")
    initial = work(contract, facts, "commitments")
    _, state, _ = evaluate(contract, initial)
    updated = p.commitment(entity="A", state="completed") + p.commitment(entity="B")
    data = work(contract, updated, "commitments", old=state, sequence=1)
    expected = oracle(contract, data)
    monkeypatch.setattr(delta, "full_project_question", lambda *a, **kw: pytest.fail("full called"))
    result, next_state, trace = evaluate(contract, data)
    assert result == expected
    assert trace["groups_evaluated"] == 1 and trace["groups_total"] == 2
    assert next_state["matched_ids"] == ["B"]
    assert next_state["aggregate"]["sum"] == 1


@pytest.mark.parametrize("damage", ["gap", "conflict", "operator", "definition", "unsafe", "state"])
def test_unproved_delta_forces_real_full_without_false_frontier(contract, damage):
    data = work(contract, p.commitment(), "commitments")
    _, state, _ = evaluate(contract, data)
    data = work(contract, p.commitment(state="completed"), "commitments", old=state, sequence=2)
    if damage in {"gap", "conflict"}:
        log = data["change_logs"]["route"]
        if damage == "gap":
            log["entries"].pop(0)
        else:
            log["entries"].append({**log["entries"][0], "reason": "conflict"})
        log["sha256"] = digest({k: v for k, v in log.items() if k != "sha256"})
    elif damage == "operator":
        data["delta_state"]["operator"] = "unsupported/2"
    elif damage == "definition":
        data["instance"]["version"] = "2"
    elif damage == "unsafe":
        data["generation_safe"] = False
    else:
        data["delta_state"]["rows"] = {}
    result, rebuilt, trace = evaluate(contract, data)
    assert trace["compute_mode"] == "full"
    assert result == oracle(contract, data)
    assert rebuilt["query_generations"] == {"route": 2}


def test_log_duplicates_and_disorder_canonicalize_but_missing_sequence_does_not(contract):
    data = work(contract, (), "owner", sequence=3)
    row = data["change_logs"]["route"]
    row["entries"] = [row["entries"][2], row["entries"][0], row["entries"][1], row["entries"][0]]
    row["sha256"] = digest({k: v for k, v in row.items() if k != "sha256"})
    assert continuous({"route": 0}, {"route": 3}, data["change_logs"])
    assert not continuous({"route": 0}, {"route": 1_000_000_000}, data["change_logs"])


def test_keyed_aggregate_extrema_distinct_unknown_missing_unit_and_retractions():
    state = KeyedAggregate(
        "USD-cent", [("a", 10), ("b", 10), ("c", -3), ("d", "unknown"), ("e", "missing")]
    )
    assert state.result()["reference_counts"] == [[-3, 1], [10, 2]]
    state.remove("a", expected=10)
    assert state.result()["max"] == 10
    state.replace("b", 2, unit="USD-cent", expected=10)
    assert state.result()["sum"] == -1
    state.remove("c", expected=-3)
    assert state.result()["min"] == 2
    state.remove("b")
    result = state.result()
    assert result["min"] is result["max"] is result["sum"] is None
    assert result["unknown_count"] == result["missing_count"] == 1
    with pytest.raises(DerivedError):
        state.replace("bad", 2, unit="EUR-cent")
    with pytest.raises(DerivedError):
        state.replace("bad", True, unit="USD-cent")
    with pytest.raises(DerivedError):
        state.replace("d", 2, unit="USD-cent", expected=0)


def test_append_never_reseals_corrupt_prior_log_as_continuous():
    import asyncio

    from agent_memory.derived.question_delta import record_change

    class Ledger:
        def __init__(self):
            self.row = None

        async def derived_get(self, *args):
            return self.row

        async def derived_put(self, scope, kind, key, row):
            self.row = row

    async def run():
        ledger = Ledger()
        await record_change(ledger, None, "route", 1, reason="first")
        ledger.row["entries"][0]["after"] = digest("corrupt")
        await record_change(ledger, None, "route", 2, reason="new-commit")
        assert not continuous({"route": 0}, {"route": 2}, {"route": ledger.row})
        assert continuous({"route": 1}, {"route": 2}, {"route": ledger.row})

    asyncio.run(run())
