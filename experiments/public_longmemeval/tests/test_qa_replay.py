import asyncio
import json
import os
from pathlib import Path

import pytest

from experiments.public_longmemeval import replay_qa


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def prepare(tmp_path):
    source = tmp_path / "source"
    write(source / "ALL_RESULTS.json", {"row_origins": {"a": "cases/a/result.json"}})
    write(source / "prepared/blind-manifest.json", {"case_ids": ["a"]})
    write(
        source / "cases/a/result.json",
        {
            "reader_raw": "OLD RESPONSE MUST NOT ENTER GENERATION",
            "status": "completed",
            "judge_correct": True,
            "hypothesis": "OLD ANSWER MUST NOT ENTER GENERATION",
            "reference": "GOLD MUST NOT ENTER GENERATION",
            "delivered": [{"text": "user: I painted a blue cube."}],
        },
    )
    write(
        source / "prepared/blind/a/query.json",
        {"case_id": "a", "question": "What did I paint?", "question_date": None},
    )
    write(
        source / "prepared/evaluator-only/gold.jsonl",
        {
            "case_id": "a",
            "question_type": "single-session-user",
            "answer": "GOLD ANSWER",
            "abstention": False,
        },
    )
    prior = tmp_path / "prior.sqlite3"
    prior.write_text("frozen", encoding="utf-8")
    rubric = tmp_path / "rubric.py"
    rubric.write_text(
        "def get_anscheck_prompt(kind, question, answer, hypothesis, flag):\n"
        "    return answer + hypothesis\n",
        encoding="utf-8",
    )
    helper = tmp_path / "client.py"
    helper.write_text("UNUSED=True", encoding="utf-8")
    output = tmp_path / "output"
    plan = replay_qa.prepare(
        source, output, transport_path=helper, prior_ledger=prior, rubric_path=rubric
    )
    return output, plan


def test_prepare_is_blind_frozen_and_does_not_open_gold_or_start_api(tmp_path):
    output, plan = prepare(tmp_path)
    request = (output / "requests/a.json").read_text(encoding="utf-8")
    assert all(secret not in request for secret in ("OLD RESPONSE", "OLD ANSWER", "GOLD"))
    assert plan["case_ids"] == ["a"] and not (output / "api-ledger.sqlite3").exists()
    replay_qa.verify(output, plan)
    write(output / "requests/a.json", {"tampered": True})
    with pytest.raises(ValueError, match="blind_request_changed"):
        replay_qa.verify(output, plan)


@pytest.mark.skipif(
    os.name != "posix", reason="Paid replay requires POSIX private credential permissions"
)
def test_actual_runner_records_draft_check_then_freezes_before_gold_judge(tmp_path, monkeypatch):
    output, plan = prepare(tmp_path)
    credential = tmp_path / "credential"
    credential.write_text("fake-credential", encoding="utf-8")
    credential.chmod(0o600)
    pointer = tmp_path / "pointer"
    pointer.write_text(str(credential), encoding="utf-8")
    calls = []

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["prior_ledger_path"] == plan["prior_ledger"]
            self.ledger = kwargs["ledger_path"]

        def summary(self):
            return {
                "remaining_cny": "0.8",
                "mock": True,
                "segment_cost_cny": "0",
                "combined_total_cny": "0",
            }

        async def close(self):
            import sqlite3
            from contextlib import closing

            with closing(sqlite3.connect(self.ledger)) as connection:
                connection.execute(
                    "CREATE TABLE calls(id TEXT,status TEXT,case_id TEXT,prompt_tokens INTEGER,"
                    "completion_tokens INTEGER,charged_nano_cny INTEGER,reserved_nano_cny INTEGER)"
                )
                connection.executemany(
                    "INSERT INTO calls VALUES(?,?,?,?,?,?,?)",
                    [(str(i), "completed", "a", 10, 10, 1000, 1000) for i in range(3)],
                )
                connection.commit()
            calls.append("closed")

        async def chat(self, messages, *, phase, **kwargs):
            prompt = messages[0]["content"]
            calls.append(phase)
            if phase == "judge":
                assert (output / "answers-frozen.json").exists() and "GOLD ANSWER" in prompt
                return "yes", {"mock": True}
            assert "GOLD ANSWER" not in prompt
            if "AUDIT_JSON=" in prompt:
                return json.dumps({"decision": "supported", "reason": "Explicit action."}), {
                    "mock": True
                }
            return json.dumps(
                {
                    "schema_version": "evidence-reader/4",
                    "mode": "fact",
                    "answer": "A blue cube.",
                    "supporting_quotes": [{"evidence_id": "E1", "text": "I painted a blue cube."}],
                    "missing_information": [],
                    "computation": {"kind": "none", "operands": [], "unit": ""},
                }
            ), {"mock": True}

    from types import SimpleNamespace

    monkeypatch.setattr(
        replay_qa, "load_module", lambda *args: SimpleNamespace(BudgetedClient=Client)
    )
    asyncio.run(replay_qa.execute(output, credential_pointer=pointer))
    assert calls == ["answer", "answer", "judge", "closed"]
    result = json.loads((output / "completion.json").read_text(encoding="utf-8"))
    assert result["correct"] == 1 and result["full_dataset_completed"] is False
    assert set(p.name for p in (output / "calls/a").glob("*.json")) == {"draft.json", "verify.json"}
    from experiments.public_longmemeval.qa_report import report

    projected = report(output)
    assert projected["acceptance"]["effective_correct"] == 1
    assert projected["acceptance"]["cost"]["model_calls"] == 3
    assert projected["acceptance"]["cost"]["invoiced_microunits"] is None
    judgment = output / "judgments/a.json"
    saved_judgment = judgment.read_bytes()
    wrong = json.loads(saved_judgment)
    wrong["correct"] = False
    write(judgment, wrong)
    with pytest.raises(ValueError, match="judge_verdict_mismatch"):
        report(output)
    judgment.write_bytes(saved_judgment)
    completion_path = output / "completion.json"
    saved_completion = completion_path.read_bytes()
    wrong = json.loads(saved_completion)
    wrong["correct"] = 0
    write(completion_path, wrong)
    with pytest.raises(ValueError, match="completion_summary_mismatch"):
        report(output)
    completion_path.write_bytes(saved_completion)
    destination = tmp_path / "new-reports"
    report(output, destination=destination)
    assert (destination / "REPORT_V3.json").exists()
    offline = replay_qa.replay_candidates(output, tmp_path / "offline")
    assert len(offline["cases"]) == 1 and offline["new_accuracy"] is None
    assert offline["paid_calls"] == 0 and offline["gold_opened"] is False
    # A later working-source edit must not invalidate the frozen runtime copy.
    Path(plan["transport_path"]).write_text("WORKING_COPY_CHANGED=True", encoding="utf-8")
    projected = report(output)
    assert projected["paired"]["before_correct"] == 1
    with pytest.raises(ValueError, match="runtime_changed_since_freeze"):
        replay_qa.verify(output, plan)
    with pytest.raises(ValueError, match="existing_ledger"):
        # Preserve the finished attempt; verify would fail first for the edited
        # source, so restore it from the frozen snapshot for this admission check.
        Path(plan["transport_path"]).write_bytes((output / "runtime/client.py").read_bytes())
        asyncio.run(replay_qa.execute(output, credential_pointer=pointer))


@pytest.mark.parametrize("mode", ["headroom", "transport_error", "persist_error"])
@pytest.mark.skipif(
    os.name != "posix", reason="Paid replay requires POSIX private credential permissions"
)
def test_budget_and_failure_boundaries_never_retry_and_always_close(tmp_path, monkeypatch, mode):
    output, plan = prepare(tmp_path)
    credential = tmp_path / "credential"
    credential.write_text("fake", encoding="utf-8")
    credential.chmod(0o600)
    pointer = tmp_path / "pointer"
    pointer.write_text(str(credential), encoding="utf-8")
    calls = []

    class Client:
        def __init__(self, **kwargs):
            pass

        def summary(self):
            return {
                "remaining_cny": "0.01" if mode == "headroom" else "0.8",
                "segment_cost_cny": "0",
                "combined_total_cny": "0",
            }

        async def chat(self, *args, **kwargs):
            calls.append("dispatch")
            raise RuntimeError("mock_provider_failure")

        async def close(self):
            calls.append("closed")

    from types import SimpleNamespace

    monkeypatch.setattr(
        replay_qa, "load_module", lambda *args: SimpleNamespace(BudgetedClient=Client)
    )
    original = replay_qa.atomic_write_json

    def writer(path, value):
        if mode == "persist_error" and Path(path).name == "budget-final.json":
            raise OSError("mock disk failure")
        return original(path, value)

    monkeypatch.setattr(replay_qa, "atomic_write_json", writer)

    def invocation():
        return asyncio.run(replay_qa.execute(output, credential_pointer=pointer))

    if mode == "persist_error":
        with pytest.raises(OSError, match="mock disk"):
            invocation()
    else:
        invocation()
    assert calls == (["closed"] if mode == "headroom" else ["dispatch", "closed"])
    assert (output / "answers-frozen.json").exists()
    assert not list((output / "judgments").glob("*.json"))


def test_containment_and_duplicate_output_are_admission_errors(tmp_path):
    output, plan = prepare(tmp_path)
    with pytest.raises(ValueError, match="new_output"):
        replay_qa.prepare(
            tmp_path / "source",
            output,
            transport_path=plan["transport_path"],
            prior_ledger=plan["prior_ledger"],
            rubric_path=plan["rubric_path"],
        )
    for options in (
        {"selected_case_ids": ["unknown"]},
        {"selected_case_ids": ["a", "a"]},
        {"judge_headroom_cny": 0},
    ):
        with pytest.raises(ValueError):
            replay_qa.prepare(
                tmp_path / "source",
                tmp_path / "new-output",
                transport_path=plan["transport_path"],
                prior_ledger=plan["prior_ledger"],
                rubric_path=plan["rubric_path"],
                **options,
            )
    with pytest.raises(ValueError, match="outside"):
        replay_qa._contained(tmp_path / "source", "../prior.sqlite3")


def test_command_line_preflight_performs_no_transport_calls(tmp_path, monkeypatch, capsys):
    output, plan = prepare(tmp_path)
    monkeypatch.setattr(
        "sys.argv",
        [
            "replay_qa",
            "--source",
            str(tmp_path / "source"),
            "--output",
            str(tmp_path / "preflight"),
            "--transport",
            plan["transport_path"],
            "--prior-ledger",
            plan["prior_ledger"],
            "--rubric",
            plan["rubric_path"],
        ],
    )
    replay_qa.main()
    assert json.loads(capsys.readouterr().out)["paid_calls"] == 0


def test_monetary_projection_uses_integer_rounding_without_precision_loss():
    from experiments.public_longmemeval.qa_report import nanos_to_microunits

    value = 9007199254740993001
    assert nanos_to_microunits(value) == (value + 999) // 1000
    for invalid in (-1, True, 1.5, None):
        with pytest.raises(ValueError, match="invalid_nano_cny"):
            nanos_to_microunits(invalid)


def test_paid_replay_refuses_unsupported_permissions_before_loading_any_files(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    monkeypatch.setattr(replay_qa, "os", SimpleNamespace(name="nt"))
    with pytest.raises(ValueError, match="paid_replay_requires_posix_private_credentials"):
        asyncio.run(
            replay_qa.execute(tmp_path / "missing-run", credential_pointer=tmp_path / "missing")
        )
    assert not (tmp_path / "missing-run").exists()
