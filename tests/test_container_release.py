"""Release policy regressions: mutable refs and repeated tags must fail closed."""

import runpy
from pathlib import Path

import pytest

MODULE = runpy.run_path(str(Path(__file__).resolve().parents[1] / "tools/container_release.py"))


def package(*tags):
    return {"metadata": {"container": {"tags": list(tags)}}}


@pytest.mark.parametrize("tag", ["0.1.2", "sha-abc"])
def test_existing_version_or_source_cannot_be_overwritten(tag):
    with pytest.raises(ValueError, match="immutable"):
        MODULE["promotion_plan"]("0.1.2", "abc", [package(tag)])


def test_older_release_does_not_move_latest_backwards():
    assert not MODULE["promotion_plan"]("0.1.2", "abc", [package("0.1.3", "latest")])
    assert MODULE["promotion_plan"]("0.1.10", "abc", [package("0.1.9", "latest")])
    assert MODULE["promotion_plan"]("0.1.2", "abc", [package("candidate-123-1")])


@pytest.mark.parametrize("version", ["01.1.2", "0.1.2-alpha", "0.1", "0.1.2\nlatest"])
def test_release_versions_are_unambiguous(version):
    with pytest.raises(ValueError):
        MODULE["version_tuple"](version)


def test_manual_main_publication_must_match_version_tag(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.2"\n')
    monkeypatch.setitem(MODULE["source_version"].__globals__, "command", lambda *args: "old")
    with pytest.raises(ValueError, match="exact source"):
        MODULE["source_version"](tmp_path, ref="refs/heads/main", commit="new")


def test_mismatched_package_versions_stop_publication(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.2"\n')
    child = tmp_path / "packages/integration"
    child.mkdir(parents=True)
    (child / "pyproject.toml").write_text('[project]\nversion = "0.1.1"\n')
    with pytest.raises(ValueError, match="versions must agree"):
        MODULE["source_version"](tmp_path)


def ci_run(**changes):
    run = {
        "id": 200, "run_number": 20, "run_attempt": 1,
        "head_sha": "a" * 40, "head_branch": "main", "event": "push",
        "path": ".github/workflows/ci.yml", "status": "completed", "conclusion": "success",
        "head_repository": {"full_name": "agent-memory-lab/agent-memory"},
    }
    run.update(changes)
    return run


def check_ci(monkeypatch, pages):
    import json
    calls = []
    def command(*args):
        calls.append(args)
        return json.dumps(pages)
    monkeypatch.setitem(MODULE["require_ci"].__globals__, "command", command)
    result = MODULE["require_ci"]("agent-memory-lab/agent-memory", "a" * 40)
    return result, calls


def test_exact_main_ci_success_and_paginated_query(monkeypatch):
    run, calls = check_ci(monkeypatch, [{"workflow_runs": [ci_run(id=100, run_number=19)]},
                                       {"workflow_runs": [ci_run()]}])
    assert run == 200
    assert calls == [("gh", "api", "/repos/agent-memory-lab/agent-memory/actions/workflows/ci.yml/runs"
                      "?head_sha=" + "a" * 40 + "&branch=main&event=push&per_page=100",
                      "--paginate", "--slurp")]


@pytest.mark.parametrize("changes", [
    {"head_sha": "b" * 40}, {"head_branch": "feature"}, {"event": "pull_request"},
    {"path": ".github/workflows/other.yml"},
    {"head_repository": {"full_name": "other/repo"}},
    {"status": "queued", "conclusion": None},
    {"status": "in_progress", "conclusion": None},
    {"conclusion": "failure"}, {"conclusion": "cancelled"}, {"conclusion": "skipped"},
    {"id": None}, {"run_number": None}, {"run_attempt": None},
])
def test_ci_gate_rejects_wrong_identity_or_unsuccessful_run(monkeypatch, changes):
    with pytest.raises(ValueError):
        check_ci(monkeypatch, [{"workflow_runs": [ci_run(**changes)]}])


@pytest.mark.parametrize("status,conclusion", [("in_progress", None), ("completed", "failure")])
def test_old_green_cannot_hide_latest_pending_failed_or_rerun(monkeypatch, status, conclusion):
    with pytest.raises(ValueError, match="successful"):
        check_ci(monkeypatch, [{"workflow_runs": [
            ci_run(id=100, run_number=19),
            ci_run(status=status, conclusion=conclusion, run_attempt=2),
        ]}])


@pytest.mark.parametrize("pages", [[], {}, [{}], [{"workflow_runs": []}],
                                   [{"workflow_runs": None}], [{"workflow_runs": [None]}]])
def test_missing_or_malformed_ci_fails_closed(monkeypatch, pages):
    with pytest.raises(ValueError):
        check_ci(monkeypatch, pages)


def test_ci_api_failure_is_not_swallowed(monkeypatch):
    import subprocess
    def fail(*args):
        raise subprocess.CalledProcessError(1, args)
    monkeypatch.setitem(MODULE["require_ci"].__globals__, "command", fail)
    with pytest.raises(subprocess.CalledProcessError):
        MODULE["require_ci"]("agent-memory-lab/agent-memory", "a" * 40)


def test_preflight_blocks_before_registry_access(monkeypatch):
    import sys
    globals_ = MODULE["main"].__globals__
    monkeypatch.setenv("GITHUB_REPOSITORY", "agent-memory-lab/agent-memory")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_REF", "refs/tags/v0.1.2")
    monkeypatch.setattr(sys, "argv", ["container_release.py", "preflight", "--published"])
    monkeypatch.setitem(globals_, "source_version", lambda *args, **kwargs: "0.1.2")
    def reject(*args):
        raise ValueError("CI rejected")
    def registry(*args):
        pytest.fail("Registry must not be queried before CI passes")
    monkeypatch.setitem(globals_, "require_ci", reject)
    monkeypatch.setitem(globals_, "package_versions", registry)
    with pytest.raises(ValueError, match="CI rejected"):
        MODULE["main"]()


def test_ci_only_action_uses_exact_sha_without_tag_checkout(monkeypatch, capsys):
    import sys
    globals_ = MODULE["main"].__globals__
    monkeypatch.setenv("GITHUB_REPOSITORY", "agent-memory-lab/agent-memory")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(sys, "argv", ["container_release.py", "ci"])
    def require(repository, commit):
        assert repository == "agent-memory-lab/agent-memory" and commit == "a" * 40
        return 200
    def source(*args, **kwargs):
        pytest.fail("CI-only check must work in the Python job's shallow checkout")
    monkeypatch.setitem(globals_, "require_ci", require)
    monkeypatch.setitem(globals_, "source_version", source)
    MODULE["main"]()
    assert '"ci_run_id": 200' in capsys.readouterr().out


def test_publication_workflow_checks_ci_at_each_publication_boundary():
    workflow = (Path(__file__).resolve().parents[1] /
                ".github/workflows/publish-container.yml").read_text()
    container = workflow.split("\n  publish:\n")[1].split("\n  python-release:\n")[0]
    python = workflow.split("\n  python-release:\n")[1]
    assert "actions: read" in container and "actions: read" in python
    checks = [i for i in range(len(container))
              if container.startswith("python tools/container_release.py preflight --published", i)]
    assert len(checks) == 2
    assert checks[0] < container.index("Build a candidate once")
    assert container.index("Scan arm64 candidate") < checks[1] < container.index("imagetools create")
    checks = [i for i in range(len(python))
              if python.startswith("python tools/container_release.py ci", i)]
    assert len(checks) == 2
    assert checks[0] < python.index("tools/publish_python_release.py stage")
    assert python.index("tools/verify_release_artifacts.py") < checks[1] < python.index("gh release edit")


def test_latest_attempt_of_same_run_supersedes_old_green(monkeypatch):
    with pytest.raises(ValueError, match="successful"):
        check_ci(monkeypatch, [{"workflow_runs": [
            ci_run(run_attempt=1),
            ci_run(run_attempt=2, status="in_progress", conclusion=None),
        ]}])


def test_invalid_json_ci_response_fails_closed(monkeypatch):
    import json
    monkeypatch.setitem(MODULE["require_ci"].__globals__, "command", lambda *args: "not JSON")
    with pytest.raises(json.JSONDecodeError):
        MODULE["require_ci"]("agent-memory-lab/agent-memory", "a" * 40)
