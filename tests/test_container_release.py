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
