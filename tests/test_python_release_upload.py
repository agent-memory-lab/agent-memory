import hashlib
import runpy
from pathlib import Path

import pytest


@pytest.fixture
def upload(monkeypatch):
    tools = Path(__file__).resolve().parents[1] / "tools"
    monkeypatch.syspath_prepend(str(tools))
    # runpy does not run the CLI, and the helper performs no network operations.
    return runpy.run_path(str(tools / "publish_python_release.py"))["missing_assets"]


def test_draft_retry_preserves_identical_assets(upload, tmp_path):
    first, second = tmp_path / "first.whl", tmp_path / "second.whl"
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    release = {"draft": True, "assets": [
        {"name": first.name, "digest": "sha256:" + hashlib.sha256(b"one").hexdigest()},
    ]}
    assert upload([first, second], release) == [second]


@pytest.mark.parametrize("draft,assets", [
    (False, []),
    (True, [{"name": "first.whl", "digest": "wrong"}]),
    (True, [{"name": "unexpected.whl", "digest": "wrong"}]),
])
def test_published_or_conflicting_drafts_cannot_be_clobbered(upload, tmp_path, draft, assets):
    path = tmp_path / "first.whl"
    path.write_bytes(b"one")
    with pytest.raises(ValueError):
        upload([path], {"draft": draft, "assets": assets})
