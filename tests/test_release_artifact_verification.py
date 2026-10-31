"""Exercise the actual archive gate with small wheel/sdist pairs, never mocked readers."""

import io
import runpy
import tarfile
import zipfile
from pathlib import Path

import pytest

VERIFY = runpy.run_path(str(Path(__file__).resolve().parents[1]
                           / "tools/verify_release_artifacts.py"))["verify"]


def fixture(tmp_path, *, license_text=b"complete license", version="0.1.2",
            wheel_code=b"value = 1\n", wheel_dependencies="", include_sdist_license=True):
    source = tmp_path / "source"
    (source / "src/agent_memory").mkdir(parents=True)
    (source / "src/agent_memory/__init__.py").write_bytes(b"value = 1\n")
    (source / "LICENSE").write_bytes(b"complete license")
    (source / "pyproject.toml").write_text('''
[project]
name = "agent-memory"
version = "0.1.2"
[tool.hatch.build.targets.wheel]
packages = ["src/agent_memory"]
''')
    output = tmp_path / "archives"
    output.mkdir()
    metadata = (f"Name: agent-memory\nVersion: {version}\nRequires-Python: >=3.13\n").encode()
    with zipfile.ZipFile(output / "agent_memory-0.1.2-py3-none-any.whl", "w") as archive:
        archive.writestr("agent_memory/__init__.py", wheel_code)
        archive.writestr("agent_memory-0.1.2.dist-info/METADATA",
                         metadata + wheel_dependencies.encode())
        if license_text is not None:
            archive.writestr("agent_memory-0.1.2.dist-info/licenses/LICENSE", license_text)
    with tarfile.open(output / "agent_memory-0.1.2.tar.gz", "w:gz") as archive:
        contents = {"PKG-INFO": metadata, "src/agent_memory/__init__.py": b"value = 1\n"}
        if include_sdist_license:
            contents["LICENSE"] = b"complete license"
        for name, data in contents.items():
            info = tarfile.TarInfo("agent_memory-0.1.2/" + name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return source, output


def test_public_archive_roundtrip_and_manifest_detect_changed_checksum(tmp_path, monkeypatch):
    source, output = fixture(tmp_path)
    monkeypatch.setattr(VERIFY.__globals__["subprocess"], "check_output",
                        lambda args, **kwargs: "" if args[1] == "status" else "abc\n")
    result = VERIFY(source, output, write_manifest=True)
    assert result == {"status": "passed", "archive_count": 2, "source_files": 1,
                      "licenses_verified": 2, "manifest_verified": True}
    assert VERIFY(source, output, require_manifest=True) == result
    sums = output / "SHA256SUMS"
    sums.write_text(sums.read_text().replace("  release-manifest.json", "  wrong.json"))
    with pytest.raises(ValueError, match="SHA256SUMS"):
        VERIFY(source, output, require_manifest=True)


@pytest.mark.parametrize("options, message", [
    ({"license_text": None}, "LICENSE"),
    ({"license_text": b"Apache-2.0"}, "LICENSE"),
    ({"include_sdist_license": False}, "LICENSE"),
    ({"version": "0.1.0"}, "version"),
    ({"wheel_code": b"value = 2\n"}, "source differs"),
    ({"wheel_dependencies": "Requires-Dist: unexpected-dependency\n"}, "dependency metadata"),
])
def test_corrupt_or_incomplete_archives_are_rejected(tmp_path, options, message):
    source, output = fixture(tmp_path, **options)
    with pytest.raises(ValueError, match=message):
        VERIFY(source, output)


def test_missing_archive_is_rejected(tmp_path):
    source, output = fixture(tmp_path)
    next(output.glob("*.whl")).unlink()
    with pytest.raises(ValueError, match="exactly one"):
        VERIFY(source, output)


def test_unexpected_wheel_payload_cannot_hide_outside_the_expected_module(tmp_path):
    source, output = fixture(tmp_path)
    with zipfile.ZipFile(next(output.glob("*.whl")), "a") as archive:
        archive.writestr("unexpected_module.py", "value = 2\n")
    with pytest.raises(ValueError, match="source differs"):
        VERIFY(source, output)


def test_archive_filename_cannot_claim_another_version(tmp_path):
    source, output = fixture(tmp_path)
    wheel = next(output.glob("*.whl"))
    wheel.rename(wheel.with_name(wheel.name.replace("0.1.2", "0.1.0")))
    with pytest.raises(ValueError, match="filename differs"):
        VERIFY(source, output)


def test_dirty_source_manifest_is_not_publishable(tmp_path, monkeypatch):
    source, output = fixture(tmp_path)
    monkeypatch.setattr(VERIFY.__globals__["subprocess"], "check_output", lambda *a, **k: "dirty\n")
    VERIFY(source, output, write_manifest=True)
    with pytest.raises(ValueError, match="clean source tree"):
        VERIFY(source, output, require_manifest=True)


def test_manifest_cannot_substitute_another_artifact_digest(tmp_path, monkeypatch):
    source, output = fixture(tmp_path)
    monkeypatch.setattr(VERIFY.__globals__["subprocess"], "check_output",
                        lambda args, **kwargs: "" if args[1] == "status" else "abc\n")
    VERIFY(source, output, write_manifest=True)
    manifest = output / "release-manifest.json"
    manifest.write_text(manifest.read_text().replace('"bytes":', '"wrong-size-field":'))
    with pytest.raises(ValueError, match="manifest does not match"):
        VERIFY(source, output, require_manifest=True)


def test_manifest_must_bind_the_actual_source_commit(tmp_path, monkeypatch):
    source, output = fixture(tmp_path)
    monkeypatch.setattr(VERIFY.__globals__["subprocess"], "check_output",
                        lambda args, **kwargs: "" if args[1] == "status" else "abc\n")
    VERIFY(source, output, write_manifest=True)
    manifest = output / "release-manifest.json"
    manifest.write_text(manifest.read_text().replace('"source_commit": "abc"',
                                                  '"source_commit": "other"'))
    with pytest.raises(ValueError, match="clean source tree"):
        VERIFY(source, output, require_manifest=True)
