import json

from agent_memory_postgres import preflight


def test_build_only_preflight_does_not_require_database_scope():
    args = preflight._parse_args(
        [
            "--skip-health",
            "--skip-sensitive-scan",
            "--skip-build",
            "--skip-release-scan",
        ]
    )

    assert preflight._scope_from_args(args) is None


def test_custom_build_path_replaces_default_path():
    args = preflight._parse_args(
        [
            "--skip-health",
            "--skip-sensitive-scan",
            "--build-path",
            "custom-package",
        ]
    )

    assert args.build_paths == ["custom-package"]


def test_json_mode_emits_one_machine_readable_document(capsys):
    status = preflight.main(
        [
            "--skip-health",
            "--skip-sensitive-scan",
            "--skip-build",
            "--skip-release-scan",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert status == 0
    assert payload["health"] is None
    assert payload["sensitive_scan"] is None
    assert payload["release_scan"] is None
    assert payload["build"] == []
