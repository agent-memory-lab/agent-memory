"""Fail-closed source and registry checks for container tag promotion.

Registry access uses the workflow's gh authentication; no tokens are printed.
Only the release job may promote, under a repository-wide concurrency lock.
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import tomllib
from pathlib import Path


def version_tuple(value):
    if not re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", value):
        raise ValueError(f"Invalid release version: {value!r}")
    return tuple(map(int, value.split(".")))


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def source_version(root, *, ref=None, commit=None):
    projects = [root / "pyproject.toml", *sorted(root.glob("packages/*/pyproject.toml"))]
    versions = {tomllib.loads(p.read_text())["project"]["version"] for p in projects}
    if len(versions) != 1:
        raise ValueError("All distribution versions must agree")
    version = versions.pop()
    version_tuple(version)
    if commit is not None:
        if ref not in ("refs/heads/main", f"refs/tags/v{version}"):
            raise ValueError("Publication requires main or the exact version tag")
        tagged = command("git", "rev-parse", f"refs/tags/v{version}^{{commit}}")
        if tagged != commit:
            raise ValueError("The release tag must point to this exact source commit")
        subprocess.run(["git", "merge-base", "--is-ancestor", commit, "origin/main"], check=True)
    return version


def promotion_plan(version, commit, packages):
    version_tuple(version)
    tags = {tag for package in packages for tag in package["metadata"]["container"]["tags"]}
    if version in tags or f"sha-{commit}" in tags:
        raise ValueError("Published version/source tags are immutable; refuse to overwrite")
    released = [version_tuple(tag) for tag in tags if re.fullmatch(r"\d+\.\d+\.\d+", tag)]
    return not released or version_tuple(version) > max(released)


def package_versions(repository):
    owner, name = repository.split("/")
    pages = json.loads(command(
        "gh", "api", f"/users/{owner}/packages/container/{name}/versions?per_page=100",
        "--paginate", "--slurp",
    ))
    return [package for page in pages for package in page]


def require_ci(repository, commit):
    """Require the latest exact-source main CI, never an older green run."""
    pages = json.loads(command(
        "gh", "api", f"/repos/{repository}/actions/workflows/ci.yml/runs"
        f"?head_sha={commit}&branch=main&event=push&per_page=100",
        "--paginate", "--slurp",
    ))
    if not isinstance(pages, list) or not pages:
        raise ValueError("Missing CI workflow response")
    runs = []
    for page in pages:
        if not isinstance(page, dict) or not isinstance(page.get("workflow_runs"), list):
            raise ValueError("Invalid CI workflow response")
        for run in page["workflow_runs"]:
            if (not isinstance(run, dict)
                    or run.get("head_sha") != commit
                    or run.get("head_branch") != "main"
                    or run.get("event") != "push"
                    or run.get("path") != ".github/workflows/ci.yml"
                    or not isinstance(run.get("head_repository"), dict)
                    or run["head_repository"].get("full_name") != repository
                    or any(type(run.get(key)) is not int or run[key] < 1
                           for key in ("id", "run_number", "run_attempt"))):
                raise ValueError("CI run does not identify the exact source/main workflow")
            runs.append(run)
    if not runs:
        raise ValueError("No main CI run for this exact source; wait for CI and retry")
    latest = max(runs, key=lambda run: (run["run_number"], run["run_attempt"], run["id"]))
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise ValueError("Latest exact-source main CI is not successful; wait or fix CI and retry")
    return latest["id"]


def output(**values):
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
    print(json.dumps(values))


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("action", choices=("metadata", "preflight", "verify", "ci"))
    parser.add_argument("--published", action="store_true")
    parser.add_argument("--digest")
    parser.add_argument("--verify-latest", action="store_true")
    args = parser.parse_args()
    repository = os.environ["GITHUB_REPOSITORY"]
    commit = os.environ["GITHUB_SHA"]
    if args.action == "ci":
        output(ci_run_id=require_ci(repository, commit))
        return
    version = source_version(Path.cwd(), ref=os.environ["GITHUB_REF"],
                             commit=commit if args.published else None)
    image = f"ghcr.io/{repository.lower()}"
    if args.action == "metadata":
        output(version=version, image=image)
    elif args.action == "preflight":
        require_ci(repository, commit)
        advance = promotion_plan(version, commit, package_versions(repository))
        output(advance_latest=str(advance).lower())
    else:
        if not args.digest or not re.fullmatch(r"sha256:[0-9a-f]{64}", args.digest):
            raise ValueError("A verified candidate digest is required")
        tags = [version, f"sha-{commit}"]
        if args.verify_latest:
            tags.append("latest")
        for tag in tags:
            raw = subprocess.check_output(["docker", "buildx", "imagetools", "inspect",
                                           "--raw", f"{image}:{tag}"])
            # buildx appends a display newline; JSON manifest digests omit it.
            candidates = (raw, raw.rstrip(b"\n"))
            if not any("sha256:" + hashlib.sha256(x).hexdigest() == args.digest
                       for x in candidates):
                raise ValueError(f"Published {tag} differs from the verified candidate")
        owner, name = repository.split("/")
        package = json.loads(command("gh", "api", f"/users/{owner}/packages/container/{name}"))
        report = {"image": image, "digest": args.digest, "version": version,
                  "source_commit": commit, "html_url": package["html_url"],
                  "visibility": package["visibility"], "status": "verified"}
        if report["visibility"] != "public":
            raise ValueError("Release package must be publicly readable")
        Path(os.environ["RUNNER_TEMP"], "published-package.json").write_text(
            json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))


if __name__ == "__main__":
    main()
