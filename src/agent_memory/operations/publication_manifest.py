"""Versioned publication proofs shared by workers, readiness and index recovery."""

import json
import re
from dataclasses import dataclass
from hashlib import sha256

from ..domain import canonical_json


@dataclass(frozen=True, slots=True)
class PublicationPolicy:
    """Host-selected batch size is a target; one semantic slot is never split."""

    batch_size: int = 16

    def __post_init__(self):
        if type(self.batch_size) is not int or not 1 <= self.batch_size <= 64:
            raise ValueError("publication batch size must be between 1 and 64")

    def payload(self):
        return {
            "schema": "durable-publication/1",
            "batch_size": self.batch_size,
            "grouping": "whole-slot",
            "replacement": "atomic",
        }


def commit_token(scope, row, kind):
    coordinates = [
        scope.partition_key(),
        row["epoch"],
        row["request_id"],
        row["configuration_sha256"],
    ]
    return {
        "kind": kind,
        "id": kind
        + ":"
        + sha256(json.dumps(coordinates, separators=(",", ":")).encode()).hexdigest(),
        "scope_key": coordinates[0],
        "epoch": row["epoch"],
        "generation": row["request_id"],
        "configuration_sha256": row["configuration_sha256"],
    }


def begin(scope, row):
    # Preserve the legacy internal field; reprocessing readiness exposes a processing token.
    row["capture_commit_token"] = commit_token(scope, row, "capture")
    if row.get("operation") == "reprocess":
        row["processing_commit_token"] = commit_token(scope, row, "processing")
    row["publication_manifest"] = {
        "schema": "publication-manifest/1",
        "generation": row["request_id"],
        "version": 0,
        "closed": False,
        "publication_commit_tokens": [],
        "dispositions": [],
        "no_outputs": None,
        "no_indexable_outputs": None,
    }


def close(scope, row, receipt, interpretation=None):
    if "publication_manifest" not in row:
        # The live worker can adopt a legacy request because it actually commits this publication.
        begin(scope, row)
    dispositions = [{"candidate_id": d.candidate_id, "action": d.action} for d in receipt.decisions]
    if interpretation:
        # Reconciliation can withdraw old candidates even when it produces no new candidates.
        seen = {d["candidate_id"] for d in dispositions}
        dispositions.extend(
            {
                "candidate_id": item["candidate_id"],
                "action": {
                    "withdraw_source_support": "WITHDRAWN",
                    "qualification_pending": "PENDING_VERIFICATION",
                    "retain": "ACCEPT",
                    "carry_forward": "CARRY_FORWARD",
                }[item["action"]],
            }
            for item in interpretation["dispositions"]
            if item["candidate_id"] not in seen
        )
    row["publication_manifest"] = {
        "schema": "publication-manifest/1",
        "generation": row["request_id"],
        "version": 1,
        "closed": True,
        "publication_commit_tokens": [commit_token(scope, row, "publication")]
        if dispositions
        else [],
        "dispositions": dispositions,
        "no_outputs": not dispositions,
        "no_indexable_outputs": not receipt.claim_ids,
    }


def _valid_legacy_manifest(scope, row):
    manifest = row.get("publication_manifest")
    if not isinstance(manifest, dict):
        return False
    closed = manifest.get("closed")
    dispositions = manifest.get("dispositions")
    if (
        manifest.get("schema") != "publication-manifest/1"
        or manifest.get("generation") != row["request_id"]
        or type(closed) is not bool
        or type(manifest.get("version")) is not int
        or manifest["version"] != int(closed)
        or not isinstance(dispositions, list)
        or any(
            not isinstance(d, dict)
            or not isinstance(d.get("candidate_id"), str)
            or not isinstance(d.get("action"), str)
            for d in dispositions
        )
        or row.get("capture_commit_token") != commit_token(scope, row, "capture")
    ):
        return False
    if not closed:
        return (
            not dispositions
            and manifest.get("publication_commit_tokens") == []
            and manifest.get("no_outputs") is None
            and manifest.get("no_indexable_outputs") is None
        )
    return (
        row["status"] == "completed"
        and type(manifest.get("no_outputs")) is bool
        and manifest["no_outputs"] == (not dispositions)
        and type(manifest.get("no_indexable_outputs")) is bool
        and manifest["no_indexable_outputs"] == (not row.get("result", {}).get("claim_ids"))
        and manifest.get("publication_commit_tokens")
        == ([commit_token(scope, row, "publication")] if dispositions else [])
    )


def digest(value):
    return sha256(canonical_json(value).encode()).hexdigest()


def open_batches(scope, row, policy, plan, prepared_sha256, *, mode="incremental"):
    if "capture_commit_token" not in row:
        begin(scope, row)
    contract = {
        "policy": policy.payload(),
        "plan": plan,
        "prepared_sha256": prepared_sha256,
        "mode": mode,
    }
    row["publication_manifest"] = {
        "schema": "publication-manifest/2",
        "generation": row["request_id"],
        **contract,
        "plan_sha256": digest(contract),
        "version": 0,
        "closed": False,
        "publications": [],
        "publication_commit_tokens": [],
        "dispositions": [],
        "no_outputs": None,
        "no_indexable_outputs": None,
    }


def batch_token(scope, row, publication):
    token = commit_token(scope, row, "publication")
    return {
        **token,
        "id": "publication:"
        + digest([token, row["publication_manifest"]["plan_sha256"], publication]),
        "batch_index": publication["batch_index"],
    }


def append_batch(scope, row, receipt, dispositions, committed_at):
    manifest = row["publication_manifest"]
    publication = {
        "batch_index": len(manifest["publications"]),
        "receipt": receipt,
        "dispositions": dispositions,
        "committed_at": committed_at,
    }
    # Empty executions are recorded for plan coverage without fabricating an output token.
    token = batch_token(scope, row, publication) if dispositions else None
    manifest["publications"].append({**publication, "token": token})
    if token is not None:
        manifest["publication_commit_tokens"].append(token)
    manifest["dispositions"].extend(dispositions)
    manifest["version"] += 1


def close_batches(row):
    manifest = row["publication_manifest"]
    if len(manifest["publications"]) != len(manifest["plan"]):
        raise ValueError("publication plan is incomplete")
    manifest.update(
        closed=True,
        version=manifest["version"] + 1,
        no_outputs=not manifest["dispositions"],
        no_indexable_outputs=not row["result"]["claim_ids"],
    )


def token_dispositions(row, token):
    manifest = row["publication_manifest"]
    if manifest["schema"] == "publication-manifest/1":
        return manifest["dispositions"]
    for publication in manifest["publications"]:
        if publication["token"] == token:
            return publication["dispositions"]
    raise ValueError("publication token is not a manifest member")


def token_time(row, token):
    manifest = row["publication_manifest"]
    if manifest["schema"] == "publication-manifest/2":
        return next(p["committed_at"] for p in manifest["publications"] if p["token"] == token)
    return row["completed_at"]


def _valid_batches(scope, row, manifest):
    from datetime import datetime

    policy = manifest["policy"]
    if PublicationPolicy(policy["batch_size"]).payload() != policy:
        return False
    mode, plan = manifest["mode"], manifest["plan"]
    if mode not in {"incremental", "atomic_activation"} or not 1 <= len(plan) <= 128:
        return False
    item_type = int if mode == "incremental" else str
    if any(
        not isinstance(group, list) or any(type(i) is not item_type for i in group)
        for group in plan
    ):
        return False
    items = [i for group in plan for i in group]
    if len(items) > (64 if mode == "incremental" else 128) or len(set(items)) != len(items):
        return False
    if mode == "incremental" and sorted(items) != list(range(len(items))):
        return False
    if not items and plan != [[]]:
        return False
    contract = {key: manifest[key] for key in ("policy", "plan", "prepared_sha256", "mode")}
    if (
        manifest["plan_sha256"] != digest(contract)
        or not re.fullmatch(r"[0-9a-f]{64}", manifest["prepared_sha256"])
        or manifest["generation"] != row["request_id"]
        or row["capture_commit_token"] != commit_token(scope, row, "capture")
    ):
        return False
    closed, publications = manifest["closed"], manifest["publications"]
    if (
        type(closed) is not bool
        or not isinstance(publications, list)
        or len(publications) > len(plan)
        or type(manifest["version"]) is not int
        or manifest["version"] != len(publications) + int(closed)
    ):
        return False
    tokens, dispositions = [], []
    for index, publication in enumerate(publications):
        if (
            set(publication) != {"batch_index", "receipt", "dispositions", "committed_at", "token"}
            or type(publication["batch_index"]) is not int
            or publication["batch_index"] != index
        ):
            return False
        if datetime.fromisoformat(publication["committed_at"]).utcoffset() is None:
            return False
        receipt, batch = publication["receipt"], publication["dispositions"]
        if (
            set(receipt)
            != {"event_id", "candidate_ids", "claim_ids", "decisions", "pending_ids", "duplicate"}
            or type(receipt["duplicate"]) is not bool
            or any(
                not isinstance(receipt[k], list) or len(receipt[k]) > 64
                for k in ("candidate_ids", "claim_ids", "decisions", "pending_ids")
            )
            or any(
                not isinstance(i, str) or not i
                for k in ("candidate_ids", "claim_ids", "pending_ids")
                for i in receipt[k]
            )
            or any(
                set(d) != {"candidate_id", "action", "reasons"}
                or not isinstance(d["action"], str)
                or not isinstance(d["reasons"], list)
                or any(not isinstance(r, str) for r in d["reasons"])
                for d in receipt["decisions"]
            )
            or receipt["candidate_ids"] != [d["candidate_id"] for d in receipt["decisions"]]
            or not set(receipt["pending_ids"]) <= set(receipt["candidate_ids"])
        ):
            return False
        if (
            not isinstance(batch, list)
            or len(batch) > 128
            or any(
                set(d) != {"candidate_id", "action"}
                or not isinstance(d["candidate_id"], str)
                or not isinstance(d["action"], str)
                for d in batch
            )
            or receipt["event_id"] != row["event_id"]
        ):
            return False
        body = {k: v for k, v in publication.items() if k != "token"}
        token = batch_token(scope, row, body) if batch else None
        if publication["token"] != token:
            return False
        if token is not None:
            tokens.append(token)
        dispositions.extend(batch)
        if mode == "atomic_activation" and [d["candidate_id"] for d in batch] != plan[index]:
            return False
        if mode == "incremental" and batch != [
            {"candidate_id": d["candidate_id"], "action": d["action"]} for d in receipt["decisions"]
        ]:
            return False
    if len({d["candidate_id"] for d in dispositions}) != len(dispositions):
        return False
    resumptions = manifest.get("resumptions", [])
    if not isinstance(resumptions, list) or len(resumptions) > 32:
        return False
    for item in resumptions:
        if (
            set(item) != {"actor", "reason", "attempts", "manifest_version", "recorded_at"}
            or any(
                not isinstance(item[k], str) or not 1 <= len(item[k]) <= 256 or not item[k].strip()
                for k in ("actor", "reason")
            )
            or type(item["attempts"]) is not int
            or not 1 <= item["attempts"] <= 100
            or type(item["manifest_version"]) is not int
            or not 1 <= item["manifest_version"] <= len(publications)
            or datetime.fromisoformat(item["recorded_at"]).utcoffset() is None
        ):
            return False
    if manifest["publication_commit_tokens"] != tokens or manifest["dispositions"] != dispositions:
        return False
    result = row.get("result")
    if publications:
        for name in ("candidate_ids", "claim_ids", "decisions", "pending_ids"):
            expected = [item for p in publications for item in p["receipt"][name]]
            if sorted(map(canonical_json, result[name])) != sorted(map(canonical_json, expected)):
                return False
    if closed:
        return (
            len(publications) == len(plan)
            and row["status"] == "completed"
            and type(manifest["no_outputs"]) is bool
            and manifest["no_outputs"] == (not dispositions)
            and type(manifest["no_indexable_outputs"]) is bool
            and manifest["no_indexable_outputs"] == (not row["result"]["claim_ids"])
        )
    return (
        row["status"] != "completed"
        and mode == "incremental"
        and manifest["no_outputs"] is None
        and manifest["no_indexable_outputs"] is None
    )


def valid_manifest(scope, row):
    try:
        manifest = row.get("publication_manifest")
        if not isinstance(manifest, dict):
            return False
        if manifest.get("schema") == "publication-manifest/2":
            if len(canonical_json(manifest).encode()) > 320_000:
                return False
            return _valid_batches(scope, row, manifest)
        return _valid_legacy_manifest(scope, row)
    except (KeyError, TypeError, ValueError, StopIteration, AttributeError, OverflowError):
        return False
