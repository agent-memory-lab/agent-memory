"""Bounded recall diagnostics and opt-in public-fixture SQLite replay artifacts.

This module is for the isolated LongMemEval runner, never a production export
API. Snapshots contain the benchmark source text, survive trial cleanup, and
must be deleted separately when the diagnostic experiment is no longer needed.
Replay copies immutable artifacts into a fresh sandbox and still uses the
ordinary UnifiedMemory deletion, admission, and scope gates.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import sqlite3
import tempfile
from collections.abc import Mapping
from contextlib import closing
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

_LINEAGE_KEYS = (
    "candidate_identity",
    "source_owner_id",
    "source_revision",
    "source_chars",
    "lexical_chunk_id",
    "lexical_analyzer",
    "context_anchor_chunk_id",
    "context_reason",
    "context_window",
    "retrieval_branch",
    "lexical_ranker",
    "lexical_score",
    "lexical_rank",
    "query_anchor_policy",
    "query_anchor_count",
    "query_anchor_mode",
    "matched_source_span",
    "passage_boundary",
    "context_only",
    "context_anchor_candidate_id",
    "context_anchor_score",
    "relevance_origin",
    "semantic_score",
    "semantic_dimensions",
    "semantic_reranker",
    "fusion",
    "version",
    "excerpt",
    "source_event_ids",
    "source_span",
)
_PROOF_KEYS = (
    "memory_id",
    "candidate_id",
    "source_event_ids",
    "source_span",
    "source_revision",
    "source_chars",
)
_METADATA_KEYS = (
    "candidate_count",
    "selected_count",
    "state_count",
    "strategy",
    "coverage",
    "world_negative",
    "protocol_version",
    "valid_at",
    "known_at",
    "temporal_kind",
)
_FILES = ("agent.sqlite3", "agent.sqlite3.deletions.db")


def _safe_json(value, depth=0):
    """Never repr/stringify opaque provider objects or permit unbounded metadata."""
    if depth > 4:
        return None
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value[:512]
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else None
    if isinstance(value, (tuple, list)):
        return [_safe_json(entry, depth + 1) for entry in value[:128]]
    if isinstance(value, Mapping):
        return {
            key: _safe_json(value[key], depth + 1)
            for key in ("start", "end", "unit")
            if key in value
        }
    return None


def retrieval_export(bundle, ranked):
    """Export already-governed returned evidence and body-free stage diagnostics."""
    metadata = bundle.retrieval_metadata
    proofs = metadata.get("citation_spans", ())
    proofs = proofs if isinstance(proofs, (tuple, list)) else ()
    exported = []
    for item in ranked:
        lineage = {
            key: _safe_json(item.metadata[key]) for key in _LINEAGE_KEYS if key in item.metadata
        }
        candidate_id = item.metadata.get("candidate_identity", item.id)
        citation_spans = [
            {key: _safe_json(proof[key]) for key in _PROOF_KEYS if key in proof}
            for proof in proofs[:128]
            if isinstance(proof, Mapping)
            and proof.get("memory_id") == item.id
            and proof.get("candidate_id", item.id) == candidate_id
        ]
        exported.append(
            {
                "id": item.id,
                "text": item.text,
                "kind": item.kind.value,
                "score": _safe_json(item.score),
                "lineage": lineage,
                "citation_spans": citation_spans,
            }
        )
    diagnostic = {key: _safe_json(metadata[key]) for key in _METADATA_KEYS if key in metadata}
    diagnostic.update(
        {
            key: value
            for key, value in metadata.items()
            if isinstance(key, str)
            and key.startswith("packing_")
            and type(value) is int
            and value >= 0
        }
    )
    diagnostic["returned_count"] = len(exported)
    diagnostic["returned_kinds"] = {
        kind: sum(row["kind"] == kind for row in exported)
        for kind in sorted({row["kind"] for row in exported})
    }
    expansion = metadata.get("source_context_expansion")
    if isinstance(expansion, Mapping):
        diagnostic["source_context_expansion"] = {
            key: _safe_json(expansion[key])
            for key in ("policy", "status", "anchor_count", "candidate_count", "semantic_scoring")
            if key in expansion
        }
    stages = metadata.get("retrieval_stages", {})
    if isinstance(stages, Mapping):
        diagnostic["retrieval_stages"] = {}
        # Stage names are a fixed protocol; arbitrary provider keys are not exported.
        for stage in (
            "candidate_recall",
            "candidates",
            "ranked",
            "selected",
            "retrieved",
            "reranked",
            "source_context",
            "packing_input",
            "packed",
        ):
            value = stages.get(stage)
            if not isinstance(value, Mapping):
                continue
            summary = {}
            if type(value.get("count")) is int and value["count"] >= 0:
                summary["count"] = value["count"]
            rows = value.get("items", ())
            if isinstance(rows, (tuple, list)):
                summary["items"] = [
                    {
                        key: _safe_json(row[key])
                        for key in ("id", "kind", "score", "candidate_id")
                        if key in row
                    }
                    for row in rows[:512]
                    if isinstance(row, Mapping)
                ]
            diagnostic["retrieval_stages"][stage] = summary
    return exported, diagnostic


def evidence_diagnostic(items, evidence, *, limit=8, byte_budget=8192):
    """Locate loss at the shared answer boundary without recording extra text."""
    consumed = 0
    rows = []
    for index, item in enumerate(items[:limit]):
        raw = (("\n- " if rows else "- ") + item["text"]).encode()
        emitted = raw[: max(0, byte_budget - consumed)].decode("utf-8", errors="ignore")
        written = len(emitted.encode())
        rows.append(
            {
                "rank": index + 1,
                "id": item["id"],
                "kind": item.get("kind"),
                "candidate_id": item.get("lineage", {}).get("candidate_identity", item["id"]),
                "available_utf8_bytes": len(raw),
                "emitted_utf8_bytes": written,
                "complete": written == len(raw),
            }
        )
        consumed += written
        if written < len(raw) or consumed == byte_budget:
            break
    if consumed != len(evidence.encode()):
        raise ValueError("evidence accounting disagrees with the shared evidence budget")
    return {
        "byte_budget": byte_budget,
        "emitted_utf8_bytes": consumed,
        "items": rows,
        "omitted_items": max(0, len(items) - len(rows)),
        "truncated": any(not row["complete"] for row in rows) or len(rows) < len(items),
    }


def ingestion_digest(item):
    """Exclude evaluation labels and query; replay may evaluate another query."""
    value = {key: item[key] for key in ("question_id", "haystack_sessions", "haystack_dates")}
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def snapshot_key(question_id):
    return sha256(question_id.encode()).hexdigest()[:24]


def _backup(source, destination):
    # SQLite backup includes committed WAL pages; a filesystem copy does not.
    incoming_uri = Path(source).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(incoming_uri, uri=True)) as incoming:
        with closing(sqlite3.connect(destination)) as outgoing:
            incoming.backup(outgoing)
    os.chmod(destination, 0o600)


def _inventory(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        # Identifiers are fixed in this implementation, never interpolated from the DB.
        result = {
            name: db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            for name in (
                "events",
                "claims",
                "claim_versions",
                "claim_observations",
                "lexical_chunks",
            )
            if name in tables
        }
        if "claims" in tables:
            columns = {row[1] for row in db.execute("PRAGMA table_info(claims)")}
            if {"status", "version"} <= columns:
                result["claim_status_versions"] = [
                    {"status": row[0], "version": row[1], "count": row[2]}
                    for row in db.execute(
                        "SELECT status,version,COUNT(*) FROM claims GROUP BY status,version "
                        "ORDER BY status,version"
                    )
                ]
        return result


def retain_snapshot(directory, database, scope, item, *, dataset_sha256, provenance, ingestion):
    """Retain a complete *public benchmark fixture*, only after settled ingestion."""
    if scope.tenant_id != "longmemeval-full" or not scope.user_id or not scope.session_id:
        raise ValueError("snapshots require an isolated LongMemEval benchmark scope")
    directory = Path(directory)
    directory.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.exists():
        raise FileExistsError("snapshot artifacts are immutable; use a new attempt directory")
    with tempfile.TemporaryDirectory(dir=directory.parent, prefix=".snapshot-") as temporary:
        pending = Path(temporary) / "complete"
        pending.mkdir(mode=0o700)
        for filename, source in zip(
            _FILES, (database, str(database) + ".deletions.db"), strict=True
        ):
            _backup(source, pending / filename)
        manifest = {
            "schema_version": 1,
            "artifact_kind": "public-longmemeval-fixture",
            "question_id": item["question_id"],
            "dataset_sha256": dataset_sha256,
            "ingestion_sha256": ingestion_digest(item),
            "scope": asdict(scope),
            "files": {name: sha256((pending / name).read_bytes()).hexdigest() for name in _FILES},
            "provenance": provenance,
            "ingestion": ingestion,
            "inventory": _inventory(pending / _FILES[0]),
            "retention": "explicit diagnostic copy; remove this directory to delete the artifact",
            "cost_semantics": "ingestion cost belongs to source run; replay measures recall/answer",
        }
        (pending / "snapshot.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
        os.chmod(pending / "snapshot.json", 0o600)
        pending.rename(directory)
    return manifest


def load_snapshot(directory, item, *, dataset_sha256, expected_manifest_sha256=None):
    """Validate all replay bindings before any model or writable DB operation."""
    directory = Path(directory)
    content = (directory / "snapshot.json").read_bytes()
    if (
        expected_manifest_sha256 is not None
        and sha256(content).hexdigest() != expected_manifest_sha256
    ):
        raise ValueError("snapshot manifest changed after the replay plan was frozen")
    manifest = json.loads(content)
    if (
        manifest.get("schema_version") != 1
        or manifest.get("artifact_kind") != "public-longmemeval-fixture"
        or manifest.get("dataset_sha256") != dataset_sha256
        or manifest.get("question_id") != item["question_id"]
        or manifest.get("ingestion_sha256") != ingestion_digest(item)
    ):
        raise ValueError("snapshot does not match this frozen benchmark input")
    scope = manifest.get("scope", {})
    if (
        scope.get("tenant_id") != "longmemeval-full"
        or not scope.get("user_id")
        or not scope.get("session_id")
    ):
        raise ValueError("snapshot scope is not an isolated benchmark scope")
    if set(manifest.get("files", {})) != set(_FILES):
        raise ValueError("snapshot requires the database and deletion journal")
    for name in _FILES:
        if sha256((directory / name).read_bytes()).hexdigest() != manifest["files"][name]:
            raise ValueError("snapshot file digest mismatch")
    return manifest


def restore_snapshot(
    directory, destination, item, *, dataset_sha256, expected_manifest_sha256=None
):
    """Restore into a new sandbox; never open the retained artifact for writes."""
    manifest = load_snapshot(
        directory,
        item,
        dataset_sha256=dataset_sha256,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    destination = Path(destination)
    targets = (destination, Path(str(destination) + ".deletions.db"))
    if any(path.exists() for path in targets):
        raise FileExistsError("snapshot replay requires a fresh sandbox")
    for filename, target in zip(_FILES, targets, strict=True):
        shutil.copyfile(Path(directory) / filename, target)
        os.chmod(target, 0o600)
    return manifest
