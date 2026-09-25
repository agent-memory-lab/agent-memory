"""Durable generation inventory and exclusive worker ownership for local jobs."""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
from uuid import uuid4


@dataclass
class OntologyGenerationFiles:
    owner: object
    generation_id: str

    @property
    def name(self):
        return str(self.owner.root / self.generation_id)

    def cleanup(self):
        self.owner.discard(self)


class OntologyWorkspace:
    """Small atomic manifest, OS-held exclusive lock, and bounded retention.

    Persisted checkpoints support replay, while an index UUID prevents applying
    a completed checkpoint to a replaced database. Lock ownership is released
    by the OS on process death. Local filesystems with advisory locks are needed.
    """

    def __init__(self, root, job_identity, *, retain=2):
        if type(retain) is not int or not 1 <= retain <= 8:
            raise ValueError("retain must be between 1 and 8")
        self.root = Path(root) / sha256(job_identity.encode()).hexdigest()
        self.retain = retain
        self.entries = []
        self._lock = None

    def open(self):
        self.root.mkdir(parents=True, exist_ok=True)
        handle = (self.root / "worker.lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                if handle.seek(0, os.SEEK_END) == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            manifest = self.root / "generations.json"
            if manifest.exists():
                if manifest.stat().st_size > 65536:
                    raise ValueError("generation manifest exceeds size limit")
                self.entries = json.loads(manifest.read_text())
                if not isinstance(self.entries, list) or len(self.entries) > 16:
                    raise ValueError("invalid generation manifest")
                for entry in self.entries:
                    if not isinstance(entry, dict) or not re.fullmatch(r"[a-f0-9]{32}", entry.get("id", "")):
                        raise ValueError("invalid generation identity")
            self._lock = handle
            for entry in tuple(self.entries):
                if entry["state"] == "building" and not entry.get("index_id"):
                    self.discard(OntologyGenerationFiles(self, entry["id"]))
        except BaseException:
            handle.close()
            raise

    def close(self):
        if self._lock:
            self._lock.close()
            self._lock = None

    def _save(self):
        temporary = self.root / "generations.tmp"
        with temporary.open("w") as output:
            json.dump(self.entries, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, self.root / "generations.json")
        if os.name == "nt":
            return
        directory = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def recover(self, key):
        for entry in reversed(self.entries):
            if entry["key"] == key and entry.get("index_id"):
                path = self.root / entry["id"]
                if path.is_symlink() or any((path / name).is_symlink() for name in ("index.db", "snapshot.db", "checkpoint.db")):
                    raise ValueError("symlinked generation cannot be recovered")
                return OntologyGenerationFiles(self, entry["id"])
        return None

    def create(self, key):
        for entry in tuple(self.entries):
            if entry["state"] == "building" and entry["key"] != key:
                self.discard(OntologyGenerationFiles(self, entry["id"]))
        generation_id = uuid4().hex
        (self.root / generation_id).mkdir()
        self.entries.append(dict(id=generation_id, key=key, index_id=None, state="building"))
        self._save()
        return OntologyGenerationFiles(self, generation_id)

    def index_id(self, files):
        return next(entry["index_id"] for entry in self.entries if entry["id"] == files.generation_id)

    def bind(self, files, index_id):
        for entry in self.entries:
            if entry["id"] == files.generation_id:
                entry["index_id"] = index_id
        self._save()

    def promote(self, files):
        for entry in self.entries:
            if entry["state"] == "active":
                entry["state"] = "retired"
            if entry["id"] == files.generation_id:
                entry["state"] = "active"
        self._save()

    def collect(self):
        active = next((entry for entry in self.entries if entry["state"] == "active"), None)
        if active:
            revision = active["key"].rsplit(":", 1)[-1]
            for entry in tuple(self.entries):
                if entry["state"] == "retired" and entry["key"].rsplit(":", 1)[-1] != revision:
                    self.discard(OntologyGenerationFiles(self, entry["id"]))
        retired = [entry for entry in self.entries if entry["state"] == "retired"]
        for entry in retired[:max(0, len(retired) - self.retain + 1)]:
            self.discard(OntologyGenerationFiles(self, entry["id"]))

    def discard(self, files):
        self.entries = [entry for entry in self.entries if entry["id"] != files.generation_id]
        self._save()
        path = self.root / files.generation_id
        if path.is_symlink():
            raise ValueError("refusing to remove a symlinked generation")
        if path.exists():
            shutil.rmtree(path)
