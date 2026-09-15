from __future__ import annotations

from hashlib import sha256
import os
from pathlib import Path
import re
import stat
import tempfile

from .domain import MemoryScope

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_REFERENCE = re.compile(r"artifact:([0-9a-f]{64}):([0-9a-f]{64}):([0-9a-f]{64})\Z")


class CaptureArtifactError(ValueError):
    pass


class FileCaptureArtifactStore:
    """Optional local, scoped and failure-atomic store for redacted tool results.

    Reference keys are derived from trusted scope, host event ID and payload
    digest. No name, credential or raw output is embedded in a reference.
    """

    def __init__(self, root: str | Path, *, max_bytes: int = 1_048_576) -> None:
        if type(max_bytes) is not int or not 1 <= max_bytes <= 16_777_216:
            raise CaptureArtifactError("max_bytes must be between 1 and 16777216")
        path = Path(root).expanduser()
        if path.is_symlink():
            raise CaptureArtifactError("artifact root cannot be a symlink")
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not stat.S_ISDIR(path.stat().st_mode) or path.stat().st_mode & 0o077:
            raise CaptureArtifactError("artifact root must be private to its owner")
        self._root = path.resolve()
        self._max_bytes = max_bytes

    @staticmethod
    def _identity(scope: MemoryScope, event_id: str) -> tuple[str, str]:
        if not isinstance(scope, MemoryScope):
            raise CaptureArtifactError("artifact scope must be trusted")
        if not isinstance(event_id, str) or not event_id.strip():
            raise CaptureArtifactError("artifact event ID must be non-empty")
        scope_hash = sha256(scope.partition_key().encode("utf-8")).hexdigest()
        event_hash = sha256(event_id.encode("utf-8")).hexdigest()
        return scope_hash, event_hash

    def _path(self, scope_hash: str, event_hash: str, digest: str) -> Path:
        return self._root / scope_hash / event_hash / f"{digest}.bin"

    def _reference_path(self, reference_id: str, scope: MemoryScope) -> Path:
        match = _REFERENCE.fullmatch(reference_id) if isinstance(reference_id, str) else None
        if match is None:
            raise CaptureArtifactError("invalid opaque artifact reference")
        scope_hash = sha256(scope.partition_key().encode("utf-8")).hexdigest()
        if match.group(1) != scope_hash:
            raise CaptureArtifactError("artifact reference is outside the trusted scope")
        return self._path(*match.groups())

    def reference_for(
        self,
        *,
        scope: MemoryScope,
        event_id: str,
        content_hash: str,
    ) -> str:
        if not isinstance(content_hash, str) or not _DIGEST.fullmatch(content_hash):
            raise CaptureArtifactError("artifact digest must be lowercase SHA-256")
        scope_hash, event_hash = self._identity(scope, event_id)
        return f"artifact:{scope_hash}:{event_hash}:{content_hash}"

    async def put(
        self,
        data: bytes,
        *,
        scope: MemoryScope,
        event_id: str,
        content_hash: str,
    ) -> str:
        if not isinstance(data, bytes) or len(data) > self._max_bytes:
            raise CaptureArtifactError("artifact data exceeds the store limit")
        if not isinstance(content_hash, str) or not _DIGEST.fullmatch(content_hash):
            raise CaptureArtifactError("artifact digest must be lowercase SHA-256")
        if sha256(data).hexdigest() != content_hash:
            raise CaptureArtifactError("artifact data does not match its digest")
        reference_id = self.reference_for(
            scope=scope, event_id=event_id, content_hash=content_hash
        )
        scope_hash, event_hash, _ = _REFERENCE.fullmatch(reference_id).groups()
        path = self._path(scope_hash, event_hash, content_hash)
        parent = path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if parent.is_symlink() or parent.stat().st_mode & 0o077:
            raise CaptureArtifactError("artifact directory must be private")
        if path.exists():
            if sha256(path.read_bytes()).hexdigest() != content_hash:
                raise CaptureArtifactError("existing artifact has an invalid digest")
            return reference_id

        descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=parent)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
            return reference_id
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    async def get(self, reference_id: str, *, scope: MemoryScope) -> bytes:
        path = self._reference_path(reference_id, scope)
        data = path.read_bytes()
        digest = _REFERENCE.fullmatch(reference_id).group(3)
        if len(data) > self._max_bytes or sha256(data).hexdigest() != digest:
            raise CaptureArtifactError("stored artifact failed its digest or size check")
        return data

    async def discard(self, reference_id: str, *, scope: MemoryScope) -> None:
        path = self._reference_path(reference_id, scope)
        path.unlink(missing_ok=True)
