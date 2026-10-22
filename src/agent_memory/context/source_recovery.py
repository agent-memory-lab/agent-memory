"""Recover exact compressed excerpts through the existing authorized source store.

References are versioned locators, never filesystem paths or executable tools.
The host owns retention and its scope gate. A deleted, archived, changed or
inaccessible source cannot be recovered from a summary.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256

from ..domain import MemoryScope


def _digest(text):
    return sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class SourceRecoveryReference:
    source_event_id: str
    scope_sha256: str
    source_sha256: str
    start: int
    end: int
    excerpt_sha256: str
    schema: str = "source-recovery/1"

    def __post_init__(self):
        if (
            self.schema != "source-recovery/1"
            or type(self.source_event_id) is not str
            or not 1 <= len(self.source_event_id) <= 256
        ):
            raise ValueError("invalid recovery reference")
        for value in (self.scope_sha256, self.source_sha256, self.excerpt_sha256):
            if (
                type(value) is not str
                or len(value) != 64
                or any(c not in "0123456789abcdef" for c in value)
            ):
                raise ValueError("invalid recovery reference digest")
        if (
            type(self.start) is not int
            or type(self.end) is not int
            or not 0 <= self.start < self.end
            or self.end - self.start > 16384
        ):
            raise ValueError("invalid recovery excerpt bounds")


class RepositorySourceRecovery:
    """Uses RetentionUnitOfWork.get_source_event on SQLite or PostgreSQL.

    Only a unique verbatim segment may become a reference. This proves text
    recovery, not factual truth or completeness of a compressed summary.
    """

    def __init__(self, repository):
        self.repository = repository

    async def bind(self, scope, segments):
        if type(scope) is not MemoryScope or not 1 <= len(segments) <= 128:
            raise ValueError("bounded scope-bound segments required")
        result = []
        async with self.repository.unit_of_work() as uow:
            for segment in segments:
                event = await uow.get_source_event(scope, segment.source_event_id)
                if event is None or event.scope != scope:
                    raise ValueError("recovery source unavailable")
                start = event.content.find(segment.text)
                if start < 0 or event.content.find(segment.text, start + 1) >= 0:
                    raise ValueError("recovery excerpt is missing or ambiguous")
                result.append(
                    asdict(
                        SourceRecoveryReference(
                            event.id,
                            _digest(scope.partition_key()),
                            _digest(event.content),
                            start,
                            start + len(segment.text),
                            _digest(segment.text),
                        )
                    )
                )
        return tuple(result)

    async def restore(self, scope, reference):
        ref = SourceRecoveryReference(**reference)
        if type(scope) is not MemoryScope or ref.scope_sha256 != _digest(scope.partition_key()):
            return None
        async with self.repository.unit_of_work() as uow:
            return await self._restore(uow, scope, ref)

    @staticmethod
    async def _restore(uow, scope, ref):
        event = await uow.get_source_event(scope, ref.source_event_id)
        if (
            event is None
            or event.scope != scope
            or _digest(event.content) != ref.source_sha256
            or ref.end > len(event.content)
        ):
            return None
        excerpt = event.content[ref.start : ref.end]
        return excerpt if _digest(excerpt) == ref.excerpt_sha256 else None

    async def validate(self, scope, references):
        if type(references) not in (tuple, list) or not 1 <= len(references) <= 128:
            return False
        if type(scope) is not MemoryScope:
            return False
        async with self.repository.unit_of_work() as uow:
            for reference in references:
                ref = SourceRecoveryReference(**reference)
                if (
                    ref.scope_sha256 != _digest(scope.partition_key())
                    or await self._restore(uow, scope, ref) is None
                ):
                    return False
        return True
