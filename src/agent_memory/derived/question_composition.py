"""Explicit current cross-scope composition, without retaining copied private bodies."""

from dataclasses import dataclass
from datetime import datetime

from .model import DerivedError, identity
from .question_materialize import budget


@dataclass(frozen=True, slots=True)
class QuestionPart:
    service: object
    question_id: str

    def __post_init__(self):
        from .question_service import QuestionService

        if type(self.service) is not QuestionService:
            raise DerivedError("question_composition_trusted_service_required")
        identity(self.question_id)


class CompositeQuestionReader:
    def __init__(self, parts, *, authorize, max_output_bytes=262144):
        self.parts = tuple(parts)
        if (
            not 1 <= len(self.parts) <= 4
            or any(type(p) is not QuestionPart for p in self.parts)
            or not callable(authorize)
            or type(max_output_bytes) is not int
            or not 1024 <= max_output_bytes <= 1048576
        ):
            raise DerivedError("invalid_question_composition")
        self.repository = self.parts[0].service.repository
        if any(p.service.repository is not self.repository for p in self.parts):
            raise DerivedError("question_composition_atomic_repository_required")
        keys = [(p.service.scope.partition_key(), p.question_id) for p in self.parts]
        if len(set(keys)) != len(keys):
            raise DerivedError("question_composition_duplicate_part")
        self.authorize, self.maximum = authorize, max_output_bytes
        self._bindings = tuple(
            (p.service.repository, p.service.scope, p.service.admission, p.service.context)
            for p in self.parts
        )

    def _binding(self):
        if (
            tuple(
                (p.service.repository, p.service.scope, p.service.admission, p.service.context)
                for p in self.parts
            )
            != self._bindings
        ):
            raise DerivedError("question_composition_registration_changed")

    async def read(self, *, actor, purpose):
        self._binding()
        identity(actor)
        identity(purpose)
        parts = tuple(
            sorted(self.parts, key=lambda p: (p.service.scope.partition_key(), p.question_id))
        )
        for p in parts:
            await p.service._clock_barrier()
        async with self.repository.unit_of_work() as uow:
            for scope in {p.service.scope.partition_key(): p.service.scope for p in parts}.values():
                await uow.lock_admission_scope(scope)
            if await self.authorize(uow, actor, purpose, parts) is not True:
                raise DerivedError("question_composition_denied")
            self._binding()
            answers = [
                await p.service._read_in_uow(uow, p.question_id, actor=actor, record_usage=False)
                for p in parts
            ]
            # The last parent I/O cannot leave the first parent's old safety proof unchecked.
            for p, answer in zip(parts, answers, strict=True):
                current = await p.service._read_in_uow(
                    uow, p.question_id, actor=actor, record_usage=False
                )
                if current != answer:
                    raise DerivedError("question_composition_parent_changed")
            if await self.authorize(uow, actor, purpose, parts) is not True:
                raise DerivedError("question_composition_denied")
            self._binding()
            for p, answer in zip(parts, answers, strict=True):
                if (
                    p.service.clock() >= datetime.fromisoformat(answer["valid_until"])
                    or p.service.context.expires_at <= p.service.clock()
                ):
                    raise DerivedError("question_composition_expired")
            return budget(
                dict(
                    schema="composite-question-answer/1",
                    mode="current",
                    purpose=purpose,
                    parts=[
                        dict(scope_key=p.service.scope.partition_key(), answer=answer)
                        for p, answer in zip(parts, answers, strict=True)
                    ],
                    model_calls=0,
                ),
                self.maximum,
            )
