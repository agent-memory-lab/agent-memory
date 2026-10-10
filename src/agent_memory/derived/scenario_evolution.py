"""Registered L2 scene bootstrap on the existing QuestionView page lifecycle."""

from ..operations.refresh_policy import RefreshPolicy


class ScenarioEvolution:
    """Enroll registered project scenes from their first build on the shared queue.

    Questions remain the authoritative read inputs. Existing page maintenance
    preserves revisions/proofs, withdraws unsafe reads and requests stale parents.
    This explicitly finite host registration never derives new L1 authority.
    """

    def __init__(self, questions):
        self.questions, self.repository, self.scope = (
            questions,
            questions.repository,
            questions.scope,
        )
        self.queue = questions.queue

    async def register(
        self,
        page_id,
        question_ids,
        *,
        readers,
        expected_generation=0,
        max_output_bytes=262144,
        refresh_policy=None,
    ):
        registration = await self.questions.pages.register(
            page_id,
            question_ids,
            readers=readers,
            expected_generation=expected_generation,
            max_output_bytes=max_output_bytes,
        )
        async with self.repository.unit_of_work() as uow:
            await self.questions.pages._open(uow)
            definition = await self.questions.pages.maintenance.install(uow, registration)
        await self.queue.configure(
            definition["facet_id"],
            refresh_policy or RefreshPolicy(mode="on_change"),
            processor_key=self.questions.page_processor.key,
        )
        return registration

    async def read(self, page_id, *, actor, **history):
        return await self.questions.pages.read(page_id, actor=actor, **history)

    async def discover_changes(self):
        async with self.repository.unit_of_work() as uow:
            await self.questions.pages.maintenance.initialize(uow)
            return tuple(
                row["identity"]
                for row in await uow.derived_records(self.scope, "definition")
                if row["payload"].get("spec", {}).get("schema") == "question-page-refresh/1"
                and row["payload"].get("dirty")
                and not row["payload"].get("disabled")
            )
