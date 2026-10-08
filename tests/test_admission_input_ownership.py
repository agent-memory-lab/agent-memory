"""Admission must store the same owned inputs used to calculate its fingerprint."""

import asyncio

import test_atom_admission as base

store = base.store


def test_l1_admission_owns_fingerprinted_drafts_before_first_await(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, _):
            event = base.source(scope, "Alice lives in Hangzhou and Shanghai")
            original_draft = base.atom("Hangzhou", source_quote=event.content)
            drafts = [original_draft]
            cls = type(engine.repository.unit_of_work())
            original = cls.lock_admission_scope
            entered, release = asyncio.Event(), asyncio.Event()

            async def paused(self, *args):
                await original(self, *args)
                if asyncio.current_task().get_name() == "audit-admission":
                    entered.set()
                    await release.wait()

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "lock_admission_scope", paused)
                    admission = asyncio.create_task(engine.admit(
                        event, drafts, authority=base.SELF, policy=base.POLICY
                    ), name="audit-admission")
                    await asyncio.wait_for(entered.wait(), 10)
                    drafts[0] = base.atom("Shanghai", source_quote=event.content)
                    release.set()
                    await asyncio.wait_for(admission, 10)
                claims, _ = await engine.state(scope, valid_at=base.at(1), known_at=base.at(30))
                assert [claim.value for claim in claims] == ["Hangzhou"]
                duplicate = await engine.admit(event, [original_draft],
                                               authority=base.SELF, policy=base.POLICY)
                assert duplicate.duplicate
            finally:
                release.set()

    asyncio.run(run())
