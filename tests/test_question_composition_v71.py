import asyncio
from dataclasses import replace

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.model import DerivedError
from agent_memory.derived.question_composition import CompositeQuestionReader, QuestionPart

store = project.store


def test_cross_scope_composition_requires_explicit_current_authorization(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services = []
            for realm in (scope, replace(scope, session_id="other")):
                svc = runtime(engine, realm, clock)
                item = await project.stage(
                    svc.admission, realm, identity="source:" + realm.session_id
                )
                await project.qualify(svc.admission, *item)
                await register(svc)
                await fresh(svc, clock)
                services.append(svc)
            calls = []
            allowed = [False]

            async def authorize(uow, actor, purpose, parts):
                calls.append(len(parts))
                return allowed[0] and actor == ACTOR and purpose == "portfolio"

            composite = CompositeQuestionReader(
                [QuestionPart(s, "project-a:owner") for s in services], authorize=authorize
            )
            with pytest.raises(DerivedError, match="composition_denied"):
                await composite.read(actor=ACTOR, purpose="portfolio")
            allowed[0] = True
            answer = await composite.read(actor=ACTOR, purpose="portfolio")
            assert len(answer["parts"]) == 2 and answer["model_calls"] == 0
            assert calls == [2, 2, 2]

            def bad_parts():
                return [QuestionPart(s, "project-a:owner") for s in services for _ in range(2)]

            with pytest.raises(DerivedError, match="duplicate"):
                CompositeQuestionReader(bad_parts(), authorize=authorize)

    asyncio.run(run())
