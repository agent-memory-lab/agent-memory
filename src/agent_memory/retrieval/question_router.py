"""Exact registered-ID / verified-alias routing. Ambiguity always abstains."""

import unicodedata
from copy import deepcopy

from ..derived.model import DerivedError


def normalize_alias(value):
    if type(value) is not str or not value.strip() or len(value) > 512:
        raise DerivedError("invalid_question_alias")
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


class QuestionRouter:
    """No probabilistic interpretation, entity guessing, or model fallback."""

    def __init__(self, service):
        self.service = service

    async def route(self, query, *, actor, parameters=None):
        query, parameters = deepcopy((query, parameters))
        if type(query) is not str or not query.strip() or len(query) > 512:
            return {"route": "abstain", "reason": "unregistered_question", "model_calls": 0}
        normalized = normalize_alias(query)
        async with self.service.repository.unit_of_work() as uow:
            await self.service._open(uow)
            rows = await uow.derived_records(self.service.scope, "question_registration")
            candidates = []
            exact = []
            for item in rows:
                row = item["payload"]
                if row.get("state") != "registered" or actor not in row.get("readers", ()):
                    continue
                if query == row.get("question_id"):
                    exact.append(row)
                elif normalized in row.get("aliases", ()):
                    candidates.append(row)
            candidates = exact or candidates
            if len(candidates) != 1:
                return {
                    "route": "abstain",
                    "reason": "ambiguous_question" if candidates else "unregistered_question",
                    "model_calls": 0,
                }
            row = candidates[0]
            definition = await self.service._registration(uow, row["question_id"], actor)
            expected = definition["spec"]["instance"]["parameters"]
            if parameters is not None and (
                type(parameters) is not dict
                or set(parameters) != set(expected)
                or any(
                    type(parameters[key]) is not type(value) or parameters[key] != value
                    for key, value in expected.items()
                )
            ):
                return {
                    "route": "abstain",
                    "reason": "question_parameter_conflict",
                    "model_calls": 0,
                }
            return {
                "route": "question",
                "question_id": row["question_id"],
                "instance_id": row["instance_id"],
                "parameters": deepcopy(expected),
                "model_calls": 0,
            }

    async def answer(self, query, *, actor, dedupe_key, parameters=None, max_steps=1):
        route = await self.route(query, actor=actor, parameters=parameters)
        if route["route"] != "question":
            return route
        return await self.service.answer(
            route["question_id"], actor=actor, dedupe_key=dedupe_key, max_steps=max_steps
        )
