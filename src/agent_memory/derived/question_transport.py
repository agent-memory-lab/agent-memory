"""Closed read/demand surface; registration and semantic authority stay host-owned."""

from copy import deepcopy

from ..retrieval.question_router import QuestionRouter
from .model import DerivedError, identity
from .project_questions import PROJECT_QUESTIONS
from .question_contracts import ALGORITHM, RUNTIME_SCHEMA

OPERATIONS = ("capabilities", "read", "answer", "route", "request", "status", "page_read")
FIELDS = {
    "capabilities": (set(), set()),
    "read": ({"question_id"}, {"valid_at", "known_at"}),
    "answer": ({"query", "dedupe_key"}, {"parameters", "max_steps"}),
    "route": ({"query"}, {"parameters"}),
    "request": ({"question_id", "dedupe_key"}, set()),
    "status": ({"target_id"}, set()),
    "page_read": ({"page_id"}, set()),
}


async def call(service, operation, payload, context):
    """Only trusted transport context supplies principal, scope and permissions."""
    if context.scope != service.scope:
        raise DerivedError("question_scope_mismatch")
    identity(context.actor)
    if type(operation) is not str or operation not in FIELDS or type(payload) is not dict:
        raise DerivedError("invalid_question_request")
    required, optional = FIELDS[operation]
    if not required <= set(payload) or set(payload) - required - optional:
        raise DerivedError("invalid_question_request")
    payload = deepcopy(payload)
    for name in ("question_id", "dedupe_key", "target_id", "page_id"):
        if name in payload:
            identity(payload[name])
    if "query" in payload and (
        type(payload["query"]) is not str or not payload["query"].strip()
        or len(payload["query"]) > 512
    ):
        raise DerivedError("invalid_question_request")
    if "parameters" in payload and type(payload["parameters"]) is not dict:
        raise DerivedError("invalid_question_request")
    if operation == "capabilities":
        from .question_pages import PAGE_TEMPLATE

        async with service.repository.unit_of_work() as uow:
            await service._open(uow)
            pages = getattr(uow, "question_page_contract", None) == PAGE_TEMPLATE
        return dict(
            schema="question-capabilities/1", enabled=True,
            contract=RUNTIME_SCHEMA,
            operations=[op for op in OPERATIONS if op != "page_read" or pages],
            templates=sorted(PROJECT_QUESTIONS), renderer=ALGORITHM,
            compute_modes=["full", "delta", "proof_reuse"], historical=False, models=False,
            default_enabled=False, registration="trusted_host_only",
            context="trusted_host_only", processing_grants="trusted_host_only",
            source_bases=["admitted_l1", "publication_manifest"],
            aliases="exact_registered_only", response_schema="question-answer/1",
            source_completeness="known_authorized_scope_only",
            refresh="shared_durable_budgeted_coverage_target/1",
            pages=PAGE_TEMPLATE if pages else None,
            page_publication="trusted_host_full_or_validated_reuse" if pages else None,
            page_parent_limit=4,
        )
    if operation == "page_read":
        await service.pages.read(payload["page_id"], actor=context.actor)
        return await service.pages.read(payload["page_id"], actor=context.actor)
    if operation == "status":
        return await service.queue.status(payload["target_id"], actor=context.actor)
    if operation == "request":
        return await service.request(actor=context.actor, **payload)
    if operation == "route":
        return await QuestionRouter(service).route(actor=context.actor, **payload)
    if operation == "answer":
        result = await QuestionRouter(service).answer(actor=context.actor, **payload)
    else:
        result = await service.read(actor=context.actor, **payload)
    if result.get("availability_status") == "valid":
        # Reacquire at the transport delivery boundary. Neither an earlier
        # successful direct compute nor its held dictionary authorizes bytes.
        return await service.read(result["question_id"], actor=context.actor)
    return result
