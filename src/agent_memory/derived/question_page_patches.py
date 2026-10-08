"""Bounded deterministic host plans for registered QuestionView page blocks.

No text, provenance, evidence or model output is accepted. A patch only selects
current authorized parents and changes the structure of an already published page.
"""

from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import datetime

from .model import DerivedError, digest, identity
from .question_materialize import budget
from .question_pages import PAGE_TEMPLATE, checked, sealed

MAX_PATCH_OPERATIONS = 32
MAX_PAGE_BLOCKS = 16


@dataclass(frozen=True)
class AppendQuestionBlock:
    block_id: str
    question_id: str


@dataclass(frozen=True)
class InsertQuestionBlock:
    block_id: str
    question_id: str
    before_block_id: str
    expected_before_revision_id: str


@dataclass(frozen=True)
class ReplaceQuestionBlock:
    block_id: str
    expected_revision_id: str
    question_id: str


@dataclass(frozen=True)
class RemoveQuestionBlock:
    block_id: str
    expected_revision_id: str


OPERATIONS = (AppendQuestionBlock, InsertQuestionBlock, ReplaceQuestionBlock, RemoveQuestionBlock)


def snapshot_operations(operations):
    # Exact types reject subclasses with hidden prose/provenance payloads. Snapshot
    # once, before the first await, so mutable caller containers cannot change intent.
    if type(operations) not in (tuple, list) or not 1 <= len(operations) <= MAX_PATCH_OPERATIONS:
        raise DerivedError("invalid_question_page_patch")
    result = deepcopy(tuple(operations))
    for operation in result:
        if type(operation) not in OPERATIONS:
            raise DerivedError("invalid_question_page_patch")
        for field in fields(operation):
            identity(getattr(operation, field.name))
    return result


def merge_manifests(*manifests):
    inputs, parents = {}, {}
    for manifest in manifests:
        inputs.update((digest(ref), ref) for ref in manifest["inputs"])
        parents.update(
            ("question-page-parent-generation:" + digest(header), header)
            for header in manifest["parents"].values()
        )
    if len(parents) > 128:
        raise DerivedError("question_page_lineage_capacity")
    return dict(
        schema="question-page-generation/2",
        inputs=[deepcopy(inputs[key]) for key in sorted(inputs)],
        parents={key: deepcopy(parents[key]) for key in sorted(parents)},
    )


def block_position(blocks, block_id, expected_revision_id):
    for index, block in enumerate(blocks):
        if block["block_id"] == block_id:
            if block["id"] != expected_revision_id:
                raise DerivedError("question_page_block_revision_conflict")
            return index
    raise DerivedError("question_page_block_missing")


def new_block(registration, operation, answer, manifest):
    block = dict(
        schema="question-page-block/1",
        instance_id=registration["instance_id"],
        block_id=operation.block_id,
        body=dict(kind="project_question", answer=answer),
        generation_manifest=manifest,
    )
    return sealed({**block, "id": "question-page-block-version:" + digest(block)})


async def apply_patch(
    pages,
    page_id,
    operations,
    *,
    actor,
    expected_revision_id,
    expected_certificate_revision_id,
):
    service, scope = pages.service, pages.scope
    async with service.repository.unit_of_work() as uow:
        await pages._open(uow)  # SQLite writer lock / PostgreSQL scope advisory lock.
        registration = await pages._registration(uow, page_id, actor)
        old_head = checked(
            await uow.derived_get(scope, "question_page_head", registration["instance_id"]),
            "question-page-head/1",
        )
        if old_head["content_revision_id"] != expected_revision_id:
            raise DerivedError("question_page_revision_conflict")
        if old_head["certificate_revision_id"] != expected_certificate_revision_id:
            raise DerivedError("question_page_certificate_conflict")
        if old_head["registration_sha256"] != registration["sha256"]:
            raise DerivedError("question_page_registration_conflict")
        # Old current certificates may be stale; their original processing safety
        # may not be. Guard it before reading any old page bodies.
        await pages._original_guard(uow, registration, old_head["generation_parents"], actor)
        parents = await pages._parents(uow, registration, actor)
        answers = [
            await service._read_in_uow(uow, p["question_id"], actor=actor)
            for p in registration["parents"]
        ]
        current = dict(
            schema="question-page-generation/1",
            parents=deepcopy(parents),
            inputs=[
                ref
                for answer in answers
                for ref in (
                    dict(kind="derived_content", id=answer["content_revision_id"]),
                    dict(kind="derived_certificate", id=answer["certificate_revision_id"]),
                )
            ],
        )
        split = {a["question_id"]: pages._split_answer(a) for a in answers}
        old_content, old_certificate, old_blocks = await pages._load(
            uow, old_head, registration, actor
        )
        blocks = list(old_blocks)
        for operation in operations:
            if type(operation) is RemoveQuestionBlock:
                blocks.pop(
                    block_position(blocks, operation.block_id, operation.expected_revision_id)
                )
                continue
            if operation.question_id not in split:
                raise DerivedError("question_page_patch_parent_unregistered")
            block = new_block(registration, operation, split[operation.question_id][0], current)
            if type(operation) is ReplaceQuestionBlock:
                index = block_position(blocks, operation.block_id, operation.expected_revision_id)
                blocks[index] = block
            else:
                if any(b["block_id"] == operation.block_id for b in blocks):
                    raise DerivedError("question_page_block_id_conflict")
                index = len(blocks)
                if type(operation) is InsertQuestionBlock:
                    index = block_position(
                        blocks, operation.before_block_id, operation.expected_before_revision_id
                    )
                blocks.insert(index, block)
        if not 1 <= len(blocks) <= MAX_PAGE_BLOCKS:
            raise DerivedError("question_page_block_capacity")
        for block in blocks:
            question_id = block["body"]["answer"]["question_id"]
            if question_id not in split or block["body"]["answer"] != split[question_id][0]:
                raise DerivedError("question_page_retained_block_stale")
        # Structural selection consumed the previous page. Conservatively retain
        # all that page's processing lineage, even for removed blocks; only an
        # independent full generation may discard it. Never collapse by instance.
        manifest = merge_manifests(
            old_content["generation_manifest"],
            old_certificate["validation_manifest"],
            current,
            dict(
                parents={},
                inputs=[
                    dict(kind="derived_content", id=old_content["id"]),
                    dict(kind="derived_certificate", id=old_certificate["id"]),
                ],
            ),
        )
        content = dict(
            schema="question-page-content/1",
            instance_id=registration["instance_id"],
            page_id=page_id,
            template=PAGE_TEMPLATE,
            rebuild="typed_patch",
            registration_sha256=registration["sha256"],
            blocks=[
                dict(block_id=b["block_id"], revision_id=b["id"], sha256=b["sha256"])
                for b in blocks
            ],
            generation_manifest=manifest,
        )
        content = sealed({**content, "id": "question-page-content:" + digest(content)})
        certificate = dict(
            schema="question-page-certificate/1",
            instance_id=registration["instance_id"],
            content_revision_id=content["id"],
            generation_manifest=manifest,
            validation_manifest=current,
            answer_proofs={key: value[1] for key, value in split.items()},
            rebuild="typed_patch",
            registration_sha256=registration["sha256"],
            valid_until=min((a["valid_until"] for a in answers), key=datetime.fromisoformat),
        )
        certificate = sealed(
            {**certificate, "id": "question-page-certificate:" + digest(certificate)}
        )
        head = sealed(
            dict(
                schema="question-page-head/1",
                instance_id=registration["instance_id"],
                content_revision_id=content["id"],
                content_sha256=content["sha256"],
                certificate_revision_id=certificate["id"],
                certificate_sha256=certificate["sha256"],
                registration_sha256=registration["sha256"],
                parents=parents,
                generation_parents=manifest["parents"],
                valid_until=certificate["valid_until"],
            )
        )
        budget(pages._response(content, certificate, blocks), registration["max_output_bytes"])
        # Recheck immutable inputs after all body awaits, before publication.
        if await pages._load(uow, old_head, registration, actor) != (
            old_content,
            old_certificate,
            old_blocks,
        ):
            raise DerivedError("question_page_revision_conflict")
        for answer in answers:
            if await service._read_in_uow(uow, answer["question_id"], actor=actor) != answer:
                raise DerivedError("question_page_stale")
        if (
            await uow.derived_get(scope, "question_page_head", registration["instance_id"])
            != old_head
        ):
            raise DerivedError("question_page_revision_conflict")
        await pages._store(uow, registration, content, certificate, blocks, head)
        if await pages._load(uow, head, registration, actor, current=True) != (
            content,
            certificate,
            blocks,
        ):
            raise DerivedError("question_page_revision_conflict")
        await pages._guard(uow, page_id, actor, registration, head)
