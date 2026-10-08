# Q7-23: bounded host-only deterministic page patches

This closes the deterministic partial-block scope of design §17.6 / AM70-T09.
It does not enable free-form LLM patches, arbitrary caller content/provenance,
historical pages, page-as-parent composition, or page mutations through SDK/MCP.

## API and bounds

The already opt-in `QuestionService.pages` host object provides:

```python
from agent_memory.derived.question_page_patches import (
    AppendQuestionBlock, InsertQuestionBlock, ReplaceQuestionBlock, RemoveQuestionBlock,
)

page = await service.pages.patch(
    "overview",
    [ReplaceQuestionBlock(existing_block_id, existing_block_revision, registered_question_id)],
    actor=host_actor,
    expected_revision_id=previous_page["revision_id"],
    expected_certificate_revision_id=previous_page["certificate_revision_id"],
)
```

- `AppendQuestionBlock(block_id, question_id)` appends a new stable identity.
- `InsertQuestionBlock(block_id, question_id, before_block_id, expected_before_revision_id)`
  inserts immediately before an existing, version-matched anchor.
- `ReplaceQuestionBlock(block_id, expected_revision_id, question_id)` retains the stable identity.
- `RemoveQuestionBlock(block_id, expected_revision_id)` removes the version-matched block.

Only exact frozen operation types are admitted. The operation container is copied
before the first await. There are 1–32 ordered operations, 1–16 final blocks,
1–4 already registered same-project QuestionView parents, and at most 128 distinct
original parent header generations. Empty plans and removal of the final block are
rejected. Duplicate question selections are supported; duplicate live block IDs are
not. Hosts supply identifiers only; all bodies, citations, qualifiers and lineage
come from registered, ready, currently authorized QuestionViews.

## Concurrency, freshness and atomicity

Both previous page content and certificate revisions are mandatory. Replacements,
removals and insertion anchors additionally require their exact expected block
revision. Operations are evaluated in order; any failed operation rolls back the
whole plan. Unchanged blocks retain their exact stored revisions and generation
manifests. Every retained answer must match its current parent answer including
source-sensitive structure, after separating current certificate metadata; otherwise
`question_page_retained_block_stale` rejects the plan rather than silently changing
another block. Patch callers must explicitly replace all stale retained blocks.

Conflicts distinguish `question_page_revision_conflict`,
`question_page_certificate_conflict`, `question_page_block_revision_conflict`,
`question_page_block_missing`, `question_page_block_id_conflict`, and
`question_page_patch_parent_unregistered`. Registration changes, unsafe original
inputs and unavailable/stale parents retain their existing precise guard failures.

SQLite `BEGIN IMMEDIATE` and PostgreSQL's existing per-scope advisory transaction
lock serialize reads/CAS/publication with competing patches and deletion. Body,
certificate, dependency edges, pending-validation state and head commit atomically.
The real delivered structure, including citations, status and generation references,
must fit the registered byte budget; persistent immutable-row capacity is also checked.

## Original generations and permission boundaries

Patch generation manifests use internal `question-page-generation/2` with opaque
header-digest keys. Multiple old/new header generations of the same parent coexist;
no instance-key overwrite can remove private original processing lineage. The
manifest includes prior page generation, its consumed validation certificate inputs,
current ready parents and the previous page content/certificate IDs themselves.
Removal therefore does not erase processing history. Only a genuinely independent
full generation can discard unsafe old inputs without loading their bodies.

Before and after each old content/certificate/block body await, the complete original
and consumed certificate lineages are authorized, with the earliest original grant
expiry rechecked after the last await. Publication/delivery rechecks the registered
context, authority fingerprint, current parent proof, clock and whole-page head.
Public manifests retain the existing references-only response schema: opaque IDs and
hashes, never internal source census/header metadata.

A current proof-only refresh preserves exact patched page and block revisions. A full
body regeneration that retains a custom patched layout inherits that layout's old
page/certificate inputs. If the old page cannot be safely loaded, independent full
publication reconstructs the canonical registered layout from current authorized
parents instead. Old rows remain immutable until erased.

## Storage and transport compatibility

No SQL migration or new ledger kind is required. Legacy `/1` full page generation
manifests remain readable. Hosts must upgrade readers/writers together before using
`question-page-generation/2`; older binaries do not understand mixed-generation
parents and must not read newly patched pages. Rollback disables the optional page
surface until compatible readers are restored; authoritative deletion replay remains
mandatory. Existing full/proof-reuse publication and SDK/MCP `page_read` remain.

The capability response adds `page_patch` with contract
`project-question-page-typed-patch/1`, `authority=trusted_host_only`, explicit operation
and capacity limits, expected-version fields and `free_form=false`. It does not add
transport mutation operations or change B5 model capability fields.

Erasure recognizes every retained original content/certificate and parent instance,
not merely the newest head. Deleting an old block revision, old page revision or
original source scrubs the whole affected page instance and all its revisions;
actual SQLite backup and PostgreSQL dump/restore replay follow the same graph. B6
integration must preserve the B5 model-cache cascade through affected QuestionView/page
instances, including historical revisions, rather than restricting it to newest heads.

## Verification

The focused executable suites are:

- `tests/test_question_page_patches_v7.py`: four operations; exact retained revisions;
  page/certificate/block/anchor conflicts; failed multi-op rollback; closed SDK/MCP
  mutation surface; retained stale-answer rejection; mixed original generations;
  private-lineage pre-body denial; body/source-grant/authority/clock races; real
  output-byte capacity; concurrent CAS winner/loser; physical old/new revision
  erasure and actual backup replay.
- `tests/test_question_page_patch_review_v7.py`: independent adversarial regressions,
  including consumed previous proof-only certificate lineage.
- Existing `test_question_pages_v7.py`, `test_question_page_validation_v7.py`,
  `test_question_proof_reuse_v7.py` and `test_question_transport_v7.py` cover compatibility.

Final executed counts/backend evidence are recorded by the B6 acceptance run; this
implementation note alone is not a claim that the complete V7 plan, production cost
benefit, deployment or all inherited V6.1 work has passed.
