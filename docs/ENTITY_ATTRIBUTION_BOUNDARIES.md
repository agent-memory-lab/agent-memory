# Source attribution and registered identity boundaries

## Scope and status

This optional safeguard does **not** implement natural-language NER, quotation
speaker recognition, general coreference, alias learning, KB identity linking, or
fact-relation extraction. The ontology remains a projection of accepted structured
Claims. Registering an entity ID or alias is a host/input assertion, not automatic
identity discovery. A supplied speaker label is metadata, not verified real-world
identity.

## Optional source-attribution gate

```python
from agent_memory.consolidation.attribution import ConservativeAttributionGuard
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline

pipeline = AtomExtractionPipeline(
    existing_generator,
    existing_reviewer,
    attribution_guard=ConservativeAttributionGuard(trusted_scope, "alice"),
)
```

The default is `attribution_guard=None`: the new guard leaves existing generation,
review, fingerprints and model prompts unchanged. No model dependency is added.
The separate [admission precedence correction](ADMISSION_REVIEW_PRECEDENCE.md)
always preserves terminal host decisions, including without this guard. Existing
cached receipts are not retroactively recomputed or repaired.

The opt-in guard is intended for a host-bound self-report path:

- It requires the **exact** host scope and event actor to match before generation.
  The model cannot supply or override either. Different namespaces, tenants, users,
  sessions and actor spellings fail before dispatch.
- It scans the **whole source**, not the model's selected quotation. Recognized
  quotation marks or a finite English/Chinese reporting-word list make attribution
  uncertain. Other subject IDs remain unbound; an alias-looking ID does not count
  as the actor.
- It can only lower an otherwise `ACCEPT` gate to `PENDING_VERIFICATION`. It never
  approves an unsupported candidate, assigns a quoted speaker, merges identities,
  or skips existing source fidelity, retention, admission or authority checks.
  Final admission preserves host `REJECT` and `L0_ONLY` even if this guard holds
  the review gate; those terminal candidates cannot enter domain verification.
  Reviewer `faithfulness` records the reviewer verdict; the final gate and its
  `source_attribution_uncertain` reasons record the separate guard decision.
- Its immutable configuration is captured before awaited adapters run, included in
  the extraction input fingerprint and policy audit, and checked again at publication
  and cached-result reuse. Disabling/changing it requires a new processing request,
  not reuse of an earlier result under another policy.

This is deliberately a **finite risk detector**, not a guarantee of attribution
safety. Colloquial, unpunctuated, nested, multilingual and otherwise unrecognized
reported speech can escape it. Even a known reporting verb used without a quotation,
or one quoted word elsewhere in a long source, can block valid candidates. No risk
signal is not a proof of direct speech. Do not enable this as a general production
coreference solution. Its independent semantic reviewer/verification path remains
necessary.

The post-observation GUM regression diagnostic kept the external baseline's mention
spans and name links fixed. Applying the same risk detector to whole speaker runs
removed many correct links along with errors: test pair recall fell from 44.97% to
4.35%; dev pair precision also fell from 97.03% to 83.78%. This is **not a demonstrated
quality improvement** and must not be used to justify default rollout. These are
external diagnostic figures, not native model-extraction measurements or a blind
benchmark.

## Read-only exact entity candidates

```python
result = await sqlite_ontology_store.find_entity_candidates(
    trusted_scope, "Al", ontology_id="agent.knowledge", ontology_version="1.0.0",
    max_scan=512,
)
# result.scope / ontology_id / ontology_version keep identity boundaries explicit.
# result.status: unknown | candidate | ambiguous | incomplete
# result.entity_ids: exact registered matches, not resolved mention identities
```

- Matching is case-sensitive and exact against the ID, label, and explicitly stored
  aliases. No fuzzy spelling, nickname inference, case folding, pronoun resolution,
  or name-based merge is performed.
- Lookup is exact-partition only. Unlike general recall, it does not inherit tenant,
  user or other parent scopes. Different ontology versions never join.
- One match is only `candidate`; two or more are `ambiguous`. An ID-looking alias
  colliding with another entity's canonical ID remains ambiguous.
- `max_scan` is 1–4096 and counts all active entities in the exact partition/version.
  An overflow returns `incomplete` and **no** partial candidate IDs. It cannot be
  confused with unique or absent identity evidence.
- This method reads the ontology index only. It does not establish that the original
  source is still live, that an indexed alias is semantically correct, that the host
  may disclose it to a particular recipient, or that no concurrent withdrawal occurs
  after the read. Re-authorize source evidence and scope at any eventual use/write
  boundary. No merge/write operation is supplied.

The result type lives in `ontology.model`; the method is an additive SQLite store
capability. Existing `OntologyStore` implementers are not required to implement it,
no MCP endpoint is added, and default recall is not changed.

## Alias withdrawal and compatibility

Existing rows record entity-level source IDs, **not per-alias provenance**. On any
source invalidation touching an entity, retaining its aggregated aliases would keep
identity hints from a withdrawn source. The store now clears **all** aliases on that
entity in the same invalidation transaction, including aliases that a surviving
source might support. The entity's canonical identity/label and existing assertion
lifecycle rules are unchanged. Reprojection from surviving verified sources can
restore their aliases; a fresh source cannot resurrect old aliases merely by reusing
an archived entity ID.

This alias cleanup is an intentional default safety tightening, not fully unchanged
behavior: consumers of stored alias metadata may lose candidate recall until a
rebuild. There is no SQL schema migration, and no per-alias evidence is invented.
Old aliases left by invalidations that occurred **before** this patch cannot be
retrospectively attributed; rebuild the ontology from currently authorized sources
before relying on alias lookup against an existing index. Canonical ID and label
semantic correctness still depends on the existing projection/verification contract.

The only new domain result is additive `OntologyEntityCandidates`; old serialized
entities, existing protocols, core scope schemas, model-output schemas, default
prompts, and default extraction fingerprints remain compatible.
