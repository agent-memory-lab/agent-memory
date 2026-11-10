# Registered identity boundaries

Registering an entity ID or alias is a host/input assertion, not automatic
identity discovery. The ontology remains a projection of accepted structured
Claims. This lookup does not implement NER, coreference, alias learning or
name-based identity merging.

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
