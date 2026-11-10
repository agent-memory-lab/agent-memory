# Registered identity boundaries

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
