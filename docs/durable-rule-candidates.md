# Durable rule candidates

Optional persistence for `OntologyRuleEngine` output. This module is not enabled
by default, does not add dependencies, and never promotes candidates to active
ontology assertions or adds them to default recall.

## Integration

The host supplies a trusted `scope`, a configured `engine` with a live evidence
verifier, and root assertion IDs. Do not obtain the trusted scope directly from
untrusted request fields.

```python
from agent_memory.ontology_rule_candidates import (
    DurableRuleCandidates,
    SQLiteRuleCandidateStore,
)

store = SQLiteRuleCandidateStore(
    "rule-candidates.sqlite3", max_records=10000, max_payload_bytes=65536
)
await store.initialize()
memory = DurableRuleCandidates(scope, engine, store)

result = await engine.derive(scope, root_assertion_ids)
for candidate in result.candidates:
    await memory.save(candidate)

# After reopening the database and reconstructing the same engine:
candidate = await memory.get(candidate_id)
if candidate is not None:
    # The host decides whether to use the candidate as a suggestion.
    print(candidate.subject, candidate.predicate, candidate.object)

await memory.archive(candidate_id)
await memory.erase(candidate_id)
```

## Guarantees and limits

- Every identity is partitioned by exact scope. No ancestor visibility is added.
- Save revalidates the candidate against the pinned engine and current evidence.
- Read rederives the proof. Invalid proof returns no candidate and archives it.
- Schema/rule digest mismatches return no candidate without archiving another
  version's record. A host must reconstruct the appropriate engine to read it.
- Identical saves are idempotent. Conflicting payloads are rejected. Archive is
  terminal while the record exists. Erase removes the identity and its payload;
  a later freshly validated save may create it again.
- The store has an explicit total record limit, including archived records, and
  a UTF-8 payload byte limit. Capacity exhaustion rejects writes without eviction.
- Database connections close after each operation. No background task, model,
  vector database or resident candidate cache is required.
- `RuleCandidateStore` is a structural interface for replacement backends.
  Direct store reads are **not evidence-validated**; consume through the facade.

Validation is point-in-time. It is not an atomic transaction across the evidence
source, candidate database and host action. The host must coordinate critical
actions with its own source-version or transaction controls. Evidence verifier
operational failures should raise exceptions, not report evidence as absent.

There is no background invalidation, batch listing, automatic source-forget
subscription or audit history yet. Archived payloads remain stored; privacy
deletion requires explicit `erase` and appropriate backup retention. SQLite
logical deletion does not guarantee forensic erasure of disk pages or backups.
Configure all processes sharing a database with the same capacity limits.

This extension has not yet received its own test acceptance. Earlier passing
test counts do not cover this new module.
