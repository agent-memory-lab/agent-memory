# Searchable tool-result evidence

`AgentMemoryAdapter.after_tool` records the tool status in event content and the
raw result/arguments in event metadata. Retrieval indexes content, not arbitrary
metadata. **By default, a result-only fact is not searchable.** This privacy
boundary is unchanged: installing or upgrading the adapter does not expose
previously metadata-only data to retrieval or the next model call.

Hosts can now explicitly supply an approved, sanitized evidence excerpt:

```python
await adapter.after_tool(
    context,
    tool_call_id="lookup-1",
    tool_name="archive.lookup",
    arguments={"document": "zirconium certificate"},
    result=raw_result,
    success=True,
    evidence_text="The zirconium certificate is in the blue archive box.",
)
bundle = await adapter.before_model(context, "Where is the zirconium certificate?")
```

For repeated integrations, configure a trusted host callback. It receives the
tool name and raw result, selects approved fields and returns sanitized text or
`None` to omit evidence. It must not blindly serialize arbitrary results.

```python
def approved_tool_evidence(tool_name, result):
    if tool_name == "archive.lookup" and isinstance(result, dict):
        # This host-reviewed field contains only information approved for recall.
        text = result.get("public_location")
        return text if isinstance(text, str) else None
    return None

adapter = AgentMemoryAdapter(provider, tool_result_projector=approved_tool_evidence)
```

The same constructor options are available on `LangGraphMemoryAdapter`.
LangGraph does not accept an `evidence_text` or projector from graph state or the
tool-event payload; projection must be configured by the host. The callback is
synchronous. An invalid return type or callback failure propagates before the
event is written, rather than silently storing an incomplete result.

## Boundaries and compatibility

- The selected excerpt is appended under `Result evidence:`. Tool success/failure
  and source identity remain intact. This is untrusted tool evidence, not a user
  instruction, approved claim, or current-state fact. Existing explicitly enabled
  extraction/admission policies continue to apply; this feature adds no extractor.
- The default excerpt budget is 4,096 UTF-8 bytes. Set `tool_result_max_bytes` to
  an integer from 32 to 65,536, or `0` to disable all projection. Overlong excerpts
  have a `[truncated]` marker and never split UTF-8 characters. The byte budget is
  for the excerpt, excluding the status line and evidence label.
- Explicit `evidence_text` overrides a configured callback. An empty or whitespace
  string suppresses it for that call; `None` uses the callback, if configured.
- The existing built-in capture redactor is applied to the full selected text
  before truncation. It catches some recognizable credentials and email addresses,
  including those embedded in JSON keys. This is defense in depth, **not a privacy
  guarantee**. Hosts must remove confidential data, identifiers, medical/financial
  details and unrecognized credentials before approving text for recall.
- Arguments, raw results and other metadata are not added to searchable content.
  Their existing persistence/extraction behavior is unchanged; hosts remain
  responsible for sanitizing those inputs before ingestion. The byte budget only
  limits the approved excerpt, not raw metadata storage or callback work.
- No result or artifact reference is automatically expanded, fetched or promoted
  across scopes. The normal retrieval scope, citation and deletion guards apply.
- No database, lifecycle transport or protocol schema migration is required. Old
  status-only events are not backfilled. Existing idempotency semantics are
  unchanged: replaying an old tool-call ID does not rewrite its original event.

This provides an explicit opt-in route for result recall; it does not claim that
default metadata-only results have become searchable or semantically understood.

Tests: `tests/test_adapter_tool_evidence.py` and
`packages/langgraph/tests/test_tool_evidence.py`.
