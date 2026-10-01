# Local Memory Comparison Pilot

## Objective

Compare the local Agent Memory project, Mem0 OSS, and Zep's open-source Graphiti on the same synthetic Chinese interactions, using Ollama `qwen3.5:9b` for memory extraction and answer generation. Use `nomic-embed-text` for embedding-based retrieval in each arm that supports it.

This is an end-to-end pilot, not a claim that the systems have identical internal prompts or storage backends. Mem0 and Graphiti keep their native extraction behavior. Agent Memory uses its `ClaimGenerator` boundary. The same raw role/content/timestamp events and questions go to every arm; gold answers are used only by the evaluator.

## Sandbox

- Fresh local SQLite, Qdrant, and FalkorDB Lite stores per arm and run.
- Fresh Agent Memory scope, Mem0 user ID, and Graphiti group ID.
- Telemetry disabled for Mem0 and Graphiti.
- Synthetic names and facts only; no production users or records.
- Qwen thinking disabled to keep JSON responses bounded and comparable.

## Eval cases

1. **Current fact after correction:** residence changes from Hangzhou to Suzhou. The answer must identify Suzhou as current.
2. **Personal preference:** retrieve the user's cat name and preferred food.
3. **Versioned project setting:** retrieve Python 3.13 and the updated PostgreSQL database after an earlier SQLite setting.
4. **Multi-hop event:** retrieve who is meeting, when, and where.
5. **Deletion:** after clearing the isolated run, every query must return no results.

## Measurements

- Retrieval hit: required aliases appear in any of the top eight returned memory texts.
- Answer hit: one shared Qwen prompt answers from each arm's returned texts, and all required aliases appear in the answer.
- Delete pass: post-delete query results are empty for every question.
- Record add/search/delete/answer durations, input digest, model and package versions, and model-call counts when available.

The pilot uses one run and deterministic substring grading. It does not establish statistical significance, test long-term retention, measure exact token/cost usage, or compare Zep Cloud. Repeat with rotated arm order and a larger held-out dataset before drawing general conclusions.
