# Contributing

## Development setup

```bash
./setup.sh
source .venv/bin/activate
```

Install all optional integration packages when changing an adapter or provider:

```bash
./setup.sh --all
```

## Tests

```bash
pytest -q
pytest -q packages/evolution/tests packages/langgraph/tests \
  packages/python-sdk/tests packages/mcp-server/tests
```

PostgreSQL integration tests require an explicitly configured disposable database. Never
point tests at a production database.

## Bilingual README updates (required)

**Any change to either [README.md](README.md) or [README.zh-CN.md](README.zh-CN.md)
must update the other language in the same pull request and synchronize their capability
baseline. Do not defer the translation to a later PR.**

**任一语言 README 改动，必须在同一 PR 中更新另一语言，并同步能力基线；不得留待后续 PR 补译。**

- Keep supported capabilities, limitations, roadmap status, installation instructions,
  examples, and evidence links equivalent in both languages; natural wording may differ.
- Both capability-status sections must identify the same committed implementation
  stage and link to the same implementation record. Distinguish the software version
  from the architecture version, and report validation against its actual code baseline.
- Update related tables and shared diagrams when their capability claims change.
  A matching stage label alone does not resolve contradictory feature descriptions.
- Reviewers must check both language diffs and the capability baseline before merging.
  Whitespace-only or placeholder edits do not count as updating the other language.
- For direct pushes, keep both language updates in the same commit and perform the
  same review before pushing.

## Architecture rules

- Follow [the subsystem layout and dependency boundaries](docs/ARCHITECTURE.md).
  Group code by capability, then split by responsibility; avoid both one-file-per-class
  fragmentation and generic all-purpose service modules.
- Import concrete subsystem owners in new code. Legacy flat imports remain supported
  for consumers, but do not add new entries to the compatibility table.
- Keep `agent-memory` free of third-party runtime dependencies.
- Depend on `MemoryProvider`, not a concrete database or framework.
- Keep optional imports inside independently installable packages.
- Do not accept tenant, user, or erase authorization from model tool arguments.
- New learned behavior must default off and pass evaluation, promotion, and rollback gates.
- Domain and protocol changes require a schema/version compatibility note.

Submit focused pull requests with tests and update `CHANGELOG.md` for user-visible changes.
