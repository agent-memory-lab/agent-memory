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

## Architecture rules

- Keep `agent-memory` free of third-party runtime dependencies.
- Depend on `MemoryProvider`, not a concrete database or framework.
- Keep optional imports inside independently installable packages.
- Do not accept tenant, user, or erase authorization from model tool arguments.
- New learned behavior must default off and pass evaluation, promotion, and rollback gates.
- Domain and protocol changes require a schema/version compatibility note.

Submit focused pull requests with tests and update `CHANGELOG.md` for user-visible changes.

