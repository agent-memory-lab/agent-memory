# Research experiments and offline tests

These source-checkout harnesses are opt-in diagnostics, not production defaults or
model-quality claims. They import the repository's current core; no duplicate
core is vendored. Run Python 3.13+ and install `python -m pip install -e '.[dev]'`
from the repository root.

- [Evidence obligations](../docs/EVIDENCE_OBLIGATION_EXPERIMENT.md): eight-arm
  synthetic lifecycle diagnostic, independent evaluator, durable reservations,
  checkpoints and replay. Linux/macOS only (`fcntl`; Windows via WSL).
  Offline tests: `python -m pytest tests/test_evidence*.py`.

- [Public LongMemEval](public_longmemeval/README.md): pinned public sample preparation (data not redistributed),
  frozen development/holdout IDs, local Ollama and HTTPS-provider runners,
  durable transport safeguards and independent evaluation.
  Offline tests: `python -m unittest discover -s experiments/public_longmemeval/tests -v`.
  CI covers Linux, macOS and Windows with mocked model calls.

CI runs the offline tests without model credentials. Live execution is a separate
operator action, requires approved endpoints/budgets and sends supplied source
text to the chosen provider. Do not use private data without permission. Existing
runs must retain their original cumulative accounting, including unresolved calls;
a new checkout or example configuration does not reset prior spending.
