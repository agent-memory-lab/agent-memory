# Package and platform validation

The `packages` CI matrix runs the same gate on Ubuntu, macOS, and Windows with
Python 3.13. Platform support evidence is the successful job for the exact tested
commit; adding a workflow or simulating a missing module is not a completed
Windows/macOS run. This gate is scoped to packaging and a SQLite write/recall
round trip, not full backend, model-inference, or production acceptance.

`tools/package_inventory.py` discovers the root project and every immediate
`packages/*/pyproject.toml`. Both build and installed-import checks use this
inventory, currently seven distributions including `agent-memory-local-models`.
Unsupported package layouts fail closed rather than being omitted.

```sh
python -m pip install 'pip>=22.3' build
python tools/package_gate.py --work-dir /absolute/path/outside/checkout/new-gate
```

Use a new external directory for each run. On Windows, pass a Windows path; the
gate chooses the appropriate virtualenv interpreter without shell-specific loops
or activation. Each project builds an sdist and a wheel from that sdist. The gate
then installs all base wheels, resolves their dependencies normally, checks
dependency consistency, and runs isolated Python outside the checkout to verify:

- All distribution versions, import locations, and callable entry points
- Exact packaged PostgreSQL migration bytes against the source tree
- SQLite remember/recall and honest platform-specific RSS availability
- Local model adapter imports without Torch, Transformers, tokenizers, safetensors,
  model weights, inference, or a model download
- Source and every built archive against the existing default release-scan policy

Successful runs save `evidence.json`; CI attaches it separately for each platform.
`--no-build-isolation` uses an already-provisioned build environment. `--build-only`
produces artifacts and explicitly reports `built_only_unverified`; it does not
constitute installed-package acceptance.

## Optional platform and model capabilities

Core import and SQLite operations do not require the Unix-only `resource` module.
`ProcessResourceProbe.rss_bytes()` imports it only when measurement is requested.
On hosts without it, including Windows, the probe raises `NotImplementedError`
before the evaluation scenario runs. It never substitutes zero for an unavailable
measurement. A host can pass its own supported `ResourceProbe` to
`run_resource_evaluation`; existing report and comparison schemas stay unchanged.
Linux reports `ru_maxrss` in KiB and macOS in bytes; the built-in probe normalizes
both to bytes.

The local-model base distribution depends only on core. Actual local inference
also needs the explicitly installed `agent-memory-local-models[inference]` extra
(or `pip install -e 'packages/local-models[inference]'` from the checkout), approved
local artifacts, and the existing model governance checks. Base installation does
not enable inference or grant processing permission.
