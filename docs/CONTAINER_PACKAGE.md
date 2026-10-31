# GitHub Packages container

Image: `ghcr.io/agent-memory-lab/agent-memory:0.1.2` (Alpha).
The image includes Python 3.13, the core library and MCP integration, with SQLite
storage. It does not include model weights or optional PostgreSQL/LangGraph adapters.
It requires neither a model API key nor a running model service.

## MCP client configuration

For clients accepting a `mcpServers` configuration:

```json
{
  "mcpServers": {
    "agent-memory": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i", "-v", "agent-memory-data:/data",
        "ghcr.io/agent-memory-lab/agent-memory:0.1.2",
        "--tenant-id", "demo", "--database", "/data/memory.sqlite3"
      ]
    }
  }
}
```

Replace `demo` with the tenant bound by your trusted host. User/workspace/agent
scope can be fixed with the corresponding CLI options. Scope is not accepted
from model tool arguments. Without arguments, the image prints CLI help.

The process runs as UID/GID 10001. A Docker named volume initialized at `/data`
retains its writable ownership; a host bind mount must be writable by that UID.
Do not remove the volume when restarting the container if its memory is needed.

## Versions and reproducibility

The workflow publishes Linux amd64 and arm64 images. `0.1.2` identifies this
software release; `latest` advances only for a newer version and `sha-<commit>`
identifies the built source. Publication rejects existing version/source tags.
For a deployment, pin the published `sha256` digest:

```sh
docker buildx imagetools inspect ghcr.io/agent-memory-lab/agent-memory:0.1.2
# Use the digest printed by the command above:
docker pull ghcr.io/agent-memory-lab/agent-memory@sha256:<digest>
```

The default packing policy remains v2. Experimental v4/v5 packing repairs do not
constitute complete QA quality or cost acceptance; see the
[repair evaluation](evaluation/PACKING_REPAIR_2026-10-28.md).

## HTTP deployments

The default transport is stdio and no network port is exposed by default.
Streamable HTTP requires an explicitly configured trusted gateway and its HMAC
secret. A container HTTP server must bind `--host 0.0.0.0`, and the host must
explicitly map the chosen port. Follow the [MCP server guide](../packages/mcp-server/README.md)
for gateway authentication. The isolated-development authentication override
is not enabled in the image.

## Build and publish

The Docker build uses an explicit source allowlist and installs freshly built
core and MCP wheels. Repository databases, credentials and benchmark artifacts
are excluded from the build context. The final runtime contains the installed
packages, not the source checkout.

The container workflow builds and tests pull requests. Publication requires the
exact version tag to point to the source commit on main history, including manual
main runs. A repository-wide publication lock serializes releases. The workflow
builds a candidate once, verifies its fixed digest on both architectures, and only
then promotes that same digest to release tags. Candidate tags are not releases.
It uses the repository's `GITHUB_TOKEN` with `packages: write` only in this job.

The base image is pinned by digest. Build and runtime dependencies are separately
locked with hashes in `requirements/container-build.lock` and `container.lock`.
Update these deliberately using the `uv pip compile` commands recorded in their
headers, and review the resulting versions. Trivy scans OS and Python packages;
HIGH/CRITICAL findings or scan failures block promotion. Scan reports remain in
Actions artifacts; a secret-content scan is a separate check.

Runtime checks use the exact candidate digest: non-root execution, a named volume,
write confirmation in v0.1.1 followed by SIGKILL, a new-version restart reading the
committed data, identity isolation, authorized deletion and another restart. HTTP
signatures and tenant isolation are checked over loopback inside the container.
This does not claim arbitrary mid-transaction power-loss testing or a production
TLS/reverse-proxy deployment test; those have separate source-level contracts.

Do not rerun legacy v0.1.1 publication workflows: their old code does not contain
these safeguards. The rules are enforced by this workflow, not registry-level
immutability against manual administrators. Rerunning an already published version
fails instead of rebuilding it. A failed Python-release job can be retried alone;
matching draft assets are reused and conflicting/public assets are never replaced.

Python wheel/sdist distribution remains separate from the container registry.
The repository package gate builds all seven Python distributions, verifies their
full LICENSE and source contents, and checks core plus each optional package in
its own clean environment as well as a combined environment. Packaging CI covers
Python 3.13 and 3.14 on Linux, macOS and Windows. The release job uploads a draft,
downloads those actual attachments, verifies manifest, checksums and source content,
then publishes the Alpha release. It never silently replaces existing attachments.
