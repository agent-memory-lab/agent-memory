# GitHub Packages container

Image: `ghcr.io/agent-memory-lab/agent-memory:0.1.1` (Alpha).
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
        "ghcr.io/agent-memory-lab/agent-memory:0.1.1",
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

The workflow publishes Linux amd64 and arm64 images. `0.1.1` identifies this
software release; `latest` follows published releases and `sha-<commit>` identifies
the built source. For a deployment, pin the published `sha256` digest:

```sh
docker buildx imagetools inspect ghcr.io/agent-memory-lab/agent-memory:0.1.1
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

The container workflow builds and tests pull requests. Publishing is restricted
to a trusted main/manual run or a version tag matching package metadata, using
the repository's `GITHUB_TOKEN` with `packages: write`. Tests verify installed
dependencies, non-root execution and a real MCP stdio write/recall round trip;
the published image is pulled by digest and verified again.

Python wheel/sdist distribution remains separate from the container registry.
The repository package gate builds all seven Python distributions, installs them
into a clean environment and checks their contents and entry points.
