FROM python:3.13-slim-bookworm@sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build/core
COPY requirements/container-build.lock requirements/container.lock /requirements/
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/
COPY packages/mcp-server/pyproject.toml packages/mcp-server/README.md /build/mcp/
COPY LICENSE /build/mcp/LICENSE
COPY packages/mcp-server/src/ /build/mcp/src/

# Resolve both local wheels together so MCP cannot select an older registry core.
RUN python -m pip install --require-hashes -r /requirements/container-build.lock \
    && python -m build --no-isolation --wheel --outdir /wheels /build/core \
    && python -m build --no-isolation --wheel --outdir /wheels /build/mcp \
    && python -m venv /opt/venv \
    && /opt/venv/bin/python -m pip install --require-hashes --only-binary=:all: -r /requirements/container.lock \
    && /opt/venv/bin/python -m pip install --no-deps /wheels/*.whl \
    && /opt/venv/bin/python -m pip check

FROM python:3.13-slim-bookworm@sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f AS runtime

LABEL org.opencontainers.image.source="https://github.com/agent-memory-lab/agent-memory" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.title="Agent Memory MCP" \
      org.opencontainers.image.description="SQLite memory with the Agent Memory stdio MCP server"

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /data app \
    && install -d -m 0750 -o app -g app /data
COPY --from=builder /opt/venv /opt/venv

USER 10001:10001
WORKDIR /data
ENTRYPOINT ["agent-memory-mcp"]
CMD ["--help"]
