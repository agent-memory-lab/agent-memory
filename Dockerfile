FROM python:3.13-alpine3.24@sha256:7984cf56ceaddebe4fefeb75d64000b9f45d8b77b11a09b24524da6cdc3af0c8 AS builder

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
    && /opt/venv/bin/python -m pip check \
    && /opt/venv/bin/python -m pip uninstall --yes pip

FROM python:3.13-alpine3.24@sha256:7984cf56ceaddebe4fefeb75d64000b9f45d8b77b11a09b24524da6cdc3af0c8 AS runtime

LABEL org.opencontainers.image.source="https://github.com/agent-memory-lab/agent-memory" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.title="Agent Memory MCP" \
      org.opencontainers.image.description="SQLite memory with the Agent Memory stdio MCP server"

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Installers are build tools; keep their vendored dependencies out of the server.
RUN /usr/local/bin/python -m pip uninstall --yes pip \
    && /usr/local/bin/python -c "import ensurepip, pathlib, shutil; shutil.rmtree(pathlib.Path(ensurepip.__file__).parent)" \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app -h /data app \
    && install -d -m 0750 -o app -g app /data
COPY --from=builder /opt/venv /opt/venv

USER 10001:10001
WORKDIR /data
ENTRYPOINT ["agent-memory-mcp"]
CMD ["--help"]
