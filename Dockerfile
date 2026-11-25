# syntax=docker/dockerfile:1
# One image, both transports:
#   See deploy/compose.yaml for authenticated HTTP with persistent storage.
#   docker run -i IMAGE stdio             -> stdio worker, for DuckDB on the same host
FROM python:3.14-slim

ARG VERSION=0.0.0
ARG GIT_COMMIT=unknown
LABEL org.opencontainers.image.title="vgi-rdf-worker" \
      org.opencontainers.image.description="Transactional RDF-backed SQL tables over VGI" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.licenses="0BSD"

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PORT=8000 \
    VGI_RDF_ROOT=/data VGI_RDF_DATABASE=/data/default VGI_WORKER_SQLITE_PATH=/data/.vgi-runtime.sqlite
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN uv export --locked --no-dev --extra serve --no-emit-project --format requirements-txt > /tmp/requirements.txt \
    && uv pip sync --system /tmp/requirements.txt \
    && uv pip install --system --no-deps . \
    && rm /usr/local/bin/uv /tmp/requirements.txt

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 worker && mkdir /data && chown worker:worker /data
USER worker

COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD python -c "import os,urllib.request;urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8000\")}/health',method='HEAD'),timeout=2)"
ENTRYPOINT ["docker-entrypoint.sh"]
