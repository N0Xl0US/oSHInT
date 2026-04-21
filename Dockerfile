FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build deps for confluent-kafka wheels.
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc pkg-config libcairo2-dev librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY pyproject.toml README.md ./
COPY maigret ./maigret

# Install app dependencies + package into a relocatable prefix.
RUN pip install --upgrade pip setuptools wheel && \
    pip install --prefix=/install . && \
    pip install --prefix=/install maigret

# Runtime stage
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends libcairo2 librdkafka1 && \
    rm -rf /var/lib/apt/lists/*

# Non-root user.
RUN groupadd -r app && useradd -r -g app app
WORKDIR /app

COPY --from=builder /install /usr/local
COPY --chown=app:app maigret ./maigret
COPY --chown=app:app frontend ./frontend
COPY --chown=app:app pyproject.toml README.md ./

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["python", "-m", "maigret.main"]
