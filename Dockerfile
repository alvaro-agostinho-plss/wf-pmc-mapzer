# ============ Stage 1: Build ============
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ============ Stage 2: Runtime ============
FROM python:3.11-slim

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Dependências do builder
COPY --from=builder /root/.local /home/appuser/.local
ENV PATH=/home/appuser/.local/bin:$PATH

# Copia apenas pastas necessárias para produção (ordem: menos mutável primeiro)
COPY requirements.txt .
COPY src/ ./src/
COPY static/ ./static/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Uploads e logs criados em runtime (volume ou mkdir)
RUN mkdir -p /app/uploads /app/logs \
    && chown -R appuser:appuser /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BASE_PATH=/wfpmcmapzer

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/wfpmcmapzer/health')" || exit 1

USER appuser

CMD ["uvicorn", "src.api.docker_app:app", "--host", "0.0.0.0", "--port", "8000"]
