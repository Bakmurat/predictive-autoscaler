############################
# Builder Stage
############################
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies only if needed (kept minimal for now)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies system-wide so they are available to any user
RUN pip install --no-cache-dir -r requirements.txt

############################
# Final Runtime Image
############################
FROM python:3.12-slim

WORKDIR /app

# (Optional) Minimal runtime packages (none needed now) + cleanup
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy installed site-packages and binaries from builder
COPY --from=builder /usr/local /usr/local

# Copy application source
COPY . .

# Install kubectl for cold-start job triggering. TARGETARCH must be re-declared in this stage:
# without it the URL had an empty architecture and the download was an XML error page, so kubectl
# was not executable on arm64 nodes (found by the in-cluster test run, 2026-09-20).
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fLO "https://dl.k8s.io/release/v1.35.9/bin/linux/${TARGETARCH}/kubectl" \
    && case "${TARGETARCH}" in \
         amd64) sum=3cfeaf80be482b435b0aa214aff6e0b2c312ee23c0ff20810c75517b6004c6eb ;; \
         arm64) sum=39c98bca82875d9a9ddfb6f3c5ab17c3f78ea0801230683e538a67d5c053308d ;; \
         *) echo "no pinned kubectl checksum for ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && echo "${sum}  kubectl" | sha256sum -c - \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/kubectl \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# A fixed numeric non-root user: Kubernetes' runAsNonRoot can only verify a numeric USER, and the chart runs the
# forecaster and the trainer as 10001 with a read-only root filesystem (writable: /models and /tmp).
RUN useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin app && chown -R 10001:10001 /app
USER 10001:10001

ENV PYTHONUNBUFFERED=1 \
        PYTHONPATH=/app

EXPOSE 8000

# Health check (kept lightweight)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import requests; import sys; sys.exit(0 if requests.get('http://localhost:8000/health').status_code==200 else 1)" || exit 1

# Use uvicorn executable directly (installed system-wide)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]