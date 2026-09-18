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

# Install kubectl for cold-start job triggering
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -LO "https://dl.k8s.io/release/v1.28.0/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/kubectl \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Add non-root user and set ownership (required because we use readOnlyRootFilesystem)
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

ENV PYTHONUNBUFFERED=1 \
        PYTHONPATH=/app

EXPOSE 8000

# Health check (kept lightweight)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import requests; import sys; sys.exit(0 if requests.get('http://localhost:8000/health').status_code==200 else 1)" || exit 1

# Use uvicorn executable directly (installed system-wide)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]