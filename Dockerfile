FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ ./pipeline/
COPY lean_bridge/ ./lean_bridge/
COPY pyproject.toml .
COPY workspace.yaml .

ENV DAGSTER_HOME=/app/dagster_home
ENV PYTHONPATH=/app

RUN mkdir -p /app/dagster_home /app/manifests /app/data /app/algorithms
