FROM python:3.12.12-slim-bookworm

RUN groupadd --system marketpulse \
    && useradd --system --gid marketpulse --create-home marketpulse

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[kafka]'

COPY contracts ./contracts

# Durable replay cursors live on a named volume; the mount point must belong to the service user.
RUN mkdir -p /app/state && chown marketpulse:marketpulse /app/state

USER marketpulse
ENTRYPOINT ["marketpulse-mock"]
