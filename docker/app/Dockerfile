FROM python:3.12.13-slim-bookworm
ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY docker/app/requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY pyproject.toml /app/
COPY src/ /app/src/
RUN pip install --no-cache-dir --no-deps . && useradd --create-home --uid 10001 marketpulse
USER marketpulse
CMD ["uvicorn", "marketpulse_app.api:runtime_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
