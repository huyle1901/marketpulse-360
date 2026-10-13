FROM marketpulse-360/spark-lakehouse:4.0.4-iceberg-1.12.0

USER root
WORKDIR /opt/marketpulse
COPY docker/batch/requirements.lock /opt/marketpulse/requirements.lock
RUN python3 -m pip install --no-cache-dir --require-hashes -r /opt/marketpulse/requirements.lock
COPY pyproject.toml ./pyproject.toml
COPY src ./src
RUN python3 -m pip install --no-cache-dir --no-deps . \
    && mkdir -p /opt/marketpulse/dagster-home /opt/marketpulse/batch-state \
    && chown -R 185:185 /opt/marketpulse/dagster-home /opt/marketpulse/batch-state
COPY docker/batch/dagster.yaml /opt/marketpulse/dagster-home/dagster.yaml
COPY docker/batch/workspace.yaml /opt/marketpulse/workspace.yaml
ENV DAGSTER_HOME=/opt/marketpulse/dagster-home \
    BATCH_STATE_PATH=/opt/marketpulse/batch-state \
    PYTHONPATH=/opt/marketpulse/src
USER spark
ENTRYPOINT []
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "marketpulse_batch.runtime"]
