FROM maven:3.9.11-eclipse-temurin-17 AS dependencies

WORKDIR /build
COPY docker/lakehouse/pom.xml ./pom.xml
RUN mvn -q dependency:copy-dependencies -DoutputDirectory=/jars

FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu AS spark-runtime

FROM python:3.12.12-slim-bookworm

ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH} \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=/opt/spark/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 185 spark \
    && useradd --uid 185 --gid 185 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin spark

COPY --from=spark-runtime --chown=185:185 /opt/spark /opt/spark
COPY --from=dependencies /jars/ /opt/spark/jars/
RUN test -f /opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.12.0.jar \
    && test -f /opt/spark/jars/iceberg-aws-bundle-1.12.0.jar \
    && test -f /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.4.jar

WORKDIR /opt/marketpulse
COPY pyproject.toml ./pyproject.toml
COPY src ./src
RUN python3 -m pip install --no-cache-dir '.[lakehouse]'

COPY docker/lakehouse/entrypoint.sh ./entrypoint.sh
RUN chmod 0555 /opt/marketpulse/entrypoint.sh \
    && mkdir -p /opt/marketpulse/evidence \
    && chown -R 185:185 /opt/marketpulse

USER spark
ENTRYPOINT ["/opt/marketpulse/entrypoint.sh"]
