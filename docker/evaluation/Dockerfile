FROM maven:3.9.11-eclipse-temurin-17 AS builder
WORKDIR /build
COPY evaluation-job/pom.xml pom.xml
RUN mvn -q dependency:go-offline
COPY evaluation-job/src src
RUN mvn -q package

FROM flink:2.2.1-java17
USER root
RUN mkdir -p /tmp/eval-checkpoints /tmp/eval-streams \
    && chown -R flink:flink /tmp/eval-checkpoints /tmp/eval-streams
COPY --from=builder --chown=flink:flink /build/target/evaluation-job.jar /opt/flink/usrlib/evaluation-job.jar
USER flink
