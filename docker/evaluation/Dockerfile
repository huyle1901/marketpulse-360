FROM maven:3.9.11-eclipse-temurin-17 AS builder
WORKDIR /build
COPY evaluation-job/pom.xml pom.xml
COPY evaluation-job/src src
RUN --mount=type=cache,id=marketpulse-evaluation-m2,target=/root/.m2 \
    mvn -q package && cp target/evaluation-job.jar /tmp/flink-evaluation.jar \
    && mvn -q package -Dkafka.clients.version=4.3.1 -Devaluation.engine=streams \
    && cp target/evaluation-job.jar /tmp/streams-evaluation.jar

FROM flink:2.2.1-java17
USER root
RUN mkdir -p /tmp/eval-checkpoints /tmp/eval-streams \
    && chown -R flink:flink /tmp/eval-checkpoints /tmp/eval-streams
COPY --from=builder --chown=flink:flink /tmp/flink-evaluation.jar /opt/flink/usrlib/evaluation-job.jar
COPY --from=builder --chown=flink:flink /tmp/streams-evaluation.jar /opt/evaluation/streams-evaluation.jar
USER flink
