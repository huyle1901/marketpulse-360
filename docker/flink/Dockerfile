FROM maven:3.9.11-eclipse-temurin-17 AS builder

WORKDIR /build
COPY flink-job/pom.xml flink-job/pom.xml
RUN mvn -q -f flink-job/pom.xml dependency:go-offline
COPY flink-job/src flink-job/src
RUN mvn -q -f flink-job/pom.xml clean package -DskipTests

FROM flink:2.2.1-java17

USER root
RUN mkdir -p /opt/flink/usrlib /opt/flink/checkpoints \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints
COPY --from=builder --chown=flink:flink \
    /build/flink-job/target/marketpulse-flink-job.jar \
    /opt/flink/usrlib/marketpulse-flink-job.jar
COPY --chown=flink:flink docker/flink/submit-job.sh /opt/flink/bin/submit-marketpulse-job
RUN chmod 0555 /opt/flink/bin/submit-marketpulse-job

USER flink
