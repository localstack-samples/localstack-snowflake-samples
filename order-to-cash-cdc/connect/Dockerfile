# Kafka Connect worker with the Snowflake Kafka connector.
#
# The worker runs the Kafka 3.7.1 distribution (copied from the official
# apache/kafka image) on a pinned Java 11 JRE, with the connector version and
# its checksum pinned below.
ARG KAFKA_IMAGE=apache/kafka:3.7.1
ARG JRE_IMAGE=eclipse-temurin:11.0.24_8-jre-jammy

FROM ${KAFKA_IMAGE} AS kafka

FROM ${JRE_IMAGE}

ARG CONNECTOR_VERSION=2.1.2
# SHA-256 of snowflake-kafka-connector-2.1.2.jar from Maven Central
# (its published SHA-1 is 206abda3a8ca617789926deb55a9ba223404ff2a).
ARG CONNECTOR_SHA256=9a160a9cf5e3fd62d40cfcb4d97323b0ba61a5be2e2714422d8bbe7cf68cacfb

COPY --from=kafka /opt/kafka /opt/kafka

RUN set -eux; \
    mkdir -p /opt/connect/plugins/snowflake-kafka-connector; \
    jar="snowflake-kafka-connector-${CONNECTOR_VERSION}.jar"; \
    curl -fsSL -o "/opt/connect/plugins/snowflake-kafka-connector/${jar}" \
      "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/${CONNECTOR_VERSION}/${jar}"; \
    echo "${CONNECTOR_SHA256}  /opt/connect/plugins/snowflake-kafka-connector/${jar}" | sha256sum -c -; \
    useradd --system --uid 1001 --home-dir /opt/connect connect; \
    chown -R connect /opt/connect

COPY --chown=connect worker.properties /opt/connect/worker.properties

USER connect
ENV KAFKA_HEAP_OPTS="-Xms256m -Xmx512m" \
    LOG_DIR=/tmp/connect-logs
EXPOSE 8083
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/opt/connect/worker.properties"]
