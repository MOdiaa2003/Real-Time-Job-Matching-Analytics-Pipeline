# Kafka Connect worker: Debezium 2.7.3 + JDBC sink plugin + Avro converter.
#
# Why a custom image: the debezium/connect base image ships Debezium
# source connectors but NOT the Confluent JDBC sink connector, and NOT
# the Avro converter that sinks need to read ksqlDB's Avro output.
#
# Sourcing (network-constrained environment): the Confluent Hub CDN
# (cloudfront) is DNS-blocked here, so jars are assembled from Maven
# artifacts + the pinned cp-kafka-connect image instead:
#   STAGE 1 (avro-converter): copies the complete self-contained
#     kafka-connect-avro-converter plugin (converter + schema-registry
#     client + deps, all 7.6.1) out of confluentinc/cp-kafka-connect.
#   STAGE 2 (final): debezium/connect:2.7.3.Final, plus
#     - kafka-connect-jdbc 10.7.3 (io.confluent, Confluent Community License)
#       + common-config 7.6.1 / common-utils 7.6.1 +
#       postgresql-42.7.4 driver (Postgres-only demo; the Hub zip's
#       Oracle/MSSQL/MySQL driver set is intentionally omitted)
#     - the Avro converter plugin from stage 1
#
# NOTE: the Debezium image ALSO ships io.debezium.connector.jdbc.JdbcSinkConnector
# (version-matched 2.7.3.Final). The sink connectors in connectors/*.json use
# THAT one; the Confluent kafka-connect-jdbc plugin is kept for completeness/
# interview discussion of plugin isolation.
FROM confluentinc/cp-kafka-connect:7.6.1 AS avro-converter

# connect-api/connect-runtime are `provided` by the worker - never bundle them
FROM debezium/connect:2.7.3.Final

USER root

ENV PKG=https://packages.confluent.io/maven \
    MC=https://repo1.maven.org/maven2

# Base image loads plugins from CONNECT_PLUGIN_PATH=/kafka/connect
RUN set -eux; \
    mkdir -p /kafka/connect/kafka-connect-jdbc; \
    cd /kafka/connect/kafka-connect-jdbc; \
    curl -fSLo kafka-connect-jdbc-10.7.3.jar \
      "$PKG/io/confluent/kafka-connect-jdbc/10.7.3/kafka-connect-jdbc-10.7.3.jar"; \
    curl -fSLo common-config-7.6.1.jar \
      "$PKG/io/confluent/common-config/7.6.1/common-config-7.6.1.jar"; \
    curl -fSLo common-utils-7.6.1.jar \
      "$PKG/io/confluent/common-utils/7.6.1/common-utils-7.6.1.jar"; \
    curl -fSLo postgresql-42.7.4.jar \
      "$MC/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"; \
    ls -la /kafka/connect/kafka-connect-jdbc

COPY --from=avro-converter /usr/share/java/kafka-serde-tools  /kafka/connect/kafka-connect-avro-converter/
COPY --from=avro-converter /usr/share/java/confluent-common   /kafka/connect/kafka-connect-avro-converter/
COPY --from=avro-converter /usr/share/java/cp-base-new        /kafka/connect/kafka-connect-avro-converter/

USER kafka
