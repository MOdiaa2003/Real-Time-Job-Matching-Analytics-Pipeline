# Kafka Connect worker: Debezium 2.7.3 + Confluent JDBC sink plugin.
#
# Why a custom image: the debezium/connect base image ships Debezium
# source connectors (PostgreSQL etc.) but NOT the Confluent JDBC sink
# connector. The analytics PostgreSQL sink (job_metrics_5m,
# application_alerts with upsert mode) needs the JDBC plugin + drivers.
#
# Note on licensing: the Confluent JDBC connector is under the Confluent
# Community License; fine for a local demo/education project, and it is
# the standard JDBC sink in production Confluent stacks.

FROM debezium/connect:2.7.3.Final

USER root

# The base image varies by OS package manager across versions; handle all.
RUN microdnf install -y unzip curl 2>/dev/null || \
    apt-get update && apt-get install -y --no-install-recommends unzip curl && \
    rm -rf /var/lib/apt/lists/* || \
    apk add --no-cache unzip curl

# Confluent JDBC sink connector 10.7.1 (matches CP 7.6 line used for
# cp-ksqldb-server / cp-schema-registry in this stack).
RUN set -eux; \
    cd /tmp; \
    curl -fSLo jdbc.zip https://d2p6pa8d64rpfa.cloudfront.net/confluentinc-kafka-connect-jdbc-10.7.1.zip; \
    unzip -o jdbc.zip -d /kafka/plugins; \
    rm -rf jdbc.zip; \
    # Ensure the PostgreSQL JDBC driver exists next to the connector
    for drv in /kafka/plugins/kafka-connect-jdbc/postgresql-*.jar; do \
      if [ ! -e "$$drv" ]; then \
        curl -fSLo /kafka/plugins/kafka-connect-jdbc/postgresql-42.7.4.jar \
          https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; \
      fi; \
    done

# Debezium images resolve the plugin path from this env; keep all default
# locations plus the JDBC connector.
ENV CONNECT_PLUGIN_PATH=/kafka/plugins,/kafka/connect

USER kafkaconnect
