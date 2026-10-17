#!/usr/bin/env python3
"""
Debezium CDC Connector Registration
====================================
This script owns the CDC leg of the architecture:

    PostgreSQL -> WAL -> Debezium -> Kafka -> {candidate,job,application}_events

It does NOT generate data and does NOT talk to Postgres directly for writes.
It only talks to the Kafka Connect REST API to register (or update) a
Debezium PostgreSQL source connector, which then reads the write-ahead log
via logical decoding and publishes change events to Kafka on its own.

PREREQUISITES (one-time, outside of Python):
----------------------------------------------
1. Postgres must have logical replication enabled:
       wal_level = logical
       (in postgresql.conf, requires a restart)

2. The connecting Postgres user needs REPLICATION privilege:
       ALTER ROLE <user> WITH REPLICATION;

3. Kafka Connect must be running with the Debezium Postgres connector
   plugin installed (e.g. via the debezium/connect Docker image, or
   confluentinc/cp-kafka-connect + debezium-connector-postgresql jar).

4. generate_data.py must have already run ensure_schema() at least once,
   so candidate_info / job_posts / applications have REPLICA IDENTITY FULL
   and the created_at/updated_at/version columns Debezium will report.

This script is idempotent: re-running it PUTs the same config, which
Kafka Connect treats as an update if the connector already exists.
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "candidate-job-matching-cdc")

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # ── Source Postgres connection ──────────────────────────────
        # NOTE: this hostname is resolved *inside* the kafka-connect
        # container, not on your host machine — so it must be the Docker
        # Compose service name ("postgres"), never "localhost". That's
        # different from POSTGRES_HOST, which generate_data.py uses to
        # connect from the host via the published 5432 port.
        "database.hostname": os.getenv("DEBEZIUM_DB_HOST", "postgres"),
        "database.port": os.getenv("POSTGRES_PORT", "5432"),
        "database.user": os.getenv("POSTGRES_USER", "postgres"),
        "database.password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "database.dbname": os.getenv("POSTGRES_DB", "postgres"),

        # Unique logical name for this Postgres instance; prefixes every
        # topic Debezium creates, e.g. <server>.public.candidate_info
        "topic.prefix": os.getenv("DEBEZIUM_TOPIC_PREFIX", "talent"),

        # ── Logical decoding ─────────────────────────────────────────
        "plugin.name": "pgoutput",
        "slot.name": os.getenv("DEBEZIUM_SLOT_NAME", "talent_cdc_slot"),
        "publication.autocreate.mode": "filtered",

        # ── Which tables to capture ──────────────────────────────────
        "table.include.list": "public.candidate_info,public.job_posts,public.applications",

        # ── Emit full before/after state (matches REPLICA IDENTITY FULL) ──
        "tombstones.on.delete": "false",

        # ── Route the 3 auto-named tables onto your target topic names ──
        # talent.public.candidate_info   -> candidate_events
        # talent.public.job_posts        -> job_events
        # talent.public.applications     -> application_events
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": r"talent\.public\.(candidate_info|job_posts|applications)",
        "transforms.route.replacement": "$1",

        # ── Payload shape ─────────────────────────────────────────────
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
}

# Map raw captured table -> desired downstream topic name.
# (RegexRouter above renames talent.public.<table> -> <table>; if you'd
# rather land directly on candidate_events/job_events/application_events,
# swap the replacement group below.)
TOPIC_NAME_OVERRIDES = {
    "candidate_info": "candidate_events",
    "job_posts": "job_events",
    "applications": "application_events",
}


def build_config_with_topic_names():
    """Return a connector config that routes straight to the final topic names."""
    cfg = json.loads(json.dumps(CONNECTOR_CONFIG))  # deep copy
    table_to_topic = ";".join(f"{src}:{dst}" for src, dst in TOPIC_NAME_OVERRIDES.items())
    cfg["config"]["transforms"] = "route"
    cfg["config"]["transforms.route.type"] = "org.apache.kafka.connect.transforms.RegexRouter"
    cfg["config"]["transforms.route.regex"] = r"talent\.public\.(candidate_info|job_posts|applications)"
    # RegexRouter can't do arbitrary per-table renames in one rule, so we
    # register three connectors' worth of routing via a single regex per
    # table when names differ this much — simplest robust approach is one
    # SMT chain per table using multiple transforms.
    cfg["config"]["transforms"] = "routeCandidate,routeJob,routeApplication"
    cfg["config"]["transforms.routeCandidate.type"] = "org.apache.kafka.connect.transforms.RegexRouter"
    cfg["config"]["transforms.routeCandidate.regex"] = r"talent\.public\.candidate_info"
    cfg["config"]["transforms.routeCandidate.replacement"] = "candidate_events"
    cfg["config"]["transforms.routeJob.type"] = "org.apache.kafka.connect.transforms.RegexRouter"
    cfg["config"]["transforms.routeJob.regex"] = r"talent\.public\.job_posts"
    cfg["config"]["transforms.routeJob.replacement"] = "job_events"
    cfg["config"]["transforms.routeApplication.type"] = "org.apache.kafka.connect.transforms.RegexRouter"
    cfg["config"]["transforms.routeApplication.regex"] = r"talent\.public\.applications"
    cfg["config"]["transforms.routeApplication.replacement"] = "application_events"
    return cfg


def register_connector(use_final_topic_names: bool = True):
    cfg = build_config_with_topic_names() if use_final_topic_names else CONNECTOR_CONFIG
    url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/config"

    resp = requests.put(url, headers={"Content-Type": "application/json"}, data=json.dumps(cfg["config"]))

    if resp.status_code in (200, 201):
        print(f"✅ Connector '{CONNECTOR_NAME}' registered/updated.")
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"❌ Failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()


def connector_status():
    url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"
    resp = requests.get(url)
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"Status check failed ({resp.status_code}): {resp.text}")


def delete_connector():
    url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}"
    resp = requests.delete(url)
    print(f"Delete status: {resp.status_code}")


if __name__ == "__main__":
    import sys

    action = sys.argv[1] if len(sys.argv) > 1 else "register"

    if action == "register":
        register_connector()
    elif action == "status":
        connector_status()
    elif action == "delete":
        delete_connector()
    else:
        print("Usage: python register_debezium_connector.py [register|status|delete]")