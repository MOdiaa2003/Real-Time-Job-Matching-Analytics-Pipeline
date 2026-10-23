# Real-Time Job Matching — Pipeline Control Center
# Shared data-access layer. READ-ONLY: never writes DB, Kafka, or checkpoints.
import json
import subprocess
import warnings
from datetime import datetime

import pandas as pd
import psycopg2
import requests
import streamlit as st

from dashboard import config

# pd.read_sql_query with a raw psycopg2 connection emits a noisy "only supports
# SQLAlchemy" UserWarning in Python 3.12/pandas — harmless for read-only use.
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")


# ---------------------------------------------------------------- postgres ---

@st.cache_data(ttl=30, show_spinner=False)
def analytics_table(table: str, limit: int = 0) -> pd.DataFrame:
    """Read an analytics-DB table read-only. Cached briefly."""
    q = f"SELECT * FROM {table}"
    if limit:
        q += f" LIMIT {int(limit)}"
    try:
        conn = psycopg2.connect(
            host=config.ANALYTICS_HOST, port=config.ANALYTICS_PORT,
            dbname=config.ANALYTICS_DB, user=config.ANALYTICS_USER,
            password=config.ANALYTICS_PASSWORD, connect_timeout=5)
        try:
            return pd.read_sql_query(q, conn)
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame()


# ------------------------------------------------------------- delta exports --

EXPORT_TABLES = [
    "match_results", "candidate_dim", "job_dim",
    "pending_applications", "match_invalidations", "dead_letter_applications",
]


@st.cache_data(ttl=120, show_spinner=False)
def delta_export(table: str) -> pd.DataFrame:
    p = config.EXPORTS / f"{table}.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, low_memory=False)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120, show_spinner=False)
def exports_freshness() -> dict:
    out = {}
    for t in EXPORT_TABLES:
        p = config.EXPORTS / f"{t}.csv"
        if p.exists():
            out[t] = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    return out


def run_exporter() -> bool:
    r = subprocess.run(
        ["python", str(config.REPO / "scripts" / "export_delta_for_bi.py")],
        capture_output=True, text=True, cwd=str(config.REPO), timeout=900)
    delta_export.clear()
    exports_freshness.clear()
    return r.returncode == 0


# ------------------------------------------------------------ engine metrics --

@st.cache_data(ttl=20, show_spinner=False)
def batch_metrics() -> pd.DataFrame:
    p = config.BATCH_METRICS
    if not p.exists():
        return pd.DataFrame()
    rows = []
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ("batch_start_time", "batch_end_time"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df.sort_values("batch_id")


# -------------------------------------------------------------- docker + REST --

@st.cache_data(ttl=30, show_spinner=False)
def docker_services() -> list:
    try:
        r = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True, text=True, timeout=40, cwd=str(config.REPO))
        items = json.loads(r.stdout) if r.stdout.strip().startswith("[") else \
            [json.loads(x) for x in r.stdout.splitlines()
             if x.strip() and x.strip().startswith("{")]
        return [{"name": i.get("Service") or i.get("Name") or i.get("name"),
                 "state": i.get("State") or i.get("state"),
                 "health": i.get("Health") or i.get("health") or "-"}
                for i in items]
    except Exception:
        return []


@st.cache_data(ttl=30, show_spinner=False)
def connectors_status() -> list:
    try:
        names = requests.get(f"{config.CONNECT_URL}/connectors", timeout=10).json()
        out = []
        for n in names:
            try:
                s = requests.get(f"{config.CONNECT_URL}/connectors/{n}/status", timeout=10).json()
                tasks = ",".join(t["state"] for t in s.get("tasks", [])) or "-"
                out.append({"connector": n, "state": s.get("connector", {}).get("state", "?"),
                            "tasks": tasks})
            except Exception:
                out.append({"connector": n, "state": "unreachable", "tasks": "-"})
        return out
    except Exception:
        return []


@st.cache_data(ttl=30, show_spinner=False)
def ksql_queries() -> list:
    try:
        r = requests.post(config.KSQL_URL + "/ksql",
                          json={"ksql": "SHOW QUERIES;", "streamsProperties": {}},
                          headers={"Content-Type": "application/vnd.ksql.v1+json"}, timeout=10)
        queries = r.json()[0].get("queries", [])
        return [{"id": q.get("id", "?"), "state": q.get("state", "?"),
                 "sql": q.get("queryString", "?")[:60]} for q in queries]
    except Exception:
        return []


# --------------------------------------------------------------------- kafka --

_KAFKA_BOOT = "localhost:29092"


@st.cache_data(ttl=30, show_spinner=False)
def kafka_topic_end_offsets() -> dict:
    try:
        r = subprocess.run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                            "--bootstrap-server", _KAFKA_BOOT, "--time", "latest"],
                           capture_output=True, text=True, timeout=40)
        out = {}
        for line in r.stdout.splitlines():
            parts = line.strip().split(":")
            if len(parts) != 3:
                continue
            topic, _p, off = parts
            out.setdefault(topic, 0)
            out[topic] += int(off)
        return dict(sorted(out.items()))
    except Exception:
        return {}


@st.cache_data(ttl=30, show_spinner=False)
def kafka_group_lags() -> pd.DataFrame:
    try:
        r = subprocess.run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                            "--bootstrap-server", _KAFKA_BOOT, "--all-groups", "--describe"],
                           capture_output=True, text=True, timeout=60)
        rows = []
        for line in r.stdout.splitlines():
            f = line.split()
            # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
            if len(f) >= 6 and f[2].isdigit():
                try:
                    rows.append({"group": f[0], "topic": f[1], "lag": int(f[5])})
                except ValueError:
                    pass
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        return df.groupby("group", as_index=False)["lag"].max().sort_values("lag", ascending=False)
    except Exception:
        return pd.DataFrame()


def kafka_total_lag(lagdf) -> int:
    return int(lagdf["lag"].max()) if not lagdf.empty else 0


# ------------------------------------------------------------------- receipts --

@st.cache_data(ttl=300, show_spinner=False)
def acceptance_receipts() -> list:
    suites = {
        "01_pipeline_health": [],
        "02_dedup_dlq_integrity": ["dlq=", "summary:"],
        "03_job_state_invalidation": [],
        "04_windowed_metrics": [],
        "05_jdbc_sink_upsert": [],
        "06_latency_freshness": ["measured:"],
    }
    out = []
    for name, pins in suites.items():
        p = config.REPO / "tests" / name / "results" / "last_run.txt"
        if not p.exists():
            continue
        try:
            lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        ts = lines[0][:19] if lines else ""
        verdict = "PASS" if ": PASS" in lines[0] else ("FAIL" if ": FAIL" in lines[0] else "?")
        detail = next((l.split("-", 2)[-1].strip() for l in lines if any(pin in l for pin in pins)), "")
        out.append({"suite": name, "timestamp": ts, "verdict": verdict, "detail": detail})
    return out
