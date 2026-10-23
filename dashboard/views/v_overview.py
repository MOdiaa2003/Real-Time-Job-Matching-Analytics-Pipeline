"""Page: Executive Overview."""
import os
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard import config, sources
from dashboard.components import apply_theme, show_badge, badge, empty_state, kpi, src_note


def render():
    apply_theme()
    st.subheader("Executive Overview")
    st.caption(config.MIGRATION_NOTE +
               " · [LIVE] = read now from running services/exports · [SAVED RUN] = recorded acceptance-test results")

    # ---------------- service health ------------------------------------
    svcs = sources.docker_services()
    left, mid, right = st.columns([2, 2, 3])
    with left:
        if not svcs:
            show_badge(badge("off", "docker: unavailable"))
        else:
            up = sum(1 for s in svcs if s["state"] == "running")
            show_badge(badge("ok" if up == len(svcs) > 0 else "warn",
                              f"docker compose: {up}/{len(svcs)} up"))
    with mid:
        conns = sources.connectors_status()
        if not conns:
            show_badge(badge("off", "Kafka Connect: unavailable"))
        else:
            run = sum(1 for c in conns if c["state"] == "RUNNING")
            show_badge(badge("ok" if run == len(conns) else "err",
                              f"connectors: {run}/{len(conns)} RUNNING"))
    with right:
        topics = sources.kafka_topic_end_offsets()
        lagdf = sources.kafka_group_lags()
        if topics:
            dlq = sum(v for k, v in topics.items() if k.startswith("dlq."))
            tot = sources.kafka_total_lag(lagdf)
            show_badge(badge("ok" if tot <= 20 else "warn",
                              f"kafka · {len(topics)} topics · max lag {tot} · DLQ {dlq:,}"))
        else:
            show_badge(badge("off", "kafka: unreachable"))

    # ---------------- KPI row -------------------------------------------
    bm = sources.batch_metrics()
    mr = sources.delta_export("match_results")
    jl = sources.delta_export("job_dim")
    pa = sources.delta_export("pending_applications")
    dlf = sources.delta_export("dead_letter_applications")
    mdf = sources.analytics_table("job_metrics_5m")
    aldf = sources.analytics_table("application_alerts")

    k1, k2, k3 = st.columns(3)
    with k1:
        n_apps = int(mr["application_id"].nunique()) if not mr.empty and "application_id" in mr else 0
        kpi(f"{n_apps:,}", "applications scored", "[LIVE] Delta match_results")
    with k2:
        n_hp = int((mr["priority"] == "HIGH_PRIORITY").sum()) if not mr.empty and "priority" in mr else 0
        kpi(f"{n_hp:,}", "high-priority matches", "score ≥ 80 [LIVE]")
    with k3:
        open_jobs = int((jl["status"] == "open").sum()) if not jl.empty and "status" in jl else 0
        kpi(f"{open_jobs:,}", "open jobs served", "[LIVE] Delta job_dim")
    k4, k5, k6 = st.columns(3)
    with k4:
        kpi(f"{len(pa):,}", "unresolved (pending)", "[LIVE] Delta pending_applications")
    with k5:
        kpi(f"{len(dlf):,}", "dead-letter applications", "[LIVE] Delta dead_letter_applications")
    with k6:
        kpi(f"{len(aldf):,}", "rate alerts", f"[LIVE] analytics db · {len(mdf):,} metric windows")

    # ---------------- three processing paths ------------------------------
    colA, colB, colC = st.columns(3)
    with colA:
        st.markdown("##### Kafka integration path")
        st.markdown("Postgres **CDC via Debezium** → Kafka **Avro + Schema Registry** → Quix Streams dedup → "
                    "ksqlDB windows/alerts → JDBC sinks → analytics Postgres")
        st.caption(f"[LIVE] records on built-in topics: {sum(topics.values()):,}" if topics else "kafka offline")
    with colB:
        st.markdown("##### Real-time matching path")
        st.markdown("CDC events → **PySpark Structured Streaming** → dims via Delta MERGE → weighted scoring "
                    "(skills 30 / exp 25 / loc 20 / sal 15 / lang 10) → high-priority matches")
        if not bm.empty:
            last = bm.iloc[-1]
            kpi(f"{int(last['batch_id'])}", "latest engine batch",
                f"{last['batch_duration_sec']:.0f} s · scored {int(last['scored_rows']):,} rows")
    with colC:
        st.markdown("##### Batch medallion path")
        st.markdown("Bounded Kafka reads → **Bronze** raw envelopes → **Silver** dedup/validate → "
                    "**Gold** SCD2 dims + facts (`spark_pipeline/batch_processing.py`)")
        lake = os.getenv("LAKE_ROOT")
        present = bool(lake and Path(lake).exists())
        show_badge(badge("ok", "warehouse outputs present: " + lake) if present
                    else badge("off", "warehouse outputs: not on this machine (LAKE_ROOT unset)"))

    # ---------------- charts ----------------------------------------------
    st.divider()
    c1, c2 = st.columns([3, 2])
    with c1:
        if not bm.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=bm["batch_start_time"], y=bm["incoming_rows"],
                                     name="incoming rows", line=dict(color="#6BAFEF")))
            fig.add_trace(go.Scatter(x=bm["batch_start_time"], y=bm["scored_rows"],
                                     name="scored rows", line=dict(color="#8FD2A0")))
            fig.update_layout(template="plotly_dark", height=300,
                              title="Match-engine per-batch volume [LIVE]",
                              xaxis_title="batch start", yaxis_title="rows")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            empty_state("the match-engine metrics file is empty", "start the engine: scripts\\run.ps1")
    with c2:
        if not mdf.empty:
            w = mdf.sort_values("window_start").tail(60)
            fig = px.bar(w, x=w.index.astype(str), y="applications_cnt")
            fig.update_layout(template="plotly_dark", height=300,
                              title="Applications per 5-min window [LIVE]",
                              xaxis_title=None, yaxis_title="applications")
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            empty_state("the analytics db is unreachable",
                        "start docker compose (scripts/run.ps1); Power BI dialog shows the same creds")

    src_note("Kafka offsets/groups read-only via docker exec · Postgres read-only via psycopg2 · "
             "Delta tables via exports/ · console: " + config.CONSOLE_URL)

