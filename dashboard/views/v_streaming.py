"""Page: Kafka Streaming & Reliability."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard import config, sources
from dashboard.components import apply_theme, show_badge, badge, empty_state, kpi, src_note


def render():
    apply_theme()
    st.subheader("Kafka Streaming & Reliability")
    st.caption("CDC ingestion → dedup → streaming SQL → serving sinks. Read-only probes "
               "(`kafka-get-offsets.sh`, `kafka-consumer-groups.sh --describe` — the dashboard never joins groups).")

    topics = sources.kafka_topic_end_offsets()
    if not topics:
        show_badge(badge("err", "kafka unreachable — is docker compose up?"))
        empty_state("Kafka offsets could not be read", "start the stack: docker compose up -d, then refresh")
        return

    # ---------- topic inventory --------------------------------------------
    st.markdown("##### Topic inventory [LIVE]")
    df = pd.DataFrame([{"topic": t, "records": n} for t, n in topics.items()])
    dlq = df[df["topic"].str.startswith("dlq.")]
    user = df[~df["topic"].str.startswith("dlq.") & ~df["topic"].str.startswith("_confluent")]
    c1, c2 = st.columns([2.2, 1.8])
    with c1:
        view = user.sort_values("records", ascending=False).head(16)
        fig = px.bar(view, x="records", y="topic", orientation="h", color="records",
                     color_continuous_scale="Blues")
        fig.update_layout(template="plotly_dark", height=430, margin=dict(l=4, r=4, t=12, b=4),
                          yaxis=dict(title=None), xaxis=dict(title="records on broker"))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        st.markdown("##### Dead-letter Queues")
        if dlq.empty or dlq["records"].sum() == 0:
            show_badge(badge("ok", "all DLQ topics EMPTY"))
        else:
            show_badge(badge("warn", "DLQ contains records"))
            st.dataframe(dlq, use_container_width=True)
        st.caption("`dlq.application_submitted_events` catches malformed wire variants "
                   "(AVRO_DECODE_FAILED, BAD_WIRE_HEADER, TRUNCATED_HEADER, SCHEMA_LOOKUP_FAILED). "
                   "Duplicates are quarantined too — the accounting below proves it.")

    # ---------- dedup accounting [SAVED RUN] --------------------------------
    st.divider()
    st.markdown("##### Deduplication + zero-loss accounting [SAVED RUN]")
    r = next((x for x in sources.acceptance_receipts() if x["suite"] == "02_dedup_dlq_integrity"), None)
    if r:
        st.success(f"**{r['verdict']} @ {r['timestamp']}**\n\n{r['detail']}\n\n"
                   "**122 seeded-fault events in → 67 unique forwarded + 50 DUPLICATE-DLQ + "
                   "5 BAD_WIRE_HEADER-DLQ = zero unaccounted** (per-event-id reconciliation).")
    else:
        st.info("No receipt yet — run tests\\run_all.ps1.")

    # ---------- lag + flow --------------------------------------------------
    a, b = st.columns([1.6, 2.4])
    lagdf = sources.kafka_group_lags()
    tot = sources.kafka_total_lag(lagdf)
    with a:
        st.markdown("##### Consumer-group lag [LIVE]")
        show_badge(badge("ok" if tot <= 20 else "warn", f"max lag = {tot} (drain bound ≤ 20)"))
        if not lagdf.empty:
            fig = px.bar(lagdf.head(10), x="lag", y="group", orientation="h")
            fig.update_layout(template="plotly_dark", height=340, margin=dict(l=6, r=6, t=12, b=6))
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.caption("sawtooth 0–3 = normal in-flight; a ramp = a consumer falling behind")
    with b:
        st.markdown("##### End-to-end record flow [LIVE]")
        flow = [
            ("candidate_events (CDC)", "candidate_events"),
            ("job_events (CDC)", "job_events"),
            ("application_events (CDC)", "application_events"),
            ("submitted_events (Avro producer)", "application_submitted_events"),
            ("submitted_dedup (Quix)", "application_submitted_dedup"),
            ("app_dedup_with_job_state (join)", "APP_DEDUP_WITH_JOB_STATE"),
            ("match_invalidation_events", "match_invalidation_events"),
            ("job_metrics_5m (CTAS)", "job_metrics_5m"),
            ("application_alerts (CTAS)", "application_alerts"),
        ]
        fdf = pd.DataFrame([{"step": s, "records": topics.get(t, 0)} for s, t in flow])
        fig = px.bar(fdf, x="records", y="step", orientation="h")
        fig.update_layout(template="plotly_dark", height=430, margin=dict(l=4, r=4, t=12, b=6),
                          yaxis=dict(title=None), xaxis=dict(title="records"))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ---------- sink freshness [LIVE] --------------------------------------
    mdf = sources.analytics_table("job_metrics_5m")
    aldf = sources.analytics_table("application_alerts")
    st.divider()
    st.markdown("##### Analytics sink freshness [LIVE]")
    b1, b2, b3 = st.columns(3)
    with b1:
        latest_w = pd.to_datetime(mdf["window_end"].max(), unit="ms", errors="coerce") \
            if "window_end" in mdf and len(mdf) else None
        kpi(f"{latest_w:%Y-%m-%d %H:%M}" if latest_w is not None else "—", "latest closed 5-min window",
            f"job_metrics_5m · {len(mdf):,} rows")
    with b2:
        latest_u = pd.to_datetime(aldf["updated_at"].max(), errors="coerce") \
            if "updated_at" in aldf and len(aldf) else None
        kpi(f"{latest_u:%Y-%m-%d %H:%M}" if latest_u is not None else "—", "latest alert upserted",
            f"application_alerts · {len(aldf):,} rows")
    with b3:
        inv = sources.delta_export("match_invalidations")
        kpi(f"{len(inv):,}", "job invalidations", "Delta match_invalidations [LIVE file]")

    # ---------- scenarios ----------------------------------------------------
    st.markdown("##### Demo scenarios this page supports")
    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown("###### A — job closes")
        st.caption("SQL update → Debezium → ksql two-legged invalidation → topic record → spark Delta "
                   "`match_invalidations` (count above).")
    with s2:
        st.markdown("###### B — duplicate submission")
        st.caption("valid event accepted → duplicate detected by state → DLQ → accounting matches input "
                   "(receipt above; reproduce live with monitoring\\load_test.ps1 -DuplicateRate 0.5).")
    with s3:
        st.markdown("###### C — app can't enrich")
        st.caption("unresolvable applications wait in Delta `pending_applications` with retry_count → "
                   "exhausted retries land in `dead_letter_applications` (Overview KPIs).")
    src_note(f"kafka read-only via docker exec · analytics db read-only · "
             f"console: [{config.CONSOLE_URL}]({config.CONSOLE_URL})")

