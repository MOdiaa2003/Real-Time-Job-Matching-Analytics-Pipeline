"""Page: Engine Performance & Observability."""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard import sources
from dashboard.components import apply_theme, kpi, src_note


def render():
    apply_theme()
    st.subheader("Engine performance observability")
    st.caption("Every number comes from the engine's own instrumentation "
               "`streaming_lake/_metrics/batch_metrics.jsonl` — one JSON line per completed batch. "
               "Never estimated.")

    bm = sources.batch_metrics()
    if bm.empty:
        st.info("metrics file is empty/unreadable — the engine appends one line per batch while running "
                "(start with scripts\\run.ps1).")
        return

    n = min(len(bm), 20)
    n_last = st.slider("Show the last N batches", 3, len(bm), n)
    d = bm.tail(n_last)
    last = d.iloc[-1]

    a, b, c = st.columns(3)
    with a:
        kpi(f"{last['batch_duration_sec']:.0f} s", "latest batch duration", f"batch {int(last['batch_id'])}")
    with b:
        kpi(f"{last['input_rows_per_sec']:.0f}", "input rate (rows/s)", "last batch")
    with c:
        kpi(f"{int(last['high_priority_rows']):,}", "high-priority rows", "score ≥ 80 in batch")
    e2, f2 = st.columns(2)
    with e2:
        kpi(f"{int(last['unresolved_rows']):,}", "pending rows", "→ pending_applications Delta")
    with f2:
        kpi(f"{int(last['dead_letter_rows']):,}", "dead-letter rows", "exhausted retries")

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(d, x="batch_id", y="batch_duration_sec", markers=True)
        fig.update_layout(template="plotly_dark", height=300, title="Batch duration [LIVE]",
                          xaxis_title="batch id", yaxis_title="seconds")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=d["batch_id"], y=d["incoming_rows"], name="incoming",
                                 line=dict(color="#6BAFEF")))
        fig.add_trace(go.Scatter(x=d["batch_id"], y=d["resolved_rows"], name="resolved",
                                 line=dict(color="#8FD2A0")))
        fig.add_trace(go.Scatter(x=d["batch_id"], y=d["unresolved_rows"],
                                 name="unresolved → pending", line=dict(color="#F2B65A")))
        fig.add_trace(go.Scatter(x=d["batch_id"], y=d["dead_letter_rows"], name="dead-letter",
                                 line=dict(color="#E57A7A")))
        fig.update_layout(template="plotly_dark", height=300, title="Rows in → resolved → scored",
                          xaxis_title="batch id", yaxis_title="rows")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.divider()
    st.markdown("##### Dimension maintenance (Delta MERGE sizes) [LIVE]")
    dd = d[["batch_id", "candidate_dim_updated_rows", "job_dim_updated_rows"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=dd["batch_id"], y=dd["candidate_dim_updated_rows"],
                         name="candidate_dim merged", marker_color="#A78BE8"))
    fig.add_trace(go.Bar(x=dd["batch_id"], y=dd["job_dim_updated_rows"],
                         name="job_dim merged", marker_color="#6BD6C2"))
    fig.update_layout(barmode="group", template="plotly_dark", height=280,
                      title="Rows merged per batch", xaxis_title="batch id", yaxis_title="rows")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("##### Broadcast sizes — why the engine is fast [LIVE]")
    bb, note = st.columns([2.6, 1.4])
    with bb:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=d["batch_id"], y=d["candidate_dim_broadcast_mb"],
                                 name="candidate_dim broadcast MB"))
        fig.add_trace(go.Scatter(x=d["batch_id"], y=d["job_dim_broadcast_mb"],
                                 name="job_dim broadcast MB"))
        fig.update_layout(template="plotly_dark", height=260, title="Cached dims broadcast per batch",
                          xaxis_title="batch id", yaxis_title="MB")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with note:
        kpi(f"{d['candidate_dim_broadcast_mb'].mean():.2f} MB", "avg candidate-dim broadcast",
            "sub-GB broadcast joins keep every batch sub-2-min")

    st.markdown("##### Where the time goes — stage timings (latest batch) [LIVE]")
    stagecols = ["pending_write_sec", "dead_letter_write_sec", "resolved_check_sec",
                 "scoring_write_sec", "high_priority_check_sec"]
    if all(cc in d.columns for cc in stagecols):
        pie = px.pie(values=[last[c] for c in stagecols],
                     names=[c.replace("_sec", "").replace("_", " ") for c in stagecols], hole=0.42)
        pie.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(pie, use_container_width=True, config={"displayModeBar": False})
        st.caption(f"batch {int(last['batch_id'])}: scoring + Delta writes dominate; shuffle "
                   f"{d['shuffle_read_mb'].iloc[-1]:.0f} MB read / {d['shuffle_write_mb'].iloc[-1]:.0f} MB written; "
                   f"{int(d['files_read'].iloc[-1])} files read / {int(d['files_written'].iloc[-1])} written.")

    st.markdown("##### Runtime vs the documented baseline (README)")

    import pandas as pd
    band = pd.DataFrame({
        "period": ["this run — live [LIVE]", "README: steady batches 1–3", "README: batch 0 initial replay"],
        "sec": [d["batch_duration_sec"].mean(), 31.3, 85.2],
        "kind": ["live", "documented", "documented"]})
    fig = px.bar(band, x="period", y="sec", color="kind",
                 color_discrete_map={"live": "#8FD2A0", "documented": "#8B93A3"})
    fig.update_layout(template="plotly_dark", height=260,
                      title="Average batch time (s) — measured now vs 25 Aug logs",
                      yaxis_title="seconds")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.caption(f"Right bars are historical documentation values (README, 25 Aug 2026 logs), not measured "
               f"here — the live engine is averaging "
               f"{d['batch_duration_sec'].mean():.1f} s over the last {len(d)} batches.")

    src_note("streaming_lake/_metrics/batch_metrics.jsonl (cache 20 s) · Spark UI live at http://localhost:4040")

