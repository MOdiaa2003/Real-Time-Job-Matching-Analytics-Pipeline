"""Recruitment Pipeline Control Center — Streamlit entry point.

Run (repo root):
    streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from dashboard import config
from dashboard.views import (v_batch, v_health, v_matching, v_overview,
                             v_performance, v_streaming)

st.set_page_config(page_title="Recruitment Pipeline Control Center",
                   page_icon="🛰️", layout="wide",
                   initial_sidebar_state="expanded")

PAGES = {
    "Executive Overview": v_overview.render,
    "Candidate–Job Matching Explorer": v_matching.render,
    "Kafka Streaming & Reliability": v_streaming.render,
    "Batch Medallion & Quality": v_batch.render,
    "Engine Performance": v_performance.render,
    "Testing & Health": v_health.render,
}

with st.sidebar:
    st.markdown("## 🛰️ Control Center")
    st.caption(config.MIGRATION_NOTE)
    page = st.radio("Go to", list(PAGES), index=0)
    st.divider()
    st.markdown("**Exports freshness**")
    try:
        from dashboard import sources
        fr = sources.exports_freshness()
        if fr:
            st.caption("\n".join(f"`{k}`: {v}" for k, v in sorted(fr.items())))
    except Exception:
        pass
    if st.button("🔄 Re-snapshot Delta exports",
                 help="read-only Spark snapshot of streaming_lake → exports/ (~1–2 min)"):
        with st.spinner("running scripts/export_delta_for_bi.py …"):
            ok = sources.run_exporter()
        (st.success("exports refreshed") if ok else st.error("exporter failed — check terminal logs"))

PAGES[page]()
