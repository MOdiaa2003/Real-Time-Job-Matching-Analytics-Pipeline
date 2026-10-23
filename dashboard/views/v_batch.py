"""Page: Batch Medallion & Data Quality."""
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard import config, sources
from dashboard.components import apply_theme, show_badge, badge, kpi, src_note


def _lake_root_present() -> bool:
    lake = os.getenv("LAKE_ROOT")
    return bool(lake and Path(lake).exists())


def render():
    apply_theme()
    st.subheader("Batch Medallion & Data Quality")
    st.caption("The BATCH path — separate from real-time streaming: bounded Kafka reads → "
               "**Bronze** raw envelopes → **Silver** deduped/validated → **Gold** SCD2 dims + facts.")

    st.markdown("##### Warehouse status [LIVE detection]")
    if _lake_root_present():
        show_badge(badge("ok", "LAKE_ROOT present — rendering real layers"))
        _render_layers(os.environ["LAKE_ROOT"])
    else:
        show_badge(badge("off", "batch outputs not on this machine"))
        st.info("**The medallion code ships in this repo; its Delta warehouse outputs were produced on the "
                "development machine's `LAKE_ROOT` (default `/home/claude/lake`) and are not here.**\n\n"
                "To generate locally:\n```\n$env:LAKE_ROOT=\"C:\\data\\lake\"\npython spark_pipeline\\batch_processing.py\n"
                "```\nThen refresh this page — it will detect the outputs and render them. Nothing here is fabricated.")

    # ---- layer responsibilities (real modules) -------------------------
    st.divider()
    st.markdown("##### What each layer does — mapped to shipped code")
    mods = pd.DataFrame([
        {"layer": "Bronze", "what": "raw Debezium envelopes (append-only, offset bookmarks)",
         "code": "spark_pipeline/bronze/bronze_ingest.py"},
        {"layer": "Silver", "what": "flatten + dedupe + CDC version-gap check + normalize/validate → rejects quarantined",
         "code": "spark_pipeline/silver/*.py"},
        {"layer": "Gold", "what": "dim_candidate/dim_job (SCD Type 2), dim_city, fact_application history",
         "code": "spark_pipeline/gold/dim_candidate.py …"},
        {"layer": "Orchestration", "what": "end-to-end run book: ingest → silver → gold",
         "code": "spark_pipeline/run_batch_pipeline.py"},
    ])
    st.dataframe(mods, use_container_width=True)

    # ---- data model diagram (text, honest) ------------------------------
    st.markdown("##### Data model (`spark_pipeline/DATA_MODEL.md`)")
    st.caption("Kafka (candidate/job/application CDC) → bounded batch read → **BRONZE** verbatim envelopes → "
               "**SILVER** one clean row per (entity_id, version, op); rejects → quarantine → "
               "**GOLD** SCD2 candidate/job dims, city dim, application fact. Event history vs current state: "
               "Silver holds the *lifetime of versions*; Gold dims expose `is_current` rows plus history rows.")

    # ---- SCD2 ------------------------------------------------------------
    st.markdown("##### SCD Type 2 (why history exists)")
    st.caption("A candidate's salary or skill change doesn't overwrite the past: `effective_at`/`end_at`/`is_current` "
               "keep every version addressable — the analytics layer can answer 'who did what, when' reliably.")

    src_note("code + DATA_MODEL.md + batch_metrics.jsonl on repo files; warehouse auto-detected via LAKE_ROOT env")


def _render_layers(root: str):
    # Save-if-later implementation: enumerate <root>/bronze|silver|gold and count parquet rows
    n = Path(root)
    for layer in ("bronze", "silver", "gold", "quarantine"):
        d = n / layer
        if d.exists():
            size_mb = sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) / 1e6
            kpi(f"{size_mb:.1f} MB", f"{layer} on disk", str(d))
    st.caption("detailed row counts require a Delta reader — parity with exports flow when generated")

