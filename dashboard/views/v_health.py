"""Page: Testing & System Health."""
import pandas as pd
import streamlit as st

from dashboard import config, sources
from dashboard.components import apply_theme, show_badge, badge, empty_state, kpi, src_note


def render():
    apply_theme()
    st.subheader("Testing & System Health")
    st.caption("[SAVED RUN] = recorded acceptance-test receipts (they do not describe the current "
               "service state on purpose). [LIVE] = probed right now.")

    # live health ----------------------------------------------------------
    st.markdown("##### Docker services [LIVE]")
    svcs = sources.docker_services()
    if not svcs:
        show_badge(badge("err", "docker unreachable"))
    else:
        ok = sum(1 for s in svcs if s["health"] == "healthy" or s["state"] == "running")
        st.dataframe(pd.DataFrame(svcs), use_container_width=True, height=230)
        show_badge(badge("ok" if ok == len(svcs) else "warn", f"{ok}/{len(svcs)} running/healthy"))

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("##### Kafka Connect connectors [LIVE]")
        conns = sources.connectors_status()
        if not conns:
            empty_state("Kafka Connect REST unreachable", "docker compose up -d, then refresh")
        else:
            run = sum(1 for x in conns if x["state"] == "RUNNING")
            show_badge(badge("ok" if run == len(conns) else "err", f"{run}/{len(conns)} RUNNING"))
            st.dataframe(pd.DataFrame(conns), use_container_width=True)
    with c2:
        st.markdown("##### ksqlDB persistent queries [LIVE]")
        qs = sources.ksql_queries()
        if not qs:
            empty_state("ksqlDB REST unreachable", "docker compose up -d, then refresh")
        else:
            runq = sum(1 for q in qs if q["state"] == "RUNNING")
            show_badge(badge("ok" if runq == len(qs) else "err", f"{runq}/{len(qs)} RUNNING"))
            st.dataframe(pd.DataFrame(qs), use_container_width=True, height=240)

    st.divider()
    st.markdown("##### Acceptance suites [SAVED RUN]")
    recs = sources.acceptance_receipts()
    if not recs:
        empty_state("no suite receipts found",
                    "run tests\\run_all.ps1 — it writes tests/<suite>/results/last_run.txt")
    else:
        rdf = pd.DataFrame(recs)
        passed = sum(1 for r in recs if r["verdict"] == "PASS")
        a, b, c = st.columns(3)
        with a:
            kpi(f"{passed}/{len(recs)}", "suites PASS", "recorded run")
        with b:
            kpi(rdf["timestamp"].max(), "receipt timestamp", "newest of all suites")
        with c:
            kpi("6", "suites defined", "01 health · 02 dedup · 03 invalid · 04 windows · 05 sink · 06 latency")
        st.dataframe(rdf, use_container_width=True, height=250)

        r02 = next((r for r in recs if r["suite"] == "02_dedup_dlq_integrity"), None)
        r06 = next((r for r in recs if r["suite"] == "06_latency_freshness"), None)
        ca, cb = st.columns(2)
        with ca:
            if r02:
                st.success(f"**Zero-loss accounting · {r02['verdict']} @ {r02['timestamp']}**\n\n{r02['detail']}")
        with cb:
            if r06:
                st.success(f"**End-to-end SLA · {r06['verdict']} @ {r06['timestamp']}**\n\n{r06['detail']}\n\n"
                           "SLA gate: p95 ≤ 10 000 ms")

    src_note("live: docker ps + connect/ksql REST + read-only kafka probes · saved: tests/*/results/last_run.txt")

