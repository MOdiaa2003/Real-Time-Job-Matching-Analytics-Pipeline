"""Page: Candidate–Job Matching Explorer (business-facing hero page)."""
import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import config, sources
from dashboard.components import (apply_theme, empty_state, kpi, priority_donut,
                                  src_note)
from dashboard.config import PRIORITY_ORDER


def render():
    apply_theme()
    st.subheader("Candidate–Job Matching Explorer")
    st.caption("Persisted match results from the Spark engine's Delta `match_results` (via exports snapshot). "
               "Weights: skills 30 % · experience 25 % · location 20 % · salary 15 % · language 10 %; "
               "priority: high ≥ 80, medium ≥ 65, low < 65.")

    mr = sources.delta_export("match_results")
    jd = sources.delta_export("job_dim")

    if mr.empty:
        empty_state("no match_results export yet",
                    "run `python scripts\\export_delta_for_bi.py` from the repo root "
                    "(sidebar button does this too)")
        return

    # ------------- filters -----------------------------------------------
    with st.container(border=True):
        f1, f2, f3, f4 = st.columns([2.2, 2.0, 1.6, 3.0])
        # default job = the one with the MOST matches (always interesting)
        match_counts = mr.groupby("post_id").size()
        if not jd.empty:
            jinfo = jd[["post_id", "job_title", "company"]].dropna().drop_duplicates("post_id")
            jinfo = jinfo.set_index("post_id")
            options = [p for p in match_counts.sort_values(ascending=False).index
                       if p in jinfo.index]
        else:
            jinfo = pd.DataFrame()
            options = list(match_counts.sort_values(ascending=False).index)
        with f1:
            def label(pid):
                if not jinfo.empty and pid in jinfo.index:
                    r = jinfo.loc[pid]
                    return f"{str(r['job_title'])[:38]} — {str(r['company'])[:20]}"
                return pid[:30]
            sel_job = st.selectbox("Job (ordered by match count)", options, index=0,
                                   format_func=label, help="ranked candidates for this job")
        with f2:
            prio = st.pills("Priority", PRIORITY_ORDER, selection_mode="multi",
                            default=PRIORITY_ORDER,
                            format_func=lambda p: {"HIGH_PRIORITY": "High",
                                                   "MEDIUM_PRIORITY": "Medium",
                                                   "LOW_PRIORITY": "Low"}.get(p, p))
        with f3:
            score_range = st.slider("Overall score range", 0, 100, (0, 100))
        with f4:
            q = st.text_input("Search candidate name / job title", "").lower()

    overall_only = mr[mr["overall_score"].between(*score_range) & mr["priority"].isin(prio)]
    if q:
        mask = (mr["cand_name"].fillna("").str.lower().str.contains(q)
                | mr["job_job_title"].fillna("").str.lower().str.contains(q))
        overall_only = mr[mask]
    if sel_job:
        filtered = overall_only[overall_only["post_id"] == sel_job]
    else:
        filtered = overall_only

    # ------------- KPIs ----------------------------------------------------
    a, b, c = st.columns(3)
    with a:
        kpi(f"{len(mr):,}", "persisted match records", "Delta match_results [LIVE]")
    with b:
        kpi(f"{mr['application_id'].nunique():,}", "distinct applications", "each scored once (dedup upstream)")
    with c:
        kpi(f"{int((mr['priority'] == 'HIGH_PRIORITY').sum()):,}", "high-priority matches", "score ≥ 80")
    a2, b2 = st.columns(2)
    with a2:
        avg = filtered["overall_score"].mean() if not filtered.empty else None
        kpi(f"{avg:.1f}" if avg is not None else "—", "avg score (selection)",
            "empty selection shows —" if avg is None else "weighted mean of current filters")
    with b2:
        rd = sources.exports_freshness().get("match_results", "n/a")
        kpi(rd, "exports snapshot", "freshness of the CSV the page reads")

    # ------------- ranked bars + components ---------------------------------
    left, right = st.columns([2.6, 2.4])
    with left:
        sel = mr[mr["post_id"] == sel_job].sort_values("overall_score", ascending=False).head(15) \
            if sel_job else pd.DataFrame()
        if not sel.empty:
            sel = sel.drop_duplicates("candidate_id")
            st.markdown(f"##### Ranked candidates ({len(sel)}, best score per candidate "
                        f"of {int(match_counts.get(sel_job, 0)):,} match records)")
            names = [f"{str(n)[:24]} · {str(cid)[:6]}" for n, cid in zip(sel["cand_name"], sel["candidate_id"])]
            fig = px.bar(x=sel["overall_score"], y=names, orientation="h",
                         color=sel["priority"],
                         color_discrete_map={"HIGH_PRIORITY": "#E57A7A",
                                             "MEDIUM_PRIORITY": "#F2B65A",
                                             "LOW_PRIORITY": "#6BAFEF"})
            fig.update_layout(template="plotly_dark", height=470, margin=dict(l=8, r=8, t=30, b=8),
                              yaxis_title="candidate", xaxis_title="overall score (0–100)",
                              xaxis_range=[0, 100], legend_title="priority")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No matches recorded for this job yet — it may not have applications scored so far. "
                    "Search by candidate name to explore.") if sel_job else None

    with right:
        st.markdown("##### Where do the scores come from?")
        comp = mr[["skill_score", "experience_score", "salary_score",
                   "location_score", "language_score"]].mean()
        cdf = pd.DataFrame({"component": ["skills (30 %)", "experience (25 %)", "salary (15 %)",
                                          "location (20 %)", "language (10 %)"],
                            "avg": comp.values})
        fig = px.bar(cdf, x="component", y="avg")
        fig.update_layout(template="plotly_dark", height=250, margin=dict(l=8, r=8, t=24, b=28),
                          yaxis_title="avg score")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        kpi(f"{mr['event_ts'].max()[:10] if 'event_ts' in mr else 'n/a'}",
            "latest scored event date", "candidate/job data freshness")
        vc = mr["priority"].value_counts().reindex(PRIORITY_ORDER).fillna(0)
        st.plotly_chart(priority_donut(vc), use_container_width=True,
                        config={"displayModeBar": False})

    st.divider()
    show = st.checkbox("Show full results table (filtered)", value=True)
    if show:
        cols = [c for c in ["overall_score", "priority", "cand_name", "job_job_title", "company",
                            "city", "country", "skill_score", "experience_score", "location_score",
                            "salary_score", "language_score", "scored_at"] if c in filtered.columns]
        view = filtered.sort_values("overall_score", ascending=False)[cols]
        st.dataframe(view, use_container_width=True, height=420)
        st.download_button("Download filtered (CSV)",
                           view.to_csv(index=False).encode(), "matching_results.csv", "text/csv")
    src_note("Delta match_results + job_dim/candidate_dim via exports snapshots (cached 120 s)")

