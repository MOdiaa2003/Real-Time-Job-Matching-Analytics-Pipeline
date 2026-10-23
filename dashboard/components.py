"""UI building blocks: KPI cards, statuses, shared plotly styling."""
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

CSS = """<style>
div.kpi {border:1px solid #444; border-radius:10px; padding:12px 14px; margin:2px; background:#1d1d24;}
div.kpi .v {font-size:1.28rem; font-weight:600; color:#fff; line-height:1.25;}
div.kpi .l {font-size:0.78rem; color:#a0a0a8; margin-top:3px;}
div.kpi .s {font-size:0.7rem; color:#7a7a82; margin-top:1px;}
.badge {display:inline-block; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:600;}
.ok {background:#123c20; color:#7de08e;} .warn {background:#3d2f10; color:#f2c164;}
.err {background:#3c1414; color:#ef8a8a;} .off {background:#2a2a31; color:#9a9aa2;}
</style>"""

PRIORITY_COLORS = {"HIGH_PRIORITY": "#E57A7A", "MEDIUM_PRIORITY": "#F2B65A",
                   "LOW_PRIORITY": "#6BAFEF"}


def apply_theme():
    st.markdown(CSS, unsafe_allow_html=True)


def kpi(value, label, sub=""):
    st.markdown(f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div>'
                f'<div class="s">{sub}</div></div>', unsafe_allow_html=True)


def show_badge(html: str):
    """Render a badge() string with HTML enabled."""
    st.markdown(html, unsafe_allow_html=True)


def badge(kind: str, text: str):
    return f'<span class="badge {kind}">{text}</span>'


def src_note(text: str):
    st.caption(f"Data source: {text} · refreshed {datetime.now().strftime('%H:%M:%S')}")


def empty_state(msg: str, how: str):
    st.info(f"**Nothing to show — {msg}**\n\nHow to make it appear: {how}")


def priority_donut(counts: pd.Series):
    fig = px.pie(names=counts.index, values=counts.values, hole=0.55,
                 color=counts.index, color_discrete_map=PRIORITY_COLORS)
    fig.update_traces(textinfo="percent", textfont_size=11,
                      hovertemplate="%{label}<br>%{value} matches<extra></extra>")
    fig.update_layout(template="plotly_dark", height=300,
                      margin=dict(l=4, r=4, t=8, b=8),
                      legend=dict(orientation="h", y=-0.12, title=""))
    fig.update_layout(legend_font=dict(size=10))
    return fig


def bar(x, y, xlabel="", ylabel="", horizontal=False, height=320, title=""):
    fig = go.Figure()
    fig.add_bar(x=x, y=y, orientation="h" if horizontal else "v", marker_color="#6BAFEF")
    fig.update_layout(template="plotly_dark", height=height, title=title,
                      margin=dict(l=8, r=8, t=34, b=8),
                      xaxis_title=xlabel, yaxis_title=ylabel)
    return fig
