"""
dashboard/app.py
Job Market Trends — Public Streamlit Dashboard
"""

import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import psycopg2

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Job Market Trends",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }
    .block-container { padding-top: 2rem; }

    .metric-card {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-radius: 12px;
        padding: 1.25rem 1.5rem;
        text-align: center;
    }
    .metric-label {
        font-size: 0.75rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #64748b;
        margin-bottom: 0.25rem;
    }
    .metric-value {
        font-family: 'DM Mono', monospace;
        font-size: 2rem;
        font-weight: 500;
        color: #f1f5f9;
    }
    .api-box {
        background: #0f172a;
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 1.25rem 1.5rem;
        font-family: 'DM Mono', monospace;
        font-size: 0.85rem;
        color: #94a3b8;
        margin-bottom: 0.75rem;
    }
    .pill {
        display: inline-block;
        background: #1e293b;
        color: #7dd3fc;
        border-radius: 20px;
        padding: 2px 12px;
        font-size: 0.75rem;
        font-family: 'DM Mono', monospace;
        margin: 2px;
    }
</style>
""", unsafe_allow_html=True)


# ── DB ────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@st.cache_data(ttl=3600)
def load_trending_skills(role_filter, weeks):
    conn = get_conn()
    if role_filter and role_filter != "All roles":
        df = pd.read_sql("""
            SELECT skill, SUM(count) AS total
            FROM skills
            WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
              AND role ILIKE %s
            GROUP BY skill ORDER BY total DESC LIMIT 20
        """, conn, params=(weeks, f"%{role_filter}%"))
    else:
        df = pd.read_sql("""
            SELECT skill, SUM(count) AS total
            FROM skills
            WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
            GROUP BY skill ORDER BY total DESC LIMIT 20
        """, conn, params=(weeks,))
    return df


@st.cache_data(ttl=3600)
def load_weekly_trend(skill, weeks):
    conn = get_conn()
    return pd.read_sql("""
        SELECT week, SUM(count) AS total
        FROM skills
        WHERE skill ILIKE %s
          AND day >= CURRENT_DATE - (%s * INTERVAL '1 day')
        GROUP BY day ORDER BY day ASC
    """, conn, params=(f"%{skill}%", weeks))


@st.cache_data(ttl=3600)
def load_skills_by_role(weeks):
    conn = get_conn()
    return pd.read_sql("""
        SELECT role, skill, SUM(count) AS total
        FROM skills
        WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
        GROUP BY role, skill
        ORDER BY role, total DESC
    """, conn, params=(weeks,))


@st.cache_data(ttl=3600)
def load_summary_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM jobs")
    jobs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM api_keys WHERE is_active = true")
    keys = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM api_usage_logs")
    reqs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT skill) FROM skills")
    skills = cur.fetchone()[0]
    cur.close()
    return jobs, keys, reqs, skills


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📊 Job Market Trends")
    st.markdown("Live data from remote job listings, updated weekly.")
    st.divider()

    role_options = ["All roles", "Data Engineer", "ML Engineer", "Data Scientist",
                    "Data Analyst", "Backend Engineer", "Frontend Engineer", "DevOps"]
    role_filter = st.selectbox("Role", role_options)
    weeks = st.slider("Lookback (days)", 7, 180, 30)

    st.divider()
    st.markdown("### Get API access")
    st.markdown("Query this data from your own code.")

    with st.form("register"):
        email_input = st.text_input("Email", placeholder="you@example.com")
        name_input  = st.text_input("Name (optional)")
        submitted   = st.form_submit_button("Get free API key →")
        if submitted and email_input:
            import requests as req
            api_base = os.environ.get("API_BASE_URL", "http://localhost:8000")
            try:
                r = req.post(f"{api_base}/v1/keys/register",
                             json={"email": email_input, "name": name_input}, timeout=10)
                if r.ok:
                    key = r.json()["api_key"]
                    st.success("Key generated!")
                    st.code(key, language=None)
                    st.caption("Save this — shown only once.")
                else:
                    st.error("Something went wrong. Try again.")
            except Exception:
                st.error("API unavailable. Try again later.")

    st.divider()
    st.caption("Data from RemoteOK. Updated every day at 02:00 UTC.")


# ── Main ──────────────────────────────────────────────────────────────────────

st.markdown("# Job Market Trends")
st.markdown(f"Tracking in-demand skills across remote tech jobs — last **{weeks} days**.")

# Summary stats
try:
    jobs_count, keys_count, reqs_count, skills_count = load_summary_stats()
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Jobs indexed", f"{jobs_count:,}")
    with c2:
        st.metric("Skills tracked", skills_count)
    with c3:
        st.metric("API users", keys_count)
    with c4:
        st.metric("API requests", f"{reqs_count:,}")
except Exception:
    st.info("Connect a database to see live stats.")

st.divider()

# ── Top skills bar chart ──────────────────────────────────────────────────────

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Top skills" + (f" — {role_filter}" if role_filter != 'All roles' else ""))
    try:
        df_skills = load_trending_skills(role_filter, weeks)
        if not df_skills.empty:
            fig = px.bar(
                df_skills.sort_values("total"),
                x="total", y="skill",
                orientation="h",
                color="total",
                color_continuous_scale=["#1e3a5f", "#0ea5e9"],
                labels={"total": "Mentions", "skill": ""},
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=0, b=0),
                height=420,
                font=dict(family="DM Sans"),
                xaxis=dict(gridcolor="#1e293b"),
                yaxis=dict(gridcolor="rgba(0,0,0,0)"),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet — run the ETL pipeline first.")
    except Exception as e:
        st.error(f"DB error: {e}")

with col2:
    st.subheader("Skill trend")
    try:
        df_skills2 = load_trending_skills(role_filter, weeks)
        if not df_skills2.empty:
            skill_pick = st.selectbox("Track skill", df_skills2["skill"].tolist(), label_visibility="collapsed")
            df_trend = load_weekly_trend(skill_pick, weeks)
            if not df_trend.empty:
                fig2 = go.Figure(go.Scatter(
                    x=df_trend["week"], y=df_trend["total"],
                    mode="lines+markers",
                    line=dict(color="#0ea5e9", width=2),
                    marker=dict(size=6, color="#0ea5e9"),
                    fill="tozeroy",
                    fillcolor="rgba(14,165,233,0.1)",
                ))
                fig2.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=0, r=0, t=0, b=0),
                    height=380,
                    font=dict(family="DM Sans"),
                    xaxis=dict(gridcolor="#1e293b"),
                    yaxis=dict(gridcolor="#1e293b", title="mentions/week"),
                )
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No data yet.")
    except Exception as e:
        st.error(f"DB error: {e}")

st.divider()

# ── Skills by role heatmap ────────────────────────────────────────────────────

st.subheader("Skills by role — top 8 each")
try:
    df_by_role = load_skills_by_role(weeks)
    if not df_by_role.empty:
        top_skills_per_role = (
            df_by_role.groupby("role")
            .apply(lambda g: g.nlargest(8, "total"))
            .reset_index(drop=True)
        )
        pivot = top_skills_per_role.pivot_table(index="skill", columns="role", values="total", fill_value=0)
        fig3 = px.imshow(
            pivot,
            color_continuous_scale=["#0f172a", "#0369a1", "#38bdf8"],
            aspect="auto",
            labels=dict(color="mentions"),
        )
        fig3.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=0, b=0),
            height=400,
            font=dict(family="DM Sans"),
            coloraxis_showscale=True,
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No data yet.")
except Exception as e:
    st.error(f"DB error: {e}")

st.divider()

# ── API docs preview ──────────────────────────────────────────────────────────

st.subheader("API endpoints")
api_base = os.environ.get("API_BASE_URL", "https://your-api.onrender.com")

endpoints = [
    ("GET", "/v1/skills/trending", "?role=data+engineer&weeks=8"),
    ("GET", "/v1/skills/by-role",  "?weeks=4"),
    ("GET", "/v1/trends/weekly",   "?skill=python&weeks=12"),
    ("GET", "/v1/jobs/recent",     "?role=backend&skill=rust&limit=20"),
    ("GET", "/v1/jobs/count",      "?skill=kubernetes&days=30"),
]

cols = st.columns(len(endpoints))
for col, (method, path, params) in zip(cols, endpoints):
    with col:
        st.markdown(f"""
        <div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:0.75rem;font-family:'DM Mono',monospace;font-size:0.7rem;">
            <div style="color:#34d399;margin-bottom:4px">{method}</div>
            <div style="color:#f1f5f9">{path}</div>
            <div style="color:#64748b;margin-top:4px">{params}</div>
        </div>
        """, unsafe_allow_html=True)

st.caption(f"Full docs at [{api_base}/docs]({api_base}/docs)")
