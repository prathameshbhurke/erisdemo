import streamlit as st
import redshift_connector
import pandas as pd
from datetime import datetime
import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

st.set_page_config(
    page_title="Eris Solutions — Pipeline Health",
    page_icon="🔧",
    layout="wide"
)

REDSHIFT_CONFIG = {
    'host': os.environ.get("REDSHIFT_HOST", "ecommerce-workgroup.680019129594.us-east-1.redshift-serverless.amazonaws.com"),
    'database': os.environ.get("REDSHIFT_DB", "dev"),
    'port': int(os.environ.get("REDSHIFT_PORT", 5439)),
    'user': os.environ.get("REDSHIFT_USER", "admin"),
    'password': os.environ.get("REDSHIFT_PASSWORD", "")
}

@st.cache_data(ttl=60)
def load_health_data():
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    # Row counts for all tables
    tables = [
        "raw_olist_orders", "raw_olist_customers", "raw_olist_order_items",
        "raw_olist_order_payments", "raw_olist_products", "raw_olist_sellers",
        "raw_olist_category_translation", "stg_olist_orders", "stg_olist_customers",
        "stg_olist_order_items", "stg_olist_order_payments", "stg_olist_products",
        "stg_olist_sellers", "fct_olist_orders", "dim_olist_customers",
        "dim_olist_products", "dim_olist_sellers", "dim_olist_payments",
        "rpt_olist_orders"
    ]

    table_counts = []
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            layer = "Raw" if table.startswith("raw") else \
                    "Staging" if table.startswith("stg") else \
                    "Dimension" if table.startswith("dim") else \
                    "Fact" if table.startswith("fct") else "Reporting"
            table_counts.append({
                "table": table,
                "layer": layer,
                "row_count": count,
                "status": "✅" if count > 0 else "❌"
            })
        except:
            table_counts.append({
                "table": table,
                "layer": "Unknown",
                "row_count": 0,
                "status": "❌"
            })

    counts_df = pd.DataFrame(table_counts)

    # Data freshness
    cursor.execute("SELECT MAX(ordered_at) FROM rpt_olist_orders")
    latest_order = cursor.fetchone()[0]

    # Null checks on rpt_olist_orders
    cursor.execute("""
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
            SUM(CASE WHEN ordered_at IS NULL THEN 1 ELSE 0 END) as null_dates,
            SUM(CASE WHEN total_order_value < 0 THEN 1 ELSE 0 END) as negative_values
        FROM rpt_olist_orders
    """)
    cols = [d[0] for d in cursor.description]
    quality_row = dict(zip(cols, cursor.fetchone()))

    # Agent activity log
    cursor.execute("""
        SELECT agent_name, run_time, status, summary, slack_channel
        FROM agent_activity_log
        ORDER BY run_time DESC
        LIMIT 20
    """)
    agent_cols = [d[0] for d in cursor.description]
    agent_rows = cursor.fetchall()
    agent_df = pd.DataFrame(agent_rows, columns=agent_cols)

    # GX checks summary
    gx_checks = [
        ("Row count 90K-250K", "rpt_olist_orders", quality_row['total_rows'] >= 90000),
        ("order_id not null", "rpt_olist_orders", quality_row['null_order_ids'] == 0),
        ("customer_id not null", "rpt_olist_orders", quality_row['null_customers'] == 0),
        ("ordered_at not null", "rpt_olist_orders", quality_row['null_dates'] == 0),
        ("No negative values", "rpt_olist_orders", quality_row['negative_values'] == 0),
    ]
    gx_df = pd.DataFrame(gx_checks, columns=["check", "table", "passed"])

    conn.close()
    return counts_df, latest_order, quality_row, agent_df, gx_df

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
    <style>
    .pipeline-header { 
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 20px 24px;
        border-radius: 12px;
        margin-bottom: 24px;
        color: white;
    }
    .health-score {
        font-size: 48px;
        font-weight: 700;
        color: #00FF88;
    }
    .layer-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown("""
    <div class="pipeline-header">
        <h2 style="margin:0;color:white;">🔧 Eris Solutions — Pipeline Health Console</h2>
        <p style="margin:4px 0 0 0;color:#aaa;font-size:13px;">Real-time monitoring · Powered by AI Agents</p>
    </div>
""", unsafe_allow_html=True)

st.caption(f"Last refreshed: {datetime.now().strftime('%B %d, %Y at %H:%M')} · Auto-refreshes every 60 seconds")

# Load data
with st.spinner("Loading pipeline health data..."):
    try:
        counts_df, latest_order, quality_row, agent_df, gx_df = load_health_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Failed to connect: {e}")
        data_loaded = False

if data_loaded:

    # ── Overall Health Score ───────────────────────────────────────────────────
    tables_healthy = len(counts_df[counts_df['row_count'] > 0])
    total_tables = len(counts_df)
    gx_passed = len(gx_df[gx_df['passed'] == True])
    gx_total = len(gx_df)
    health_score = int((tables_healthy / total_tables * 50) + (gx_passed / gx_total * 50))

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Health Score", f"{health_score}/100",
                  delta="Healthy" if health_score >= 80 else "Warning")
    with col2:
        st.metric("Tables Loaded", f"{tables_healthy}/{total_tables}")
    with col3:
        st.metric("GX Checks", f"{gx_passed}/{gx_total} passing")
    with col4:
        st.metric("Total Rows (rpt)", f"{quality_row['total_rows']:,}")
    with col5:
        st.metric("Latest Data", str(latest_order)[:10] if latest_order else "Unknown")

    st.divider()

    # ── Tabs ───────────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📊 Table Health", "✅ Quality Checks", "🤖 Agent Activity"])

    with tab1:
        st.subheader("Table Row Counts by Layer")

        for layer in ["Raw", "Staging", "Fact", "Dimension", "Reporting"]:
            layer_df = counts_df[counts_df['layer'] == layer]
            if not layer_df.empty:
                st.markdown(f"**{layer} Layer**")
                cols = st.columns(len(layer_df))
                for i, (_, row) in enumerate(layer_df.iterrows()):
                    with cols[i]:
                        st.metric(
                            row['table'].replace('_', ' ').title(),
                            f"{row['row_count']:,}",
                            delta=row['status']
                        )
                st.divider()

    with tab2:
        st.subheader("Data Quality Check Results")

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("**Great Expectations Checks**")
            for _, row in gx_df.iterrows():
                icon = "✅" if row['passed'] else "❌"
                color = "green" if row['passed'] else "red"
                st.markdown(f"{icon} {row['check']} — `{row['table']}`")

        with col_right:
            st.markdown("**Data Quality Metrics**")
            st.metric("Total Rows", f"{quality_row['total_rows']:,}")
            st.metric("Null Order IDs", quality_row['null_order_ids'],
                      delta="Clean" if quality_row['null_order_ids'] == 0 else "Issues found",
                      delta_color="normal" if quality_row['null_order_ids'] == 0 else "inverse")
            st.metric("Null Customers", quality_row['null_customers'],
                      delta="Clean" if quality_row['null_customers'] == 0 else "Issues found",
                      delta_color="normal" if quality_row['null_customers'] == 0 else "inverse")
            st.metric("Null Dates", quality_row['null_dates'],
                      delta="Clean" if quality_row['null_dates'] == 0 else "Issues found",
                      delta_color="normal" if quality_row['null_dates'] == 0 else "inverse")
            st.metric("Negative Values", quality_row['negative_values'],
                      delta="Clean" if quality_row['negative_values'] == 0 else "Issues found",
                      delta_color="normal" if quality_row['negative_values'] == 0 else "inverse")

    with tab3:
        st.subheader("AI Agent Activity Log")

        if not agent_df.empty:
            for _, row in agent_df.iterrows():
                status_color = "🟢" if row['status'] == 'success' else \
                               "🟡" if row['status'] == 'warning' else "🔴"
                with st.expander(
                    f"{status_color} {row['agent_name']} — {str(row['run_time'])[:16]} → {row['slack_channel']}"
                ):
                    st.write(row['summary'])
        else:
            st.info("No agent activity logged yet — run agents to populate this log")

        if st.button("🤖 Run All Agents Now"):
            with st.spinner("Running agents..."):
                import subprocess
                result = subprocess.run(
                    ["python3", "agents/agent_runner.py", "--mode", "all"],
                    cwd="/Users/prathameshbhurke/project_phoenix/eris-data-platform",
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    st.success("✅ All agents completed successfully!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Agent run failed: {result.stderr}")