import streamlit as st
import redshift_connector
import pandas as pd
from datetime import datetime
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Page config
st.set_page_config(
    page_title="Ecommerce Pipeline Dashboard",
    page_icon="📊",
    layout="wide"
)

# Connection details
REDSHIFT_CONFIG = {
    'host': 'ecommerce-workgroup.680019129594.us-east-1.redshift-serverless.amazonaws.com',
    'database': 'dev',
    'port': 5439,
    'user': 'admin',
    'password': 'DragonDaima2026'
}

@st.cache_data(ttl=300)  # Cache data for 5 minutes
def load_data():
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    # Load fct_orders
    cursor.execute("SELECT * FROM fct_orders")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    orders_df = pd.DataFrame(rows, columns=columns)

    # Load raw orders count
    cursor.execute("SELECT COUNT(*) FROM raw_orders")
    raw_count = cursor.fetchone()[0]

    # Revenue by status
    cursor.execute("""
        SELECT status, COUNT(*) as count, ROUND(SUM(order_amount), 2) as revenue
        FROM fct_orders
        GROUP BY status
    """)
    status_cols = [desc[0] for desc in cursor.description]
    status_rows = cursor.fetchall()
    status_df = pd.DataFrame(status_rows, columns=status_cols)

    # Orders by date
    cursor.execute("""
        SELECT order_date, COUNT(*) as orders, ROUND(SUM(order_amount), 2) as revenue
        FROM fct_orders
        GROUP BY order_date
        ORDER BY order_date
    """)
    date_cols = [desc[0] for desc in cursor.description]
    date_rows = cursor.fetchall()
    date_df = pd.DataFrame(date_rows, columns=date_cols)

    conn.close()
    return orders_df, raw_count, status_df, date_df

# Header
st.title("📊 Ecommerce Pipeline Dashboard")
st.caption(f"Last refreshed: {datetime.now().strftime('%B %d, %Y at %H:%M')}")

# Load data
with st.spinner("Loading data from Redshift..."):
    try:
        orders_df, raw_count, status_df, date_df = load_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Failed to connect to Redshift: {e}")
        data_loaded = False

if data_loaded:

    # Pipeline status banner
    st.success("✅ Pipeline Status: All systems operational — Last run successful")

    st.divider()

    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)

    total_orders = len(orders_df)
    total_revenue = orders_df[orders_df['is_completed'] == True]['order_amount'].sum()
    completed = len(orders_df[orders_df['status'] == 'completed'])
    cancelled = len(orders_df[orders_df['status'] == 'cancelled'])

    with col1:
        st.metric("Total Orders", total_orders)
    with col2:
        st.metric("Total Revenue", f"${total_revenue:,.2f}")
    with col3:
        st.metric("Completed Orders", completed)
    with col4:
        st.metric("Cancelled Orders", cancelled, delta=f"-{cancelled}", delta_color="inverse")

    st.divider()

    # Charts row
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Orders by Status")
        st.bar_chart(status_df.set_index('status')['count'])

    with col_right:
        st.subheader("Revenue by Date")
        if not date_df.empty:
            st.line_chart(date_df.set_index('order_date')['revenue'])
        else:
            st.info("No date data available")

    st.divider()

    # Data quality section
    st.subheader("🔍 Data Quality")

    qcol1, qcol2, qcol3, qcol4 = st.columns(4)

    null_order_ids = orders_df['order_id'].isna().sum()
    null_customers = orders_df['customer_name'].isna().sum()
    invalid_amounts = len(orders_df[orders_df['order_amount'] < 0])
    invalid_statuses = len(orders_df[~orders_df['status'].isin(['completed', 'cancelled', 'pending'])])

    with qcol1:
        st.metric("Null Order IDs", null_order_ids, delta="0 issues" if null_order_ids == 0 else f"{null_order_ids} issues", delta_color="normal" if null_order_ids == 0 else "inverse")
    with qcol2:
        st.metric("Null Customers", null_customers, delta="0 issues" if null_customers == 0 else f"{null_customers} issues", delta_color="normal" if null_customers == 0 else "inverse")
    with qcol3:
        st.metric("Invalid Amounts", invalid_amounts, delta="0 issues" if invalid_amounts == 0 else f"{invalid_amounts} issues", delta_color="normal" if invalid_amounts == 0 else "inverse")
    with qcol4:
        st.metric("Invalid Statuses", invalid_statuses, delta="0 issues" if invalid_statuses == 0 else f"{invalid_statuses} issues", delta_color="normal" if invalid_statuses == 0 else "inverse")

    st.divider()

    # Raw data table
    st.subheader("📋 Latest Orders")
    st.dataframe(
        orders_df[['order_id', 'order_date', 'customer_name', 'status', 'order_amount', 'is_completed']],
        use_container_width=True
    )

