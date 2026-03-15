from flask import Flask, jsonify, render_template
from flask_cors import CORS
import redshift_connector
import os
from dotenv import load_dotenv
from datetime import datetime
import threading
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

load_dotenv()

app = Flask(__name__)
CORS(app)   

REDSHIFT_CONFIG = {
    'host': os.getenv("REDSHIFT_HOST"),
    'database': os.getenv("REDSHIFT_DB"),
    'port': int(os.getenv("REDSHIFT_PORT", 5439)),
    'user': os.getenv("REDSHIFT_USER"),
    'password': os.getenv("REDSHIFT_PASSWORD")
}

def query(sql):
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]

def query_one(sql):
    result = query(sql)
    return result[0] if result else {}

@app.route('/api/prompts/<agent_name>')
def get_prompt(agent_name):
    prompt_path = os.path.join(
        os.path.dirname(__file__),
        f'../agents/prompts/{agent_name}_prompt.txt'
    )
    try:
        with open(prompt_path, 'r') as f:
            return jsonify({"prompt": f.read()})
    except:
        return jsonify({"prompt": "Prompt file not found"})

@app.route('/api/prompts/<agent_name>', methods=['POST'])
def save_prompt(agent_name):
    from flask import request
    prompt_path = os.path.join(
        os.path.dirname(__file__),
        f'../agents/prompts/{agent_name}_prompt.txt'
    )
    try:
        data = request.get_json()
        with open(prompt_path, 'w') as f:
            f.write(data['prompt'])
        return jsonify({"status": "saved"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

# ── Pipeline Summary ───────────────────────────────────────────────────────────
@app.route('/api/summary')
def summary():
    tables = [
        ("raw_olist_orders", "Ingestion"),
        ("raw_olist_customers", "Ingestion"),
        ("raw_olist_order_items", "Ingestion"),
        ("raw_olist_order_payments", "Ingestion"),
        ("raw_olist_products", "Ingestion"),
        ("raw_olist_sellers", "Ingestion"),
        ("raw_olist_category_translation", "Ingestion"),
        ("stg_olist_orders", "Transformation"),
        ("stg_olist_customers", "Transformation"),
        ("stg_olist_order_items", "Transformation"),
        ("stg_olist_order_payments", "Transformation"),
        ("stg_olist_products", "Transformation"),
        ("stg_olist_sellers", "Transformation"),
        ("fct_olist_orders", "Modeling"),
        ("dim_olist_customers", "Modeling"),
        ("dim_olist_products", "Modeling"),
        ("dim_olist_sellers", "Modeling"),
        ("dim_olist_payments", "Modeling"),
        ("rpt_olist_orders", "Analytics Layer"),
    ]

    counts = []
    for table, layer in tables:
        try:
            result = query_one(f"SELECT COUNT(*) as cnt FROM {table}")
            counts.append({
                "table": table,
                "layer": layer,
                "count": result.get("cnt", 0),
                "healthy": result.get("cnt", 0) > 0
            })
        except:
            counts.append({"table": table, "layer": layer, "count": 0, "healthy": False})

    # Run stats
    run_stats = query_one("""
        SELECT
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
            ROUND(AVG(CASE WHEN status = 'success' THEN duration_seconds END), 0) as avg_duration,
            MAX(rows_processed) as max_rows,
            ROUND(AVG(CASE WHEN status = 'success' THEN rows_per_second END), 0) as avg_throughput
        FROM pipeline_run_log
    """)

    # Latest run
    latest_run = query_one("""
        SELECT status, run_date, duration_seconds, rows_processed
        FROM pipeline_run_log
        ORDER BY run_date DESC
        LIMIT 1
    """)

    # Quality

    quality = query_one("""
        SELECT
            COUNT(order_id) as prod_rows,
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
            SUM(CASE WHEN total_order_value < 0 THEN 1 ELSE 0 END) as negative_values
        FROM rpt_olist_orders
    """)

    # Prod layer totals
    prod_totals = query_one("""
        SELECT
            (SELECT COUNT(order_id) FROM fct_olist_orders) +
            (SELECT COUNT(customer_unique_id) FROM dim_olist_customers) +
            (SELECT COUNT(product_id) FROM dim_olist_products) +
            (SELECT COUNT(seller_id) FROM dim_olist_sellers) +
            (SELECT COUNT(order_id) FROM rpt_olist_orders) as prod_total
    """)
    # Latest data timestamp
    latest_data = query_one("SELECT MAX(ordered_at) as latest FROM rpt_olist_orders")

    healthy_tables = sum(1 for t in counts if t['healthy'])
    success_rate = round(run_stats.get('successful_runs', 0) / max(run_stats.get('total_runs', 1), 1) * 100, 1)
    health_score = int((healthy_tables / len(counts)) * 60 + (success_rate / 100) * 40)

    return jsonify({
        "health_score": health_score,
        "success_rate": success_rate,
        "avg_duration": run_stats.get("avg_duration", 0),
        "avg_throughput": run_stats.get("avg_throughput", 0),
        "prod_total": prod_totals.get("prod_total", 0),
        "rpt_rows": quality.get("prod_rows", 0),
        "tables": counts,
        "latest_run": {
            "status": latest_run.get("status", "unknown"),
            "run_date": str(latest_run.get("run_date", ""))[:16],
            "duration": latest_run.get("duration_seconds", 0),
            "rows": latest_run.get("rows_processed", 0)
        },
        "latest_data": str(latest_data.get("latest", ""))[:10],
        "last_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# ── Run History ────────────────────────────────────────────────────────────────
@app.route('/api/runs')
def runs():
    data = query("""
        SELECT run_id, pipeline_name, run_date, status,
               duration_seconds, rows_processed, rows_per_second
        FROM pipeline_run_log
        ORDER BY run_date DESC
        LIMIT 30
    """)
    for r in data:
        r['run_date'] = str(r['run_date'])[:16]
    return jsonify(data)

# ── Quality Checks ─────────────────────────────────────────────────────────────
@app.route('/api/quality')
def quality():
    q = query_one("""
        SELECT
            COUNT(order_id) as total_orders,
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
            SUM(CASE WHEN ordered_at IS NULL THEN 1 ELSE 0 END) as null_dates,
            SUM(CASE WHEN total_order_value < 0 THEN 1 ELSE 0 END) as negative_values,
            SUM(CASE WHEN order_status NOT IN ('delivered','shipped','canceled',
                'invoiced','processing','unavailable','approved','created')
                THEN 1 ELSE 0 END) as invalid_statuses,
            SUM(CASE WHEN total_order_value = 0 THEN 1 ELSE 0 END) as zero_values
        FROM rpt_olist_orders
    """)

    checks = [
        {"name": "Order volume within expected range (90K–250K)", "passed": 90000 <= q.get('total_orders', 0) <= 250000},
        {"name": "All orders have unique identifiers", "passed": q.get('null_order_ids', 1) == 0},
        {"name": "All orders linked to a customer", "passed": q.get('null_customers', 1) == 0},
        {"name": "All orders have valid timestamps", "passed": q.get('null_dates', 1) == 0},
        {"name": "No negative financial values", "passed": q.get('negative_values', 1) == 0},
        {"name": "All order statuses are recognized", "passed": q.get('invalid_statuses', 1) == 0},
        {"name": "Zero-value orders within acceptable threshold", "passed": q.get('zero_values', 0) < 1000},
    ]

    passed = sum(1 for c in checks if c['passed'])
    return jsonify({
        "checks": checks,
        "passed": passed,
        "total": len(checks),
        "metrics": q
    })

# ── Agent Activity ─────────────────────────────────────────────────────────────
@app.route('/api/agents')
def agents():
    data = query("""
        SELECT agent_name, run_time, status, summary, slack_channel
        FROM agent_activity_log
        ORDER BY run_time DESC
        LIMIT 10
    """)
    for a in data:
        a['run_time'] = str(a['run_time'])[:16]
    return jsonify(data)

# ── Table Counts ───────────────────────────────────────────────────────────────
@app.route('/api/tables')
def tables():
    raw_tables = [
        ("raw_olist_orders", "order_id"),
        ("raw_olist_customers", "customer_id"),
        ("raw_olist_order_items", "order_id"),
        ("raw_olist_order_payments", "order_id"),
        ("raw_olist_products", "product_id"),
        ("raw_olist_sellers", "seller_id"),
    ]
    prod_tables = [
        ("fct_olist_orders", "order_id"),
        ("dim_olist_customers", "customer_unique_id"),
        ("dim_olist_products", "product_id"),
        ("dim_olist_sellers", "seller_id"),
        ("rpt_olist_orders", "order_id"),
    ]

    def get_counts(table_list):
        results = []
        for table, grain in table_list:
            try:
                r = query_one(f"SELECT COUNT({grain}) as cnt FROM {table}")
                results.append({
                    "table": table,
                    "grain": grain,
                    "count": r.get("cnt", 0)
                })
            except:
                results.append({"table": table, "grain": grain, "count": 0})
        return results

    return jsonify({
        "raw": get_counts(raw_tables),
        "prod": get_counts(prod_tables)
    })

# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('pipeline.html')


def keep_redshift_warm():
    """Ping Redshift every 5 minutes to prevent auto-pause"""
    import time
    while True:
        try:
            query_one("SELECT 1 as ping")
            print("✅ Redshift keepalive ping")
        except:
            pass
        time.sleep(300)  # every 5 minutes

# Start keepalive thread
warm_thread = threading.Thread(target=keep_redshift_warm, daemon=True)
warm_thread.start()

if __name__ == '__main__':
    app.run(debug=True, port=5001)