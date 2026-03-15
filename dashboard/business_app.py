from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import redshift_connector
import os
import anthropic
from dotenv import load_dotenv
from datetime import datetime
import ssl
import threading
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

@app.route('/')
def business_dashboard():
    return render_template('business.html')

@app.route('/api/business/overview')
def business_overview():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    period = request.args.get('period', 'all')
    segment = request.args.get('segment', 'all')
    state = request.args.get('state', 'all')
    category = request.args.get('category', 'all')

    filters = ["1=1"]
    if period != 'all':
        filters.append(f"order_year = {period}")
    if segment != 'all':
        filters.append(f"customer_segment = '{segment}'")
    if state != 'all':
        filters.append(f"customer_state = '{state}'")
    if category != 'all':
        filters.append(f"primary_category = '{category}'")

    where = " AND ".join(filters)

    # Define all queries
    queries = {
        "kpis": (True, f"""
            SELECT
                COUNT(order_id) as total_orders,
                ROUND(SUM(CASE WHEN is_delivered = true THEN total_order_value ELSE 0 END), 2) as total_revenue,
                ROUND(AVG(CASE WHEN is_delivered = true THEN total_order_value END), 2) as avg_order_value,
                ROUND(AVG(CASE WHEN is_delivered = true THEN days_to_deliver END), 1) as avg_delivery_days,
                ROUND(SUM(CASE WHEN is_delivered = true THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 1) as delivery_rate,
                ROUND(SUM(CASE WHEN customer_segment != 'One Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 1) as retention_rate
            FROM rpt_olist_orders WHERE {where}
        """),
        "revenue_trend": (False, f"""
            SELECT order_year, order_month_num,
                COUNT(order_id) as orders,
                ROUND(SUM(total_order_value), 2) as revenue
            FROM rpt_olist_orders
            WHERE {where} AND order_year IS NOT NULL
            GROUP BY order_year, order_month_num
            ORDER BY order_year, order_month_num
        """),
        "segments": (False, f"""
            SELECT customer_segment, COUNT(order_id) as orders,
                ROUND(SUM(total_order_value), 2) as revenue
            FROM rpt_olist_orders
            WHERE {where} AND customer_segment IS NOT NULL
            GROUP BY customer_segment ORDER BY orders DESC
        """),
        "states": (False, f"""
            SELECT customer_state, COUNT(order_id) as orders,
                ROUND(SUM(total_order_value), 2) as revenue,
                ROUND(AVG(CASE WHEN is_delivered = true THEN days_to_deliver END), 1) as avg_delivery
            FROM rpt_olist_orders
            WHERE {where} AND customer_state IS NOT NULL
            GROUP BY customer_state ORDER BY revenue DESC LIMIT 10
        """),
        "delivery": (False, f"""
            SELECT delivery_speed_tier, COUNT(order_id) as orders
            FROM rpt_olist_orders
            WHERE {where} AND delivery_speed_tier IS NOT NULL
            GROUP BY delivery_speed_tier ORDER BY orders DESC
        """),
        "payments": (False, f"""
            SELECT primary_payment_type, COUNT(order_id) as orders,
                ROUND(SUM(total_order_value), 2) as revenue
            FROM rpt_olist_orders
            WHERE {where} AND primary_payment_type IS NOT NULL
            GROUP BY primary_payment_type ORDER BY orders DESC
        """),
        "categories": (False, f"""
            SELECT primary_category, COUNT(order_id) as orders,
                ROUND(SUM(total_order_value), 2) as revenue,
                ROUND(AVG(total_order_value), 2) as avg_order_value
            FROM rpt_olist_orders
            WHERE {where} AND primary_category IS NOT NULL
            GROUP BY primary_category ORDER BY revenue DESC LIMIT 12
        """),
        "day_of_week": (False, f"""
            SELECT order_day_of_week, COUNT(order_id) as orders
            FROM rpt_olist_orders
            WHERE {where} AND order_day_of_week IS NOT NULL
            GROUP BY order_day_of_week ORDER BY order_day_of_week
        """),
    }

    def run_query(name, is_one, sql):
        try:
            return name, query_one(sql) if is_one else query(sql)
        except Exception as e:
            return name, {} if is_one else []

    # Run all queries in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(run_query, name, is_one, sql): name
            for name, (is_one, sql) in queries.items()
        }
        for future in as_completed(futures):
            name, result = future.result()
            results[name] = result

    results["filters"] = {
        "period": period, "segment": segment,
        "state": state, "category": category
    }

    return jsonify(results)

@app.route('/api/business/ai_insight', methods=['POST'])
def business_ai_insight():
    data = request.get_json()
    question = data.get('question', '')
    context = data.get('context', {})

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    prompt = f"""You are an AI business analyst for an e-commerce company.
Answer this question concisely in 2-3 sentences with a specific recommended action.
Be direct, use numbers from the context, write for a non-technical business owner.

Current dashboard context:
- Total orders: {context.get('total_orders', 'N/A')}
- Total revenue: ${context.get('total_revenue', 'N/A')}
- Avg order value: ${context.get('avg_order_value', 'N/A')}
- Avg delivery days: {context.get('avg_delivery_days', 'N/A')}
- Retention rate: {context.get('retention_rate', 'N/A')}%
- Active filters: {context.get('filters', {})}

Question: {question}

Format your response as exactly two lines:
INSIGHT: [2-3 sentence analysis with specific numbers]
ACTION: [One specific recommended action starting with a verb]"""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )

    response_text = message.content[0].text
    lines = response_text.strip().split('\n')
    insight = ''
    action = ''
    for line in lines:
        if line.startswith('INSIGHT:'):
            insight = line.replace('INSIGHT:', '').strip()
        elif line.startswith('ACTION:'):
            action = line.replace('ACTION:', '').strip()

    return jsonify({
        "insight": insight or response_text,
        "action": action or "Review the data and take appropriate action"
    })

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
    app.run(debug=True, port=5002)
