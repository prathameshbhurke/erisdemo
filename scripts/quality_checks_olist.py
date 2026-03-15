import great_expectations as gx
import redshift_connector
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def get_conn():
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        database=os.getenv("REDSHIFT_DB"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )

def run_quality_checks():
    print(f"Starting GX quality checks — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Load rpt_olist_orders into dataframe
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM rpt_olist_orders", conn)
    conn.close()
    print(f"Loaded {len(df):,} rows from rpt_olist_orders")

    # Set up GX context
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="olist_datasource")
    data_asset = data_source.add_dataframe_asset(name="rpt_olist_orders")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    suite = context.suites.add(gx.ExpectationSuite(name="olist_reporting_suite"))

    results = []

    def check(expectation, label):
        r = batch.validate(expectation)
        results.append((label, r.success))

    # ── Row Count ──────────────────────────────────────────────────
    check(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=90000, max_value=250000),
        "Row count between 90K-250K"
    )

    # ── Critical Columns Not Null ──────────────────────────────────
    for col in ["order_id", "customer_id", "order_status", "ordered_at"]:
        check(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col),
            f"{col} not null"
        )

    # ── Order Status Valid Values ──────────────────────────────────
    check(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="order_status",
            value_set=["delivered", "shipped", "canceled", "invoiced",
                       "processing", "unavailable", "approved", "created"]
        ),
        "order_status valid values"
    )

    # ── Revenue Sanity Checks ──────────────────────────────────────
    check(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_order_value", min_value=0, max_value=50000
        ),
        "total_order_value between 0-50000"
    )

    check(
        gx.expectations.ExpectColumnMeanToBeBetween(
            column="total_order_value", min_value=50, max_value=500
        ),
        "avg order value between $50-$500"
    )

    # ── Delivery Checks ────────────────────────────────────────────
    check(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="delivery_speed_tier",
            value_set=["Fast", "Normal", "Slow", "Not Delivered"]
        ),
        "delivery_speed_tier valid values"
    )

    check(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="days_to_deliver", min_value=0, max_value=365, mostly=0.95
        ),
        "days_to_deliver between 0-365"
    )

    # ── Customer Segment ───────────────────────────────────────────
    check(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="customer_segment",
            value_set=["One Time", "Repeat", "Loyal"],
            mostly=0.95
        ),
        "customer_segment valid values"
    )

    # ── Payment Checks ─────────────────────────────────────────────
    check(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="primary_payment_type",
            value_set=["credit_card", "boleto", "voucher", "debit_card", "not_defined"],
            mostly=0.95
        ),
        "primary_payment_type valid values"
    )

    check(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_payment_value", min_value=0, max_value=50000
        ),
        "total_payment_value between 0-50000"
    )

    # ── Print Results ──────────────────────────────────────────────
    print("\n========== GX QUALITY REPORT ==========")
    passed = 0
    failed = 0
    for label, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} — {label}")
        if success:
            passed += 1
        else:
            failed += 1

    print(f"\nResults: {passed}/{len(results)} checks passed")
    print("========================================\n")

    if failed > 0:
        raise Exception(f"❌ {failed} quality check(s) failed — pipeline halted")

    print("✅ All quality checks passed — safe to proceed to dashboard and reporting")
    return passed, len(results)

if __name__ == '__main__':
    run_quality_checks()
