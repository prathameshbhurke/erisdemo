from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import redshift_connector
import pandas as pd
import great_expectations as gx
import sys
sys.path.insert(0, '/usr/local/airflow/dags')
from ai_report import run_ai_report
from airflow.utils.state import DagRunState

t_wait_for_ingestion = ExternalTaskSensor(
    task_id='wait_for_olist_ingestion',
    external_dag_id='olist_ingestion',
    external_task_id=None,
    allowed_states=[DagRunState.SUCCESS],
    failed_states=[DagRunState.FAILED],
    execution_delta=timedelta(minutes=30),
    timeout=3600,
    poke_interval=60,
    mode='poke'
)


# Default settings for all tasks
default_args = {
    'owner': 'prathamesh',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Connection details
REDSHIFT_CONFIG = {
    'host': 'ecommerce-workgroup.680019129594.us-east-1.redshift-serverless.amazonaws.com',
    'database': 'dev',
    'port': 5439,
    'user': 'admin',
    'password': 'DragonDaima2026'
}

# Task 1: Check Redshift connection
def check_redshift():
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_orders")
    count = cursor.fetchone()[0]
    conn.close()
    print(f"✅ Redshift connected — {count} raw orders found")
    return count

# Task 2: Run data quality checks
def run_quality_checks():
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fct_orders")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()

    df = pd.DataFrame(rows, columns=columns)
    context = gx.get_context()

    try:
        data_source = context.data_sources.add_pandas("airflow_pandas_source")
    except:
        data_source = context.data_sources.get("airflow_pandas_source")

    data_asset = data_source.add_dataframe_asset("fct_orders_airflow")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name="airflow_fct_orders_suite")
    suite = context.suites.add(suite)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["completed", "cancelled", "pending"]
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="order_amount",
        min_value=0,
        max_value=10000
    ))

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="airflow_fct_orders_validation",
            data=batch_definition,
            suite=suite
        )
    )

    results = validation_definition.run(batch_parameters={"dataframe": df})

    passed = sum(1 for r in results.results if r.success)
    total = len(results.results)
    print(f"✅ Quality checks: {passed}/{total} passed")

    if not results.success:
        raise Exception(f"❌ Quality checks failed: {passed}/{total} passed")

    return f"{passed}/{total} checks passed"

# Task 3: Log pipeline completion
def log_completion(**context):
    ti = context['ti']
    quality_result = ti.xcom_pull(task_ids='run_quality_checks')
    print(f"""
    ================================
    ✅ PIPELINE COMPLETED
    ================================
    Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}
    Quality Checks: {quality_result}
    Status: SUCCESS
    ================================
    """)

# Task 4: Generate AI report
def generate_report(**context):
    ti = context['ti']
    quality_result = ti.xcom_pull(task_ids='run_quality_checks')
    report = run_ai_report()
    print(f"\n✅ AI Report generated successfully")
    return report


# Define the DAG
with DAG(
    dag_id='ecommerce_pipeline',
    default_args=default_args,
    description='Daily e-commerce data pipeline',
    schedule='0 6 * * *',  # Every day at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'production'],
) as dag:

    # Task 1: Check connection
    check_connection = PythonOperator(
        task_id='check_redshift_connection',
        python_callable=check_redshift,
    )

    # Task 2: Run dbt transformations
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
            cd /usr/local/airflow && \
            dbt run \
                --project-dir /usr/local/airflow/dags/dbt_project \
                --profiles-dir /usr/local/airflow/dags/dbt_project
        """,
    )

    # Task 3: Run quality checks
    quality_checks = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks,
    )

    complete = PythonOperator(
        task_id='log_completion',
        python_callable=log_completion,
    )

    # Task 4: AI Report
    ai_report = PythonOperator(
        task_id='generate_ai_report',
        python_callable=generate_report,
    )

    # Define order: check → dbt → quality → complete → ai_report
    check_connection >> run_dbt >> quality_checks >> complete >> ai_report
