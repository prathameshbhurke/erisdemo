# Eris Data Platform

End-to-end data pipeline built for e-commerce analytics.

## Architecture
GitHub CSV → Airbyte Cloud → S3 → Redshift → dbt → Great Expectations → Airflow → Claude AI → Email + Dashboard

## Stack
- **Ingestion:** Airbyte Cloud
- **Warehouse:** AWS Redshift Serverless
- **Transformation:** dbt Core
- **Quality:** Great Expectations
- **Orchestration:** Airflow (Astronomer)
- **AI Layer:** Claude API (Anthropic)
- **Reporting:** SendGrid + Streamlit

## Setup
1. Clone this repo
2. Create virtual environment: `python3 -m venv dbt-env`
3. Activate: `source dbt-env/bin/activate`
4. Install packages: `pip install dbt-redshift great-expectations anthropic sendgrid streamlit`
5. Copy `.env.example` to `.env` and fill in your credentials
6. Run `dbt debug` to verify connection

## Project Structure
- `dbt/` — dbt models and configuration
- `airflow/` — Airflow DAGs
- `scripts/` — Python scripts (quality checks, AI report)
- `dashboard/` — Streamlit dashboard
- `infrastructure/` — AWS CDK infrastructure code

