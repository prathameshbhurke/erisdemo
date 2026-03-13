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
```

---

### Step 4: Push to GitHub

First create a new repo on GitHub:
1. Go to **github.com**
2. Click **"+"** → **"New repository"**
3. Name it: `eris-data-platform`
4. Keep it **Private**
5. Don't initialize with README (we already have one)
6. Click **"Create repository"**

Then push from terminal:
```
cd /Users/prathameshbhurke/project_phoenix/eris-data-platform
git add .
git commit -m "Initial commit — full data pipeline setup"
git branch -M main
git remote add origin https://github.com/prathameshbhurke/eris-data-platform.git
git push -u origin main
