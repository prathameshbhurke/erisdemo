import os
import redshift_connector
import requests
import ssl
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

load_dotenv()

# Local dev SSL fix
if os.getenv("ENVIRONMENT", "development") == "development":
    ssl._create_default_https_context = ssl._create_unverified_context

# ── Redshift Tool ──────────────────────────────────────────────────────────────
def query_redshift(sql: str) -> list:
    """Execute a SQL query against Redshift and return results"""
    conn = redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        database=os.getenv("REDSHIFT_DB"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

# ── Airflow Tool ───────────────────────────────────────────────────────────────
def get_dag_status(dag_id: str) -> dict:
    """Get the latest run status of an Airflow DAG"""
    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(
            url,
            auth=("admin", "admin"),
            params={"limit": 1, "order_by": "-execution_date"},
            verify=False
        )
        runs = response.json().get("dag_runs", [])
        if runs:
            run = runs[0]
            return {
                "dag_id": dag_id,
                "state": run.get("state"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "run_id": run.get("run_id")
            }
        return {"dag_id": dag_id, "state": "no_runs_found"}
    except Exception as e:
        return {"dag_id": dag_id, "state": "error", "error": str(e)}

# ── Slack Tool ─────────────────────────────────────────────────────────────────
def post_to_slack(channel: str, message: str, color: str = None) -> bool:
    """Post a message to a Slack channel"""
    client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
    try:
        if color:
            client.chat_postMessage(
                channel=channel,
                attachments=[{
                    "color": color,
                    "text": message,
                    "mrkdwn_in": ["text"]
                }]
            )
        else:
            client.chat_postMessage(
                channel=channel,
                text=message
            )
        return True
    except SlackApiError as e:
        print(f"Slack error: {e.response['error']}")
        return False

# ── Tool Definitions for Claude ────────────────────────────────────────────────
TOOLS = [
    {
        "name": "query_redshift",
        "description": "Execute a SQL query against the Redshift data warehouse and return results. Use this to check data freshness, row counts, quality metrics, and business KPIs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL query to execute against Redshift"
                }
            },
            "required": ["sql"]
        }
    },
    {
        "name": "get_dag_status",
        "description": "Get the latest run status of an Airflow DAG. Returns state (success/failed/running), start time, end time.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The Airflow DAG ID to check"
                }
            },
            "required": ["dag_id"]
        }
    },
    {
        "name": "post_to_slack",
        "description": "Post a message to a Slack channel. Use this to alert the team about pipeline status, data quality issues, or business insights.",
        "input_schema": {
            "type": "object",
            "properties": {
                "channel": {
                    "type": "string",
                    "description": "Slack channel name e.g. #pipeline-alerts"
                },
                "message": {
                    "type": "string",
                    "description": "The message to post to Slack"
                },
                "color": {
                    "type": "string",
                    "description": "Optional color for the message attachment: good (green), warning (yellow), danger (red)"
                }
            },
            "required": ["channel", "message"]
        }
    }
]

def execute_tool(tool_name: str, tool_input: dict):
    """Execute a tool by name with given input"""
    if tool_name == "query_redshift":
        return query_redshift(tool_input["sql"])
    elif tool_name == "get_dag_status":
        return get_dag_status(tool_input["dag_id"])
    elif tool_name == "post_to_slack":
        return post_to_slack(
            tool_input["channel"],
            tool_input["message"],
            tool_input.get("color")
        )
    else:
        return {"error": f"Unknown tool: {tool_name}"}