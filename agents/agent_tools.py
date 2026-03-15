import ssl
import os

if os.getenv("ENVIRONMENT", "development") == "development":
    ssl._create_default_https_context = ssl._create_unverified_context

import redshift_connector
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import uuid
from datetime import datetime

load_dotenv()


def load_prompt(agent_name: str) -> str:
    """Load agent prompt from file — allows runtime updates via UI"""
    prompt_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "prompts",
        f"{agent_name}_prompt.txt"
    )
    try:
        with open(prompt_path, 'r') as f:
            content = f.read()
            print(f"✅ Loaded {agent_name} prompt ({len(content)} chars)")
            return content
    except FileNotFoundError:
        print(f"⚠️ Prompt file not found for {agent_name}")
        return f"You are an intelligent {agent_name} agent for Eris Solutions."


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


def log_agent_activity(agent_name: str, status: str, summary: str, slack_channel: str):
    """Log agent run to Redshift for dashboard tracking"""
    conn = redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        database=os.getenv("REDSHIFT_DB"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO agent_activity_log
            (log_id, agent_name, run_time, status, summary, slack_channel)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        str(uuid.uuid4())[:50],
        agent_name,
        datetime.now(),
        status,
        summary[:500],
        slack_channel
    ))
    conn.close()
    print(f"✅ Activity logged for {agent_name}")


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
                    "description": "Optional color: good (green), warning (yellow), danger (red)"
                }
            },
            "required": ["channel", "message"]
        }
    },
    {
        "name": "log_agent_activity",
        "description": "Log the agent's activity to Redshift for dashboard tracking. Call this at the end of every run with a summary of what was found.",
        "input_schema": {
            "type": "object",
            "properties": {
                "agent_name": {
                    "type": "string",
                    "description": "Name of the agent e.g. Pipeline Monitor, Data Quality, Business Insights"
                },
                "status": {
                    "type": "string",
                    "description": "Status of the run: success, warning, or failure"
                },
                "summary": {
                    "type": "string",
                    "description": "Brief summary of findings (max 500 chars)"
                },
                "slack_channel": {
                    "type": "string",
                    "description": "Slack channel where results were posted"
                }
            },
            "required": ["agent_name", "status", "summary", "slack_channel"]
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
    elif tool_name == "log_agent_activity":
        return log_agent_activity(
            tool_input["agent_name"],
            tool_input["status"],
            tool_input["summary"],
            tool_input["slack_channel"]
        )
    else:
        return {"error": f"Unknown tool: {tool_name}"}