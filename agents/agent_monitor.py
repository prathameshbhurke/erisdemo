import os
import json
import anthropic
from datetime import datetime
from dotenv import load_dotenv
from agent_tools import TOOLS, execute_tool, load_prompt

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")


def run_monitor_agent():
    """
    Pipeline Monitor Agent — checks DAG health, data freshness,
    and posts status to Slack automatically.
    """
    print(f"\n🤖 Pipeline Monitor Agent starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Load prompt from file — editable via dashboard UI
    system_prompt = load_prompt("monitor")

    user_message = f"""Please perform a full pipeline health check right now.

Check the following:
1. Status of olist_ingestion DAG
2. Status of olist_pipeline DAG
3. Row count in rpt_olist_orders (should be ~100,000)
4. Latest order timestamp in rpt_olist_orders
5. Any null order_ids or customer_ids
6. Check pipeline_thresholds table for per-table thresholds and validate each table

Based on your findings, post an appropriate status update to #pipeline-alerts.
Then log your activity using the log_agent_activity tool.

Current time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""

    messages = [{"role": "user", "content": user_message}]

    print("Agent is thinking and acting...")
    max_iterations = 10
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        print(f"\n  Iteration {iteration}...")

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=system_prompt,
            tools=TOOLS,
            messages=messages
        )

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            print("\n✅ Monitor Agent completed successfully")
            for block in response.content:
                if hasattr(block, 'text'):
                    print(f"\nAgent summary:\n{block.text}")
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    print(f"  → Using tool: {block.name}")
                    print(f"    Input: {json.dumps(block.input, indent=2)[:200]}")
                    result = execute_tool(block.name, block.input)
                    print(f"    Result: {str(result)[:200]}")
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, default=str)
                    })
            messages.append({"role": "user", "content": tool_results})

    print(f"\n🤖 Monitor Agent finished — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return messages


if __name__ == '__main__':
    run_monitor_agent()