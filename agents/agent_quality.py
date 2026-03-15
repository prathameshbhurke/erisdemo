import os
import json
import anthropic
from datetime import datetime
from dotenv import load_dotenv
from agent_tools import TOOLS, execute_tool, load_prompt

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")


def run_quality_agent():
    """
    Data Quality Agent — analyzes data quality across all layers
    and posts insights to #data-quality on Slack.
    """
    print(f"\n🤖 Data Quality Agent starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Load prompt from file — editable via dashboard UI
    system_prompt = load_prompt("quality")

    user_message = f"""Please run a comprehensive data quality analysis on rpt_olist_orders.

Check the following:
1. Null counts for critical columns (order_id, customer_id, ordered_at, total_order_value)
2. Orders with negative or zero total_order_value
3. Distribution of order_status values
4. Distribution of delivery_speed_tier values
5. Average and max days_to_deliver for delivered orders
6. Distribution of customer_segment values
7. Any orders where total_freight > total_order_value
8. Distribution of primary_payment_type
9. Check pipeline_thresholds table and validate counts against defined thresholds

Based on findings post a quality report to #data-quality with:
- Overall quality score (out of 100)
- Green items (passing)
- Yellow items (warnings)
- Red items (failures)
- Key insight or anomaly found
- Specific recommended actions for each issue

Then log your activity using the log_agent_activity tool.

Current time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""

    messages = [{"role": "user", "content": user_message}]

    print("Agent is thinking and acting...")
    max_iterations = 15
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
            print("\n✅ Quality Agent completed successfully")
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

    print(f"\n🤖 Quality Agent finished — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return messages


if __name__ == '__main__':
    run_quality_agent()