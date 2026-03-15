import os
import json
import anthropic
from datetime import datetime
from dotenv import load_dotenv
from agent_tools import TOOLS, execute_tool

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

def run_quality_agent():
    """
    Data Quality Agent — analyzes data quality across all layers
    and posts insights to #data-quality on Slack.
    """
    print(f"\n🤖 Data Quality Agent starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = """You are an intelligent data quality agent for Eris Solutions.
Your job is to analyze data quality across the entire pipeline and provide
actionable insights to the data team.

You have access to Redshift where the following tables exist:
- rpt_olist_orders: main reporting table (100K rows)
  columns: order_id, customer_id, order_status, ordered_at,
           total_order_value, is_delivered, is_canceled, days_to_deliver,
           delivery_speed_tier, customer_segment, primary_payment_type,
           customer_city, customer_state, total_items, total_freight,
           freight_pct_of_order, primary_category

Your quality checks should cover:
1. Completeness — null checks on critical columns
2. Validity — values within expected ranges
3. Consistency — logical relationships between fields
4. Distribution — unusual patterns in data

After running checks post a detailed quality report to #data-quality on Slack.

Redshift SQL rules:
- Use DATEDIFF('day', start::timestamp, GETDATE()) for date differences
- Use COUNT(*) for row counts
- Use ROUND(AVG(col), 2) for averages
- Keep queries simple and focused

Sign messages as "Eris Quality Agent"."""

    user_message = f"""Please run a comprehensive data quality analysis on rpt_olist_orders.

Check the following:
1. Null counts for critical columns (order_id, customer_id, ordered_at, total_order_value)
2. Orders with negative or zero total_order_value
3. Distribution of order_status values
4. Distribution of delivery_speed_tier values
5. Average and max days_to_deliver for delivered orders
6. Distribution of customer_segment values
7. Any orders where total_freight > total_order_value (freight costs more than order)
8. Distribution of primary_payment_type

Based on findings post a quality report to #data-quality with:
- Overall quality score (out of 100)
- Green items (passing)
- Yellow items (warnings)
- Red items (failures)
- Key insight or anomaly you found

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
