import os
import json
import anthropic
from datetime import datetime
from dotenv import load_dotenv
from agent_tools import TOOLS, execute_tool

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

def run_insights_agent():
    """
    Business Insights Agent — analyzes trends and posts
    actionable business recommendations to #daily-insights.
    """
    print(f"\n🤖 Business Insights Agent starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = """You are an intelligent business insights agent for Eris Solutions.
Your job is to analyze e-commerce data and surface actionable business insights
that help non-technical business owners make better decisions.

You have access to rpt_olist_orders in Redshift with these columns:
order_id, customer_id, order_status, ordered_at, total_order_value,
is_delivered, is_canceled, days_to_deliver, delivery_speed_tier,
customer_segment, primary_payment_type, customer_city, customer_state,
total_items, total_freight, freight_pct_of_order, primary_category,
order_year, order_month_num, order_day_of_week

Your analysis should uncover:
1. Revenue trends and patterns
2. Customer behavior insights
3. Delivery performance opportunities
4. Geographic patterns
5. Product category performance
6. Payment method trends

Write insights in plain English that a non-technical business owner can act on.
Every insight should have a "So what?" — what should the business DO with this information.

Redshift SQL rules:
- Use DATEDIFF('day', start::timestamp, GETDATE()) for date differences  
- Use COUNT(*) for row counts
- Use ROUND(AVG(col), 2) for averages
- Use GROUP BY for aggregations
- Keep queries focused and simple

Post findings to #daily-insights on Slack.
Sign messages as "Eris Insights Agent"."""

    user_message = f"""Please analyze the Olist e-commerce data and generate 5 key business insights.

For each insight:
1. Run the relevant SQL query to get the data
2. Interpret what it means for the business
3. Provide a specific recommendation

Focus on:
- Which customer segment drives the most revenue?
- Which states have the best/worst delivery performance?
- What is the relationship between order value and delivery speed?
- Which day of week has highest order volume?
- What % of revenue comes from repeat vs one-time customers?

After gathering all data post a business insights report to #daily-insights.
Format it clearly with emojis, headers, and actionable recommendations.
Write it for a non-technical business owner, not a data engineer.

Current time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""

    messages = [{"role": "user", "content": user_message}]

    print("Agent is thinking and acting...")
    max_iterations = 20
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
            print("\n✅ Insights Agent completed successfully")
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

    print(f"\n🤖 Insights Agent finished — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return messages

if __name__ == '__main__':
    run_insights_agent()
