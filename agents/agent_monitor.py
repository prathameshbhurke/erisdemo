import os
import ssl
import json
import anthropic
from datetime import datetime
from dotenv import load_dotenv
from agent_tools import TOOLS, execute_tool

load_dotenv()

# Local dev SSL fix
if os.getenv("ENVIRONMENT", "development") == "development":
    ssl._create_default_https_context = ssl._create_unverified_context

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

def run_monitor_agent():
    """
    Pipeline Monitor Agent — checks DAG health, data freshness,
    and posts status to Slack automatically.
    """
    print(f"\n🤖 Pipeline Monitor Agent starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = """You are an intelligent pipeline monitoring agent for Eris Solutions, 
    a data engineering consulting firm. Your job is to:

    1. Check the status of all Airflow DAGs (olist_ingestion and olist_pipeline)
    2. Verify data freshness in Redshift by checking row counts and latest timestamps
    3. Identify any anomalies or issues
    4. Post a clear, concise status update to #pipeline-alerts on Slack

    Use the available tools to gather information, then make a decision:
    - If everything is healthy: post a green success message to Slack
    - If there are warnings: post a yellow warning message to Slack  
    - If there are failures: post a red alert message to Slack

    Always be specific — include DAG names, row counts, timestamps, and exact issues.
    Format Slack messages with emojis and clear sections for easy reading.
    Sign your messages as "Eris Monitor Agent".

    IMPORTANT — Key table and column names in Redshift:
    - Main reporting table: rpt_olist_orders
    - Key columns: order_id, customer_id, order_status, ordered_at (timestamp),
    total_order_value, is_delivered, is_canceled, days_to_deliver,
    delivery_speed_tier, customer_segment, primary_payment_type
    - Raw tables: raw_olist_orders, raw_olist_customers, raw_olist_order_items
    - Staging tables: stg_olist_orders, stg_olist_customers
    - Always use rpt_olist_orders for monitoring checks
    - Never use raw table column names like order_purchase_timestamp
    
    IMPORTANT — Key table and column names in Redshift:
    - Main reporting table: rpt_olist_orders
    - Key columns: order_id, customer_id, order_status, ordered_at (timestamp),
    total_order_value, is_delivered, is_canceled, days_to_deliver,
    delivery_speed_tier, customer_segment, primary_payment_type
    - Raw tables: raw_olist_orders, raw_olist_customers, raw_olist_order_items
    - Staging tables: stg_olist_orders, stg_olist_customers
    - Always use rpt_olist_orders for monitoring checks
    - Never use raw table column names like order_purchase_timestamp

    IMPORTANT — Redshift SQL syntax rules:
    - Use DATEDIFF('day', start, end) not DATE_DIFF or date_diff
    - Use GETDATE() for current timestamp not NOW() or CURRENT_TIMESTAMP
    - Use DATEDIFF('day', ordered_at::timestamp, GETDATE()) for age calculations
    - Cast timestamps explicitly: ordered_at::timestamp
    - For data freshness use: SELECT MAX(ordered_at) FROM rpt_olist_orders
    - For row counts use: SELECT COUNT(*) FROM rpt_olist_orders
    - Keep queries simple — avoid complex date arithmetic
    """

    user_message = f"""Please perform a full pipeline health check right now.

Check the following:
1. Status of olist_ingestion DAG
2. Status of olist_pipeline DAG  
3. Row count in rpt_olist_orders (should be ~100,000)
4. Latest order timestamp in rpt_olist_orders
5. Any null order_ids or customer_ids

Based on your findings, post an appropriate status update to #pipeline-alerts.

Current time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""

    messages = [{"role": "user", "content": user_message}]

    # ── Agentic Loop ───────────────────────────────────────────────────────────
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

        # Add assistant response to messages
        messages.append({"role": "assistant", "content": response.content})

        # Check if agent is done
        if response.stop_reason == "end_turn":
            print("\n✅ Agent completed successfully")
            # Print final text response
            for block in response.content:
                if hasattr(block, 'text'):
                    print(f"\nAgent summary:\n{block.text}")
            break

        # Process tool calls
        if response.stop_reason == "tool_use":
            tool_results = []

            for block in response.content:
                if block.type == "tool_use":
                    print(f"  → Using tool: {block.name}")
                    print(f"    Input: {json.dumps(block.input, indent=2)[:200]}")

                    # Execute the tool
                    result = execute_tool(block.name, block.input)
                    print(f"    Result: {str(result)[:200]}")

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, default=str)
                    })

            # Add tool results to messages
            messages.append({"role": "user", "content": tool_results})

    print(f"\n🤖 Monitor Agent finished — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return messages

if __name__ == '__main__':
    run_monitor_agent()
