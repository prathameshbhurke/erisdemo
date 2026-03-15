import os
import sys
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Add agents folder to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent_monitor import run_monitor_agent
from agent_quality import run_quality_agent
from agent_insights import run_insights_agent

def run_all_agents():
    """Run all three agents in sequence"""
    print(f"\n{'='*60}")
    print(f"🚀 Eris Agent Service Starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    results = {}

    # 1. Monitor Agent
    try:
        print("\n[1/3] Running Pipeline Monitor Agent...")
        run_monitor_agent()
        results['monitor'] = '✅ Success'
    except Exception as e:
        results['monitor'] = f'❌ Failed: {e}'
        print(f"Monitor agent failed: {e}")

    # 2. Quality Agent
    try:
        print("\n[2/3] Running Data Quality Agent...")
        run_quality_agent()
        results['quality'] = '✅ Success'
    except Exception as e:
        results['quality'] = f'❌ Failed: {e}'
        print(f"Quality agent failed: {e}")

    # 3. Insights Agent
    try:
        print("\n[3/3] Running Business Insights Agent...")
        run_insights_agent()
        results['insights'] = '✅ Success'
    except Exception as e:
        results['insights'] = f'❌ Failed: {e}'
        print(f"Insights agent failed: {e}")

    # Summary
    print(f"\n{'='*60}")
    print("🏁 Agent Service Run Complete")
    for agent, status in results.items():
        print(f"  {agent.capitalize()} Agent: {status}")
    print(f"{'='*60}\n")

    return results

def run_monitor_only():
    """Run just the monitor agent — for frequent health checks"""
    print(f"\n⚡ Quick health check — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    run_monitor_agent()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Eris Agent Service')
    parser.add_argument('--mode', choices=['all', 'monitor', 'quality', 'insights', 'schedule'],
                        default='all', help='Which agents to run')
    args = parser.parse_args()

    if args.mode == 'all':
        run_all_agents()

    elif args.mode == 'monitor':
        run_monitor_agent()

    elif args.mode == 'quality':
        run_quality_agent()

    elif args.mode == 'insights':
        run_insights_agent()

    elif args.mode == 'schedule':
        print("🕐 Starting scheduled agent service...")
        print("  Monitor: every 1 hour")
        print("  All agents: daily at 7am")

        # Run monitor every hour
        schedule.every(1).hours.do(run_monitor_only)

        # Run all agents daily at 7am
        schedule.every().day.at("07:00").do(run_all_agents)

        # Run once immediately on startup
        run_all_agents()

        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)
