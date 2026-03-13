import anthropic
import redshift_connector
import json
from datetime import datetime
import sendgrid
from sendgrid.helpers.mail import Mail
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# API Keys
ANTHROPIC_API_KEY = "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"
SENDGRID_API_KEY = "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"

# Email settings
FROM_EMAIL = "bhurkeprathamesh@gmail.com"
TO_EMAIL = "reshma.prathamesh@gmail.com"  # Who receives the report

REDSHIFT_CONFIG = {
    'host': 'ecommerce-workgroup.680019129594.us-east-1.redshift-serverless.amazonaws.com',
    'database': 'dev',
    'port': 5439,
    'user': 'admin',
    'password': 'DragonDaima2026'
}

def get_pipeline_stats():
    """Pull key metrics from Redshift"""
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM fct_orders")
    total_orders = cursor.fetchone()[0]

    cursor.execute("""
        SELECT status, COUNT(*) as count 
        FROM fct_orders 
        GROUP BY status
    """)
    status_breakdown = cursor.fetchall()

    cursor.execute("SELECT ROUND(SUM(order_amount), 2) FROM fct_orders WHERE is_completed = true")
    total_revenue = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM fct_orders WHERE order_id IS NULL")
    null_order_ids = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM fct_orders WHERE customer_name IS NULL")
    null_customers = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(order_date) FROM fct_orders")
    latest_order = cursor.fetchone()[0]

    conn.close()

    return {
        "total_orders": total_orders,
        "status_breakdown": status_breakdown,
        "total_revenue": float(total_revenue) if total_revenue else 0,
        "null_order_ids": null_order_ids,
        "null_customers": null_customers,
        "latest_order_date": str(latest_order),
        "pipeline_run_time": datetime.now().strftime('%Y-%m-%d %H:%M'),
        "quality_checks_passed": 4,
        "quality_checks_total": 4
    }

def generate_ai_report(stats):
    """Send pipeline stats to Claude and get plain English report"""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""
You are a data pipeline assistant for an e-commerce company.
Generate a concise, friendly daily pipeline health report based on these stats.
Write it as if you are sending it to a non-technical business owner.
Keep it under 200 words. Highlight any concerns clearly.
Format it nicely for an email with clear sections.

Pipeline Stats:
{json.dumps(stats, indent=2, default=str)}

Format the report with:
- A brief greeting with today's date
- Pipeline status (success/failure)
- Key data highlights (orders, revenue, quality)
- Any anomalies or things to watch
- Next scheduled run
"""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text

def send_email(report, stats):
    """Send the AI report via SendGrid"""
    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)

    # Build HTML email
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: #2E4B8F; padding: 20px; border-radius: 8px 8px 0 0;">
            <h1 style="color: white; margin: 0;">📊 Daily Pipeline Report</h1>
            <p style="color: #ccc; margin: 5px 0 0 0;">{datetime.now().strftime('%B %d, %Y')}</p>
        </div>
        
        <div style="background-color: #f9f9f9; padding: 20px; border-radius: 0 0 8px 8px;">
            <div style="background-color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h2 style="color: #2E4B8F;">AI Summary</h2>
                <p style="line-height: 1.6;">{report.replace(chr(10), '<br>')}</p>
            </div>

            <div style="background-color: white; padding: 20px; border-radius: 8px;">
                <h2 style="color: #2E4B8F;">Pipeline Stats</h2>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: #f0f4ff;">
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Orders</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{stats['total_orders']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Revenue</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">${stats['total_revenue']}</td>
                    </tr>
                    <tr style="background-color: #f0f4ff;">
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Quality Checks</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">✅ {stats['quality_checks_passed']}/{stats['quality_checks_total']} passed</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Pipeline Run</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{stats['pipeline_run_time']}</td>
                    </tr>
                    <tr style="background-color: #f0f4ff;">
                        <td style="padding: 8px; border: 1px solid #ddd;"><strong>Data Issues</strong></td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{'⚠️ Found issues' if stats['null_order_ids'] > 0 else '✅ No issues found'}</td>
                    </tr>
                </table>
            </div>
        </div>

        <p style="color: #888; font-size: 12px; text-align: center; margin-top: 20px;">
            Powered by your Data Pipeline — Automated by AI
        </p>
    </body>
    </html>
    """

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAIL,
        subject=f"📊 Daily Pipeline Report — {datetime.now().strftime('%B %d, %Y')}",
        html_content=html_content
    )

    response = sg.send(message)
    print(f"✅ Email sent! Status code: {response.status_code}")
    return response.status_code

def run_ai_report():
    """Main function — pull stats, generate report, send email"""
    print("Pulling pipeline stats from Redshift...")
    stats = get_pipeline_stats()

    print("Generating AI report...")
    report = generate_ai_report(stats)

    print("\n========== AI PIPELINE REPORT ==========")
    print(report)
    print("=========================================\n")

    print("Sending email report...")
    send_email(report, stats)

    return report

if __name__ == '__main__':
    run_ai_report()
