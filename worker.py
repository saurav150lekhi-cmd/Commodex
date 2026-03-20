"""
Commodex background worker — runs the news poller and analysis scheduler.
Started as a separate process from the Flask web server.

News fetch: every 15 minutes (free — RSS + Google News, no AI cost)
AI analysis: 4× per day at 06:00, 12:00, 18:00, 00:00 IST
Weekly PDF:  Sunday 21:00 IST (15:30 UTC) — saved to weekly_report.pdf
"""
import os
import schedule
import time
from analysis import app, run_analysis, fetch_and_store_news, generate_newsletter_pdf, load_latest_from_db, fetch_live_prices

print("Commodex worker started.")

with app.app_context():
    from models import AnalysisRun
    from datetime import datetime, timezone, timedelta

    # Run news fetch immediately on startup
    print("Running startup news fetch...")
    fetch_and_store_news()

    # Only run AI analysis on startup if no recent run exists
    latest = AnalysisRun.query.order_by(AnalysisRun.run_at.desc()).first()
    if not latest or (datetime.now(timezone.utc) - latest.run_at) > timedelta(hours=3):
        print("No recent analysis found — running now...")
        run_analysis()
    else:
        print(f"Recent analysis found ({latest.run_at.strftime('%H:%M UTC')}) — skipping startup run.")


def _fetch_news():
    with app.app_context():
        fetch_and_store_news()


def _run_analysis():
    with app.app_context():
        run_analysis()


def _generate_weekly_pdf():
    """Generate weekly PDF every Sunday 21:00 IST and save to disk."""
    with app.app_context():
        try:
            load_latest_from_db()
            from analysis import latest_results
            prices = fetch_live_prices()
            pdf_bytes = generate_newsletter_pdf(latest_results, prices)
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekly_report.pdf")
            with open(path, "wb") as f:
                f.write(pdf_bytes)
            print(f"Weekly PDF saved to {path}")
        except Exception as e:
            print(f"Weekly PDF generation failed: {e}")


# News poller: every 15 minutes
schedule.every(15).minutes.do(_fetch_news)

# AI analysis: 00:30, 06:30, 12:30, 18:30 UTC = 06:00, 12:00, 18:00, 00:00 IST
schedule.every().day.at("00:30").do(_run_analysis)
schedule.every().day.at("06:30").do(_run_analysis)
schedule.every().day.at("12:30").do(_run_analysis)
schedule.every().day.at("18:30").do(_run_analysis)

# Weekly PDF: Sunday 15:30 UTC = Sunday 21:00 IST
schedule.every().sunday.at("15:30").do(_generate_weekly_pdf)

print("Scheduler running.")
print("  News fetch:  every 15 minutes")
print("  AI analysis: 06:00, 12:00, 18:00, 00:00 IST")
print("  Weekly PDF:  Sunday 21:00 IST")

while True:
    schedule.run_pending()
    time.sleep(30)
