"""
Commodex background worker — runs the news poller and analysis scheduler.
Started as a separate process from the Flask web server.

News fetch: every 15 minutes (free — RSS + Google News, no AI cost)
AI analysis: 4× per day at 07:00, 12:00, 17:00, 22:00 IST
"""
import schedule
import time
from analysis import app, run_analysis, fetch_and_store_news

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


# News poller: every 15 minutes
schedule.every(15).minutes.do(_fetch_news)

# AI analysis: 01:30, 06:30, 11:30, 16:30 UTC = 07:00, 12:00, 17:00, 22:00 IST
schedule.every().day.at("01:30").do(_run_analysis)
schedule.every().day.at("06:30").do(_run_analysis)
schedule.every().day.at("11:30").do(_run_analysis)
schedule.every().day.at("16:30").do(_run_analysis)

print("Scheduler running.")
print("  News fetch:  every 15 minutes")
print("  AI analysis: 07:00, 12:00, 17:00, 22:00 IST")

while True:
    schedule.run_pending()
    time.sleep(30)
