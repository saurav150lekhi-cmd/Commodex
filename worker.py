"""
Commodex background worker — runs the news poller and analysis scheduler.
Started as a separate process from the Flask web server.

News fetch:  every 15 minutes (always on)
AI analysis: Mon–Fri only, at 06:00 and 12:00 IST
             (00:30 UTC and 06:30 UTC)
             First brief: Monday 06:00 IST
             Last brief:  Friday 12:00 IST
"""
import schedule
import time
from datetime import datetime, timezone, timedelta
from analysis import app, run_analysis, fetch_and_store_news

IST_OFFSET = timedelta(hours=5, minutes=30)

print("Commodex worker started.")

with app.app_context():
    from models import AnalysisRun

    print("Running startup news fetch...")
    fetch_and_store_news()

    latest = AnalysisRun.query.order_by(AnalysisRun.run_at.desc()).first()
    if not latest or (datetime.now(timezone.utc) - latest.run_at) > timedelta(hours=3):
        print("No recent analysis found — running now...")
        run_analysis()
    else:
        print(f"Recent analysis found ({latest.run_at.strftime('%H:%M UTC')}) — skipping startup run.")


def _fetch_news():
    with app.app_context():
        fetch_and_store_news()


def _run_analysis_weekday():
    """Only run during Mon 06:00 – Fri 12:00 IST window."""
    now_ist  = datetime.now(timezone.utc) + IST_OFFSET
    weekday  = now_ist.weekday()          # 0=Mon … 6=Sun
    ist_hour = now_ist.hour

    # Monday 06:00 → Friday 12:00 IST
    if weekday > 4:                       # Saturday or Sunday
        return
    if weekday == 4 and ist_hour >= 13:   # Friday after 12:00
        return

    with app.app_context():
        run_analysis()


# News poller: every 15 minutes (always on)
schedule.every(15).minutes.do(_fetch_news)

# AI analysis: 00:30 UTC (06:00 IST) and 06:30 UTC (12:00 IST), weekdays only
schedule.every().day.at("00:30").do(_run_analysis_weekday)
schedule.every().day.at("06:30").do(_run_analysis_weekday)

print("Scheduler running.")
print("  News fetch:  every 15 minutes")
print("  AI analysis: Mon–Fri at 06:00 and 12:00 IST (00:30 and 06:30 UTC)")

while True:
    schedule.run_pending()
    time.sleep(30)
