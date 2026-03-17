"""
Commodex background worker — runs the analysis scheduler.
Started as a separate process from the Flask web server.
"""
import schedule
import time
from analysis import app, run_analysis

print("Commodex worker started.")
with app.app_context():
    from models import AnalysisRun
    from datetime import datetime, timezone, timedelta
    latest = AnalysisRun.query.order_by(AnalysisRun.run_at.desc()).first()
    if not latest or (datetime.now(timezone.utc) - latest.run_at) > timedelta(hours=3):
        print("No recent analysis found — running now...")
        run_analysis()
    else:
        print(f"Recent analysis found ({latest.run_at.strftime('%H:%M UTC')}) — skipping startup run.")

schedule.every().day.at("01:30").do(lambda: _run())
schedule.every().day.at("06:30").do(lambda: _run())
schedule.every().day.at("11:30").do(lambda: _run())
schedule.every().day.at("16:30").do(lambda: _run())


def _run():
    with app.app_context():
        run_analysis()


print("Scheduler running. Next runs at 07:00, 12:00, 17:00, 22:00 IST.")
while True:
    schedule.run_pending()
    time.sleep(30)
