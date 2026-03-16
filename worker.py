"""
Commodex background worker — runs the analysis scheduler.
Started as a separate process from the Flask web server.
"""
import schedule
import time
from analysis import app, run_analysis

print("Commodex worker started. Running initial analysis...")
with app.app_context():
    run_analysis()

schedule.every().day.at("00:00").do(lambda: _run())
schedule.every().day.at("06:00").do(lambda: _run())
schedule.every().day.at("12:00").do(lambda: _run())
schedule.every().day.at("18:00").do(lambda: _run())


def _run():
    with app.app_context():
        run_analysis()


print("Scheduler running. Next runs at 00:00, 06:00, 12:00, 18:00 UTC.")
while True:
    schedule.run_pending()
    time.sleep(30)
