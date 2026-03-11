import feedparser
import anthropic
import json
import time
import threading
import schedule
from datetime import datetime
from flask import Flask, jsonify

ANTHROPIC_API_KEY = "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"

app = Flask(__name__)
latest_results = {}

NEWS_SOURCES = [
    "https://economictimes.indiatimes.com/news/international/rssfeeds/2647163.cms",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://feeds.reuters.com/reuters/commoditiesNews",
    "https://feeds.reuters.com/reuters/businessNews",
]

COMMODITIES = {
    "Gold":        ["gold price", "gold rate", "gold futures", "bullion", "xau", "gold rises", "gold falls", "gold hits", "gold climbs"],
    "Crude Oil":   ["crude oil", "wti", "brent", "west texas", "opec", "petroleum price", "oil price", "oil rises", "oil falls"],
    "Silver":      ["silver price", "silver rate", "silver futures", "xag"],
    "Copper":      ["copper price", "copper futures", "lme copper", "comex copper"],
    "Natural Gas": ["natural gas", "natgas", "lng", "henry hub"],
}

GOOGLE_SEARCHES = {
    "Gold":        "gold commodity price today",
    "Crude Oil":   "WTI Brent crude oil price today",
    "Silver":      "silver commodity price today",
    "Copper":      "copper LME price today",
    "Natural Gas": "natural gas price today",
}

def fetch_all_articles():
    all_articles = []
    for url in NEWS_SOURCES:
        try:
            feed = feedparser.parse(url)
            name = feed.feed.get("title", url)
            for entry in feed.entries[:30]:
                all_articles.append({
                    "title":     entry.get("title", "").strip(),
                    "summary":   entry.get("summary", "").strip(),
                    "url":       entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "source":    name,
                })
        except:
            pass
    for commodity, search in GOOGLE_SEARCHES.items():
        try:
            url = "https://news.google.com/rss/search?q=" + search.replace(" ", "+") + "&hl=en-IN&gl=IN&ceid=IN:en"
            feed = feedparser.parse(url)
            for entry in feed.entries[:10]:
                all_articles.append({
                    "title":     entry.get("title", "").strip(),
                    "summary":   entry.get("summary", "").strip(),
                    "url":       entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "source":    "Google News",
                })
        except:
            pass
    return all_articles

def filter_by_commodity(articles):
    result = {name: [] for name in COMMODITIES}
    seen   = {name: set() for name in COMMODITIES}
    for article in articles:
        text = (article["title"] + " " + article["summary"]).lower()
        for commodity, keywords in COMMODITIES.items():
            for keyword in keywords:
                if keyword in text:
                    if article["title"] not in seen[commodity]:
                        result[commodity].append(article)
                        seen[commodity].add(article["title"])
                    break
    return result

def analyse_commodity(commodity_name, articles):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    headlines = "\n".join("- " + a["title"] + " (" + a["source"] + ")" for a in articles[:15]) or "No news."
    prompt = """You are a senior commodity analyst writing a briefing for a family office investment committee.

COMMODITY: """ + commodity_name + """
TIME: """ + datetime.now().strftime("%d %b %Y, %I:%M %p") + """ IST

RECENT NEWS:
""" + headlines + """

Write a detailed investment briefing covering:

1. MARKET SITUATION — What is happening right now. Price direction, key moves, volatility.

2. MACRO DRIVERS — Global factors driving this commodity. Include geopolitics, central bank policy, USD strength, demand/supply shifts, inventory data if relevant.

3. INDIA ANGLE — How this affects Indian markets specifically. MCX prices, import costs, INR impact, domestic demand.

4. RISKS TO WATCH — 2-3 specific things that could change the picture in the next 1-4 weeks.

5. POSITIONING VIEW — Clear Bullish / Bearish / Neutral call with a short reason. Include suggested time horizon (short term 1-2 weeks, medium term 1-3 months).

Be specific, direct and analytical. No generic statements. Write like a professional who has read every article carefully. Keep it under 350 words."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text

def run_analysis():
    global latest_results
    print("[" + datetime.now().strftime("%d %b %Y, %I:%M %p") + "] Fetching news...")
    all_articles = fetch_all_articles()
    news = filter_by_commodity(all_articles)
    results = {}
    for commodity, articles in news.items():
        print("Analysing " + commodity + "...")
        try:
            analysis = analyse_commodity(commodity, articles) if articles else "Not enough news."
        except Exception as e:
            analysis = "Error: " + str(e)
        results[commodity] = {
            "analysis":  analysis,
            "articles":  articles[:15],
            "timestamp": datetime.now().strftime("%d %b %Y, %I:%M %p"),
            "count":     len(articles),
        }
    latest_results = results
    with open("results.json", "w") as f:
        json.dump(results, f)
    print("Done. Next run at 09:00, 15:00, 21:00 or 23:00.")

def scheduler_loop():
    schedule.every().day.at("09:00").do(run_analysis)
    schedule.every().day.at("15:00").do(run_analysis)
    schedule.every().day.at("21:00").do(run_analysis)
    schedule.every().day.at("23:00").do(run_analysis)
    while True:
        schedule.run_pending()
        time.sleep(30)

@app.route("/data")
def get_data():
    response = jsonify(latest_results)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/")
def home():
    return "Commodex running."

run_analysis()
thread = threading.Thread(target=scheduler_loop, daemon=True)
thread.start()
print("Starting server on http://localhost:5000")

app.run(host="0.0.0.0", port=10000, use_reloader=False)
