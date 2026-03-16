import feedparser
import anthropic
import json
import time
import threading
import schedule
import os
import urllib.request
from datetime import datetime
from flask import Flask, jsonify, send_from_directory

ANTHROPIC_API_KEY = os.environ.get("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", "")

app = Flask(__name__)
latest_results = {}

NEWS_SOURCES = [
    "https://economictimes.indiatimes.com/news/international/rssfeeds/2647163.cms",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://feeds.reuters.com/reuters/commoditiesNews",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://www.livemint.com/rss/markets",
    "https://www.livemint.com/rss/money",
]

COMMODITIES = {
    "Gold":        ["gold price", "gold rate", "gold futures", "bullion", "xau", "gold rises", "gold falls", "gold hits", "gold climbs"],
    "Crude Oil":   ["crude oil", "wti", "brent", "west texas", "opec", "petroleum price", "oil price", "oil rises", "oil falls"],
    "Silver":      ["silver price", "silver rate", "silver futures", "xag"],
    "Copper":      ["copper price", "copper futures", "lme copper", "comex copper"],
    "Natural Gas": ["natural gas", "natgas", "lng", "henry hub"],
}

GOOGLE_SEARCHES = {
    "Gold":        ["gold site:bloomberg.com", "gold site:reuters.com"],
    "Crude Oil":   ["crude oil site:bloomberg.com", "crude oil site:reuters.com"],
    "Silver":      ["silver site:bloomberg.com"],
    "Copper":      ["copper LME site:bloomberg.com"],
    "Natural Gas": ["natural gas site:bloomberg.com"],
}

HIGH_IMPACT_KEYWORDS = [
    "fed", "federal reserve", "rate decision", "opec", "sanctions", "war", "conflict",
    "inflation", "cpi", "gdp", "recession", "rate hike", "rate cut", "central bank",
    "rbi", "fomc", "powell", "inventory", "supply cut", "demand surge", "crash", "rally",
    "all-time high", "record", "collapse", "shortage", "surplus"
]

MEDIUM_IMPACT_KEYWORDS = [
    "forecast", "outlook", "estimate", "analyst", "report", "weekly", "monthly",
    "import", "export", "trade", "dollar", "usd", "rupee", "inr", "mcx", "nymex", "comex"
]

def score_article(article):
    text = (article["title"] + " " + article["summary"]).lower()
    for kw in HIGH_IMPACT_KEYWORDS:
        if kw in text:
            return "HIGH"
    for kw in MEDIUM_IMPACT_KEYWORDS:
        if kw in text:
            return "MEDIUM"
    return "LOW"

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
    for commodity, searches in GOOGLE_SEARCHES.items():
        for search in searches:
            try:
                url = "https://news.google.com/rss/search?q=" + search.replace(" ", "+") + "&hl=en-IN&gl=IN&ceid=IN:en"
                feed = feedparser.parse(url)
                if "bloomberg.com" in search:
                    label = "Bloomberg"
                elif "reuters.com" in search:
                    label = "Reuters"
                elif "wsj.com" in search:
                    label = "Wall Street Journal"
                elif "livemint.com" in search:
                    label = "Mint"
                else:
                    label = "Google News"
                for entry in feed.entries[:8]:
                    all_articles.append({
                        "title":     entry.get("title", "").strip(),
                        "summary":   entry.get("summary", "").strip(),
                        "url":       entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "source":    label,
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
                        article["impact"] = score_article(article)
                        result[commodity].append(article)
                        seen[commodity].add(article["title"])
                    break
    return result

def analyse_commodity(commodity_name, articles):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    headlines = "\n".join(
        "- " + a["title"] + " (" + a["source"] + ")"
        for a in articles[:15]
    ) or "No news available."

    prompt = """You are a senior commodity analyst and trading strategist for a hedge fund.

COMMODITY: """ + commodity_name + """
TIME: """ + datetime.now().strftime("%d %b %Y, %I:%M %p") + """ IST

RECENT NEWS:
""" + headlines + """

Return ONLY a valid JSON object with exactly this structure. No markdown, no explanation, just the JSON:

{
  "briefing": "3-4 paragraph plain text investment briefing covering market situation, macro drivers, India angle. No markdown, no bullet points.",
  "sentiment": "STRONG_BULLISH or BULLISH or NEUTRAL or BEARISH or STRONG_BEARISH",
  "drivers": {
    "up": ["driver 1", "driver 2", "driver 3"],
    "down": ["driver 1", "driver 2", "driver 3"]
  },
  "levels": {
    "resistance": "$XXXX",
    "support": "$XXXX",
    "mcx_resistance": "₹XX,XXX",
    "mcx_support": "₹XX,XXX"
  },
  "takeaway": {
    "bias": "Bullish or Bearish or Neutral",
    "strategy": "One sentence actionable strategy for traders.",
    "short_term": "One sentence view for next 1-2 weeks.",
    "medium_term": "One sentence view for next 1-3 months."
  }
}

Base all values on the news provided and current macro context. Be specific with price levels for """ + commodity_name + """ as of today."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1200,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = message.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())

def run_analysis():
    global latest_results
    print("[" + datetime.now().strftime("%d %b %Y, %I:%M %p") + "] Running analysis...")
    all_articles = fetch_all_articles()
    news = filter_by_commodity(all_articles)
    results = {}
    for commodity, articles in news.items():
        print("Analysing " + commodity + " (" + str(len(articles)) + " articles)...")
        try:
            if articles:
                analysis = analyse_commodity(commodity, articles)
            else:
                analysis = {
                    "briefing": "Not enough news to generate analysis.",
                    "sentiment": "NEUTRAL",
                    "drivers": {"up": [], "down": []},
                    "levels": {"resistance": "—", "support": "—", "mcx_resistance": "—", "mcx_support": "—"},
                    "takeaway": {"bias": "Neutral", "strategy": "—", "short_term": "—", "medium_term": "—"}
                }
        except Exception as e:
            print("Error analysing " + commodity + ": " + str(e))
            analysis = {
                "briefing": "Error generating analysis: " + str(e),
                "sentiment": "NEUTRAL",
                "drivers": {"up": [], "down": []},
                "levels": {"resistance": "—", "support": "—", "mcx_resistance": "—", "mcx_support": "—"},
                "takeaway": {"bias": "Neutral", "strategy": "—", "short_term": "—", "medium_term": "—"}
            }
        priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        sorted_articles = sorted(articles, key=lambda a: priority.get(a.get("impact", "LOW"), 2))
        results[commodity] = {
            "analysis":  analysis,
            "articles":  sorted_articles[:15],
            "timestamp": datetime.now().strftime("%d %b %Y, %I:%M %p"),
            "count":     len(articles),
        }
    latest_results = results
    with open("results.json", "w") as f:
        json.dump(results, f)
    print("Done. Next run: 09:00, 15:00, 21:00, 23:00 IST.")

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

@app.route("/prices")
def get_prices():
    symbols = {
        "Gold": "GC=F", "Silver": "SI=F",
        "Crude Oil": "CL=F", "Copper": "HG=F", "Natural Gas": "NG=F"
    }
    prices = {}
    for name, sym in symbols.items():
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/" + sym + "?interval=1d&range=2d"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=5) as r:
                d = json.loads(r.read())
            meta = d["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev = meta["previousClose"]
            prices[name] = {"price": price, "change": ((price - prev) / prev) * 100}
        except:
            prices[name] = {"price": None, "change": None}
    response = jsonify(prices)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")

if __name__ == "__main__":
    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY not set.")
    try:
        with open("results.json", "r") as f:
            latest_results = json.load(f)
        print("Loaded cached results.")
    except:
        pass
    run_analysis()
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    print("Starting Commodex on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, use_reloader=False)