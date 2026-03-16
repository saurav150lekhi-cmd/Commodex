import feedparser
import anthropic
import json
import time
import threading
import schedule
import os
import urllib.request
import urllib.parse
from datetime import datetime
from flask import Flask, jsonify, send_from_directory

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
EIA_API_KEY = os.environ.get("EIA_API_KEY", "")

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
    "Silver":      ["silver price", "silver rate", "silver futures", "xag", "silver", "comex silver", "lme silver", "mcx silver", "silver demand", "silver supply", "silver output", "silver mine", "silver rally", "silver falls", "silver rises", "precious metal", "silver etf", "silver bullion"],
    "Copper":      ["copper price", "copper futures", "lme copper", "comex copper", "copper", "hg futures", "base metal", "industrial metal", "red metal", "copper demand", "copper supply", "copper output", "copper mine", "copper rally", "copper falls", "copper rises", "mcx copper", "copper cathode", "copper inventories"],
    "Natural Gas": ["natural gas", "natgas", "lng", "henry hub", "gas price", "natural gas price", "gas futures", "gas demand", "gas supply", "gas inventories", "gas storage", "nymex gas", "mcx gas", "europe gas", "us gas", "gas rally", "gas falls", "ttf gas", "gas exports"],
}

GOOGLE_SEARCHES = {
    "Gold":        ["gold site:bloomberg.com", "gold site:reuters.com"],
    "Crude Oil":   ["crude oil site:bloomberg.com", "crude oil site:reuters.com"],
    "Silver":      ["silver site:bloomberg.com", "silver price site:reuters.com"],
    "Copper":      ["copper LME site:bloomberg.com", "copper price site:reuters.com"],
    "Natural Gas": ["natural gas site:bloomberg.com", "natural gas site:reuters.com"],
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

# ── Article scoring ────────────────────────────────────────────────────────────
def score_article(article):
    text = (article["title"] + " " + article["summary"]).lower()
    for kw in HIGH_IMPACT_KEYWORDS:
        if kw in text:
            return "HIGH"
    for kw in MEDIUM_IMPACT_KEYWORDS:
        if kw in text:
            return "MEDIUM"
    return "LOW"

# ── HTTP helper ────────────────────────────────────────────────────────────────
def fetch_json(url, headers=None, timeout=8):
    try:
        req = urllib.request.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except:
        return None

# ══════════════════════════════════════════════════════════════════════════════
# EXTERNAL DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. EIA — Energy inventory & production data ────────────────────────────────
def fetch_eia_data():
    result = {}
    if not EIA_API_KEY:
        return result
    series = {
        "crude_inventory":  "PET.WCRSTUS1.W",
        "crude_production": "PET.WCRFPUS2.W",
        "natgas_storage":   "NG.NW2_EPG0_SWO_R48_BCF.W",
    }
    for key, series_id in series.items():
        url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={EIA_API_KEY}&length=2"
        data = fetch_json(url)
        if data:
            rows = data.get("response", {}).get("data", [])
            if rows:
                result[key] = {
                    "latest": rows[0].get("value"),
                    "previous": rows[1].get("value") if len(rows) > 1 else None,
                    "unit": rows[0].get("unit", ""),
                    "period": rows[0].get("period", ""),
                }
    return result

# ── 2. CFTC — Commitment of Traders positioning ────────────────────────────────
def fetch_cftc_data():
    result = {}
    # CFTC publishes free JSON via CFTC public API (Socrata)
    # Futures only, most recent report
    commodity_codes = {
        "Gold":        "088691",
        "Silver":      "084691",
        "Crude Oil":   "067651",
        "Copper":      "085692",
        "Natural Gas": "023651",
    }
    for commodity, code in commodity_codes.items():
        url = f"https://publicreporting.cftc.gov/resource/jun7-fc8e.json?cftc_contract_market_code={code}&$limit=1&$order=report_date_as_yyyy_mm_dd+DESC"
        data = fetch_json(url)
        if data and len(data) > 0:
            row = data[0]
            try:
                nc_long  = int(row.get("noncomm_positions_long_all",  0))
                nc_short = int(row.get("noncomm_positions_short_all", 0))
                net = nc_long - nc_short
                result[commodity] = {
                    "noncommercial_long":  nc_long,
                    "noncommercial_short": nc_short,
                    "net_position":        net,
                    "positioning":         "NET LONG" if net > 0 else "NET SHORT",
                    "report_date":         row.get("report_date_as_yyyy_mm_dd", ""),
                }
            except:
                pass
    return result

# ── 3. IMF — Macro indicators ──────────────────────────────────────────────────
def fetch_imf_data():
    result = {}
    indicators = {
        "PCPIPCH":      "Inflation Rate (%)",
        "NGDP_RPCH":    "GDP Growth (%)",
        "LUR":          "Unemployment Rate (%)",
    }
    countries = ["US", "CN", "IN"]
    for ind_code, ind_name in indicators.items():
        url = f"https://www.imf.org/external/datamapper/api/v1/{ind_code}/US/CN/IN"
        data = fetch_json(url)
        if data:
            values = data.get("values", {}).get(ind_code, {})
            result[ind_name] = {}
            for country in countries:
                country_data = values.get(country, {})
                if country_data:
                    latest_year = max(country_data.keys())
                    result[ind_name][country] = {
                        "value": country_data[latest_year],
                        "year":  latest_year,
                    }
    return result

# ── 4. World Bank — Additional macro data ─────────────────────────────────────
def fetch_worldbank_data():
    result = {}
    indicators = {
        "FP.CPI.TOTL.ZG": "CPI Inflation",
        "NY.GDP.MKTP.KD.ZG": "GDP Growth",
    }
    for code, name in indicators.items():
        url = f"https://api.worldbank.org/v2/country/US/indicator/{code}?format=json&mrv=1"
        data = fetch_json(url)
        if data and len(data) > 1 and data[1]:
            row = data[1][0]
            if row.get("value"):
                result[name] = {
                    "value":  round(row["value"], 2),
                    "period": row.get("date", ""),
                }
    return result

# ── 5. Live prices via Yahoo Finance (server-side) ────────────────────────────
def fetch_live_prices():
    symbols = {
        "Gold":        "GC=F",
        "Silver":      "SI=F",
        "Crude Oil":   "CL=F",
        "Copper":      "HG=F",
        "Natural Gas": "NG=F",
    }
    prices = {}
    for name, sym in symbols.items():
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=2d"
        data = fetch_json(url)
        if data:
            try:
                meta  = data["chart"]["result"][0]["meta"]
                price = meta["regularMarketPrice"]
                prev  = meta["previousClose"]
                prices[name] = {
                    "price":  price,
                    "change": ((price - prev) / prev) * 100,
                }
            except:
                prices[name] = {"price": None, "change": None}
        else:
            prices[name] = {"price": None, "change": None}
    return prices

# ── Build macro context string for Claude ─────────────────────────────────────
def build_macro_context(eia, cftc, imf, worldbank, commodity_name):
    lines = []

    # IMF data
    if imf:
        lines.append("MACRO INDICATORS (IMF):")
        for ind_name, countries in imf.items():
            for country, val in countries.items():
                lines.append(f"  {country} {ind_name}: {val['value']}% ({val['year']})")

    # World Bank
    if worldbank:
        lines.append("WORLD BANK (US):")
        for name, val in worldbank.items():
            lines.append(f"  {name}: {val['value']}% ({val['period']})")

    # EIA
    if eia and commodity_name in ["Crude Oil", "Natural Gas"]:
        lines.append("EIA SUPPLY DATA:")
        if "crude_inventory" in eia and commodity_name == "Crude Oil":
            d = eia["crude_inventory"]
            chg = ""
            if d.get("latest") and d.get("previous"):
                diff = float(d["latest"]) - float(d["previous"])
                chg = f" (change: {diff:+.0f})"
            lines.append(f"  US Crude Inventory: {d['latest']} {d['unit']}{chg} ({d['period']})")
        if "crude_production" in eia and commodity_name == "Crude Oil":
            d = eia["crude_production"]
            lines.append(f"  US Crude Production: {d['latest']} {d['unit']} ({d['period']})")
        if "natgas_storage" in eia and commodity_name == "Natural Gas":
            d = eia["natgas_storage"]
            chg = ""
            if d.get("latest") and d.get("previous"):
                diff = float(d["latest"]) - float(d["previous"])
                chg = f" (change: {diff:+.0f})"
            lines.append(f"  US NatGas Storage: {d['latest']} {d['unit']}{chg} ({d['period']})")

    # CFTC
    if cftc and commodity_name in cftc:
        d = cftc[commodity_name]
        lines.append("CFTC POSITIONING (Non-Commercial):")
        lines.append(f"  Long: {d['noncommercial_long']:,} | Short: {d['noncommercial_short']:,} | Net: {d['net_position']:+,} ({d['positioning']}) as of {d['report_date']}")

    return "\n".join(lines) if lines else "No external data available."

# ══════════════════════════════════════════════════════════════════════════════
# NEWS FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

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

# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

def analyse_commodity(commodity_name, articles, macro_context):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    headlines = "\n".join(
        "- " + a["title"] + " (" + a["source"] + ")"
        for a in articles[:15]
    ) or "No news available."

    prompt = """You are a senior commodity analyst and trading strategist for a hedge fund.

COMMODITY: """ + commodity_name + """
TIME: """ + datetime.now().strftime("%d %b %Y, %I:%M %p") + """ IST

LIVE MARKET DATA:
""" + macro_context + """

RECENT NEWS:
""" + headlines + """

Using BOTH the market data and news above, return ONLY a valid JSON object with exactly this structure. No markdown, no explanation, just the JSON:

{
  "briefing": "3-4 paragraph plain text investment briefing covering market situation, macro drivers, supply/demand, India angle. Reference specific data points from the market data provided. No markdown, no bullet points.",
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
}"""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = message.content[0].text.strip()
    # Strip markdown code fences
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                raw = part
                break
    # Extract first JSON object by brace matching
    start = raw.find("{")
    if start != -1:
        depth = 0
        for i, ch in enumerate(raw[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    raw = raw[start:i+1]
                    break
    return json.loads(raw.strip())

# ══════════════════════════════════════════════════════════════════════════════
# MAIN ANALYSIS RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_analysis():
    global latest_results
    print("[" + datetime.now().strftime("%d %b %Y, %I:%M %p") + "] Running analysis...")

    # Fetch all external data once (shared across commodities)
    print("Fetching external data...")
    eia        = fetch_eia_data()
    cftc       = fetch_cftc_data()
    imf        = fetch_imf_data()
    worldbank  = fetch_worldbank_data()
    print("External data fetched.")

    all_articles = fetch_all_articles()
    news = filter_by_commodity(all_articles)

    results = {}
    for commodity, articles in news.items():
        print("Analysing " + commodity + " (" + str(len(articles)) + " articles)...")
        macro_context = build_macro_context(eia, cftc, imf, worldbank, commodity)
        try:
            if articles:
                analysis = analyse_commodity(commodity, articles, macro_context)
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

# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

def scheduler_loop():
    schedule.every().day.at("09:00").do(run_analysis)
    schedule.every().day.at("15:00").do(run_analysis)
    schedule.every().day.at("21:00").do(run_analysis)
    schedule.every().day.at("23:00").do(run_analysis)
    while True:
        schedule.run_pending()
        time.sleep(30)

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/data")
def get_data():
    response = jsonify(latest_results)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/prices")
def get_prices():
    prices = fetch_live_prices()
    response = jsonify(prices)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")

# ══════════════════════════════════════════════════════════════════════════════
# START
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY not set.")
    if not EIA_API_KEY:
        print("NOTE: EIA_API_KEY not set — energy data will be skipped. Get free key at eia.gov/opendata")
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