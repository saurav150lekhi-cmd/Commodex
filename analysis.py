import feedparser
import anthropic
import json
import time
import threading
import schedule
import os
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from flask import Flask, jsonify, send_from_directory
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
EIA_API_KEY = os.environ.get("EIA_API_KEY", "")

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("commodex.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

app = Flask(__name__)
latest_results = {}
analysis_status = {"running": False, "last_run": None, "last_error": None}

NEWS_SOURCES = [
    "https://oilprice.com/rss/main",
    "https://www.mining.com/feed/",
    "https://feeds.reuters.com/reuters/commoditiesNews",
    "https://feeds.reuters.com/reuters/businessNews",
]

COMMODITIES = {
    "Gold":        ["gold price", "gold rate", "gold futures", "bullion", "xau", "gold rises", "gold falls", "gold hits", "gold climbs"],
    "Crude Oil":   ["crude oil", "wti", "brent", "west texas", "opec", "petroleum price", "oil price", "oil rises", "oil falls"],
    "Silver":      ["silver price", "silver rate", "silver futures", "xag", "silver", "comex silver", "lme silver", "silver demand", "silver supply", "silver output", "silver mine", "silver rally", "silver falls", "silver rises", "precious metal", "silver etf", "silver bullion"],
    "Copper":      ["copper price", "copper futures", "lme copper", "comex copper", "copper", "hg futures", "base metal", "industrial metal", "red metal", "copper demand", "copper supply", "copper output", "copper mine", "copper rally", "copper falls", "copper rises", "copper cathode", "copper inventories"],
    "Natural Gas": ["natural gas", "natgas", "lng", "henry hub", "gas price", "natural gas price", "gas futures", "gas demand", "gas supply", "gas inventories", "gas storage", "nymex gas", "europe gas", "us gas", "gas rally", "gas falls", "ttf gas", "gas exports"],
}

GOOGLE_SEARCHES = {
    "Gold":        ["gold site:bloomberg.com", "gold site:ft.com", "gold site:argusmedia.com", "gold site:kitco.com"],
    "Crude Oil":   ["crude oil site:bloomberg.com", "crude oil site:ft.com", "crude oil site:argusmedia.com"],
    "Silver":      ["silver site:bloomberg.com", "silver site:ft.com", "silver site:kitco.com", "gold silver site:kitco.com"],
    "Copper":      ["copper site:bloomberg.com", "copper site:ft.com", "copper site:argusmedia.com"],
    "Natural Gas": ["natural gas site:bloomberg.com", "natural gas site:ft.com", "natural gas site:argusmedia.com"],
}

HIGH_IMPACT_KEYWORDS = [
    "fed", "federal reserve", "rate decision", "opec", "sanctions", "war", "conflict",
    "inflation", "cpi", "gdp", "recession", "rate hike", "rate cut", "central bank",
    "rbi", "fomc", "powell", "inventory", "supply cut", "demand surge", "crash", "rally",
    "all-time high", "record", "collapse", "shortage", "surplus"
]

MEDIUM_IMPACT_KEYWORDS = [
    "forecast", "outlook", "estimate", "analyst", "report", "weekly", "monthly",
    "import", "export", "trade", "dollar", "usd", "nymex", "comex"
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
                url = "https://news.google.com/rss/search?q=" + search.replace(" ", "+") + "&hl=en-US&gl=US&ceid=US:en"
                feed = feedparser.parse(url)
                if "bloomberg.com" in search:
                    label = "Bloomberg"
                elif "reuters.com" in search:
                    label = "Reuters"
                elif "ft.com" in search:
                    label = "Financial Times"
                elif "argusmedia.com" in search:
                    label = "Argus Media"
                elif "kitco.com" in search:
                    label = "Kitco"
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

    prompt = """You are a senior commodity analyst at a global hedge fund. Analyse the data below for """ + commodity_name + """ using the 10-step pipeline, then return only the final JSON from Step 10. All prices and levels are in USD only.

TIME: """ + datetime.now().strftime("%d %b %Y, %I:%M %p") + """ UTC

LIVE MARKET DATA:
""" + macro_context + """

RECENT NEWS HEADLINES:
""" + headlines + """

---

STEP 1 - DATA INGESTION: Filter only information relevant to """ + commodity_name + """ from the inputs above.

STEP 2 - EVENT EXTRACTION: Group relevant articles into 3-5 distinct market events, ignoring duplicate coverage of the same event.

STEP 3 - RELEVANCE FILTERING: Score each event for relevance to """ + commodity_name + """ price action. Discard low relevance events.

STEP 4 - DRIVER IDENTIFICATION: Identify 3-5 primary market drivers across these categories: Macroeconomic, Supply, Demand, Geopolitical, Financial Positioning, Currency.

STEP 5 - PRICE ACTION CONTEXT: Determine if current price movement confirms, contradicts, or is unclear relative to the identified drivers. Never invent explanations if evidence is weak.

STEP 6 - MARKET SUMMARY: Write a concise professional analyst narrative of what is currently happening in this market, covering the global market situation, macro drivers, supply/demand dynamics, and regional impact across US, Europe, and China.

STEP 7 - TRADER TAKEAWAYS: Generate observational insights for three timeframes: Intraday, Next Few Days, Next Few Weeks. Never give buy/sell recommendations.

STEP 8 - DRIVER CONFIDENCE: Assign HIGH, MEDIUM or LOW confidence based on number of supporting events, source consistency, and price alignment.

STEP 9 - MARKET NARRATIVE TRACKING: Identify the dominant narrative (e.g. inflation hedge, supply disruption) and whether it is Strengthening, Stable, Weakening or Shifting.

STEP 10 - STRUCTURED OUTPUT: Return ONLY the following valid JSON. All price levels in USD. No markdown, no explanation, no text before or after the JSON:

{
  "market_summary": "concise professional narrative covering global market situation, macro drivers, supply/demand, and regional impact across US, Europe, and China",
  "sentiment": "STRONG_BULLISH or BULLISH or NEUTRAL or BEARISH or STRONG_BEARISH",
  "drivers": {
    "up": ["driver 1", "driver 2", "driver 3"],
    "down": ["driver 1", "driver 2", "driver 3"]
  },
  "price_action_context": "confirms or contradicts or unclear - one sentence explanation",
  "trader_takeaways": {
    "intraday": "what to monitor today",
    "next_few_days": "catalysts to watch this week",
    "next_few_weeks": "structural themes for medium term"
  },
  "confidence": "HIGH or MEDIUM or LOW",
  "dominant_narrative": {
    "theme": "name of the dominant narrative e.g. inflation hedge",
    "status": "Strengthening or Stable or Weakening or Shifting"
  },
  "takeaway": {
    "bias": "Bullish or Bearish or Neutral",
    "strategy": "one sentence observational insight for traders - no buy/sell recommendation",
    "short_term": "one sentence view for next 1-2 weeks",
    "medium_term": "one sentence view for next 1-3 months"
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
    global latest_results, analysis_status
    analysis_status["running"] = True
    analysis_status["last_error"] = None
    log.info("Analysis cycle started.")

    try:
        log.info("Fetching external data...")
        eia        = fetch_eia_data()
        cftc       = fetch_cftc_data()
        imf        = fetch_imf_data()
        worldbank  = fetch_worldbank_data()
        log.info("External data fetched.")

        all_articles = fetch_all_articles()
        news = filter_by_commodity(all_articles)

        results = {}
        for commodity, articles in news.items():
            log.info("Analysing %s (%d articles)...", commodity, len(articles))
            macro_context = build_macro_context(eia, cftc, imf, worldbank, commodity)
            try:
                if articles:
                    analysis = analyse_commodity(commodity, articles, macro_context)
                else:
                    analysis = {
                        "market_summary": "Not enough news to generate analysis.",
                        "sentiment": "NEUTRAL",
                        "drivers": {"up": [], "down": []},
                        "price_action_context": "—",
                        "trader_takeaways": {"intraday": "—", "next_few_days": "—", "next_few_weeks": "—"},
                        "confidence": "LOW",
                        "dominant_narrative": {"theme": "—", "status": "—"},
                        "takeaway": {"bias": "Neutral", "strategy": "—", "short_term": "—", "medium_term": "—"}
                    }
            except Exception as e:
                log.error("Error analysing %s: %s", commodity, e)
                analysis_status["last_error"] = str(e)
                analysis = {
                    "market_summary": "Error generating analysis: " + str(e),
                    "sentiment": "NEUTRAL",
                    "drivers": {"up": [], "down": []},
                    "price_action_context": "—",
                    "trader_takeaways": {"intraday": "—", "next_few_days": "—", "next_few_weeks": "—"},
                    "confidence": "LOW",
                    "dominant_narrative": {"theme": "—", "status": "—"},
                    "takeaway": {"bias": "Neutral", "strategy": "—", "short_term": "—", "medium_term": "—"}
                }
            priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
            sorted_articles = sorted(articles, key=lambda a: priority.get(a.get("impact", "LOW"), 2))
            results[commodity] = {
                "analysis":  analysis,
                "articles":  sorted_articles[:15],
                "timestamp": datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC"),
                "count":     len(articles),
            }
        latest_results = results
        with open("results.json", "w") as f:
            json.dump(results, f)
        analysis_status["last_run"] = datetime.now(timezone.utc).isoformat()
        log.info("Analysis cycle complete.")
    finally:
        analysis_status["running"] = False

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

@app.route("/health")
def health():
    response = jsonify({
        "status": "ok",
        "analysis_running": analysis_status["running"],
        "last_run": analysis_status["last_run"],
        "last_error": analysis_status["last_error"],
        "commodities": list(latest_results.keys()),
    })
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
        log.warning("ANTHROPIC_API_KEY not set.")
    if not EIA_API_KEY:
        log.info("EIA_API_KEY not set — energy data will be skipped.")
    try:
        with open("results.json", "r") as f:
            latest_results = json.load(f)
        log.info("Loaded cached results from results.json.")
    except:
        pass
    run_analysis()
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    print("Starting Commodex on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, use_reloader=False)