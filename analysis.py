import feedparser
import anthropic
import json
import re
import time
import calendar
import threading
import schedule
import os
import logging
import hashlib
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, send_from_directory, request
from flask_jwt_extended import JWTManager, jwt_required
from dotenv import load_dotenv
from db import db
from models import User, AnalysisRun, UserAlert, NewsArticle, MarketSignal
from signal_engine import get_signal_candidates
from agent import run_agent
from auth import auth_bp
from admin import admin_bp
from email_utils import send_alert_email, send_analysis_notification_email

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
EIA_API_KEY       = os.environ.get("EIA_API_KEY", "")
FRED_API_KEY      = os.environ.get("FRED_API_KEY", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "sqlite:///commodex.db")
JWT_SECRET_KEY    = os.environ.get("JWT_SECRET_KEY", "change-me-in-production")

# Fix Render's postgres:// URI (SQLAlchemy requires postgresql://)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

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

# ── Flask app ──────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"]        = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JWT_SECRET_KEY"]                 = JWT_SECRET_KEY
app.config["JWT_ACCESS_TOKEN_EXPIRES"]       = timedelta(days=30)
app.config["JWT_REFRESH_TOKEN_EXPIRES"]      = timedelta(days=90)

db.init_app(app)
JWTManager(app)
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)

latest_results       = {}
analysis_status      = {"running": False, "last_run": None, "last_error": None}
_breaking_commodities: set = set()   # commodities whose signals triggered an immediate run

# Runs whether started via gunicorn or python directly
with app.app_context():
    try:
        db.create_all()
        log.info("Database tables ready.")
    except Exception as e:
        log.error("Database init failed: %s", e)

# Safe migrations for columns added after initial deploy
from sqlalchemy import text as _sa_text
with app.app_context():
    _migrations = [
        "ALTER TABLE market_signals ADD COLUMN signal_strength INTEGER DEFAULT 0",
        "ALTER TABLE market_signals ADD COLUMN so_what TEXT",
        "ALTER TABLE market_signals ADD COLUMN triggered_analysis BOOLEAN DEFAULT 0",
    ]
    for _sql in _migrations:
        try:
            db.session.execute(_sa_text(_sql))
            db.session.commit()
        except Exception:
            db.session.rollback()

NEWS_SOURCES = [
    # ── Tier 1 — Confirmed working feeds ───────────────────────────────────────
    "https://oilprice.com/rss/main",                                # OilPrice (15 entries)
    "https://www.cnbc.com/id/23103686/device/rss/rss.html",         # CNBC Markets (18 entries)
    "https://feeds.content.dowjones.io/public/rss/mw_topstories",  # MarketWatch (10 entries)
    "https://www.investing.com/rss/news_25.rss",                    # Investing.com (10 entries)
    "https://www.rigzone.com/news/rss/rigzone_latest.aspx",         # Rigzone oil & gas (20 entries)
    "https://www.hellenicshippingnews.com/feed/",                   # Hellenic Shipping energy/metals (20)
    "https://www.naturalgasintel.com/feed/",                        # Natural Gas Intelligence (10)
    "https://seekingalpha.com/tag/commodities.xml",                 # Seeking Alpha commodities (20)
    # ── Tier 2 — Additional reliable feeds ────────────────────────────────────
    "https://www.eia.gov/rss/news.xml",                             # EIA official energy news
    "https://www.iea.org/news/rss.xml",                             # IEA energy news
    "https://www.resourceworld.com/feed/",                          # Resource World mining/metals
    "https://goldsilver.com/feed/",                                 # Gold & Silver news
    "https://www.silverseek.com/rss.xml",                           # Silver news
    "https://www.24hgold.com/rss/RSSenglish.ashx",                  # 24h Gold news
    "https://www.fxstreet.com/rss/news",                            # FX Street commodities/macro
    "https://www.mining.com/feed/",                                 # Mining.com (retry — intermittent)
    # ── Tier 3 — Additional commodity/macro feeds ─────────────────────────────
    "https://www.kitco.com/rss/",                                   # Kitco precious metals
    "https://feeds.reuters.com/reuters/businessNews",               # Reuters Business
    "https://feeds.reuters.com/reuters/companyNews",                # Reuters Company
    "https://www.platts.com/rss/feeds/oil",                        # S&P Global Platts Oil
    "https://www.argusmedia.com/rss/feed/commodities",             # Argus Media commodities
    "https://www.spglobal.com/commodityinsights/en/rss",           # S&P Commodity Insights
    "https://www.metalsbulletin.com/rss/",                         # Fastmarkets Metal Bulletin
    "https://www.world-grain.com/rss/",                            # World Grain (macro supply)
    "https://www.bloomberg.com/feeds/podcast/etf-iq.xml",         # Bloomberg markets
    "https://finance.yahoo.com/rss/topfinstories",                 # Yahoo Finance top
    "https://www.ft.com/commodities?format=rss",                   # FT Commodities
    "https://www.theice.com/rss/news",                             # ICE exchange news
    "https://www.lme.com/en/news-and-events/news/rss",            # LME news
    "https://www.cmegroup.com/rss/cme-group-news.xml",            # CME Group news
    # ── Tier 4 — Tanker / shipping / trade flow ────────────────────────────────
    "https://gcaptain.com/feed/",                                  # gCaptain maritime (tankers, LNG)
    "https://splash247.com/feed/",                                 # Splash 247 shipping intelligence
    "https://www.tradewindsnews.com/rss",                          # TradeWinds tanker market news
    "https://www.offshore-technology.com/feed/",                   # Offshore oil & gas operations
    "https://www.lngworldnews.com/feed/",                          # LNG shipping & trade
    "https://www.hellenicshippingnews.com/feed/",                  # already have but keep (energy focus)
]

COMMODITIES = {
    "Gold":        ["gold price", "gold rate", "gold futures", "bullion", "xau", "gold rises", "gold falls", "gold hits", "gold climbs"],
    "Silver":      ["silver price", "silver rate", "silver futures", "xag", "silver", "comex silver", "lme silver", "silver demand", "silver supply", "silver output", "silver mine", "silver rally", "silver falls", "silver rises", "precious metal", "silver etf", "silver bullion"],
    "Crude Oil":   ["crude oil", "wti", "brent", "west texas", "opec", "petroleum price", "oil price", "oil rises", "oil falls", "vlcc", "supertanker", "tanker", "cushing", "crude imports", "crude exports", "floating storage", "oil tanker", "strait of hormuz", "persian gulf oil"],
    "Copper":      ["copper price", "copper futures", "lme copper", "comex copper", "copper", "hg futures", "base metal", "industrial metal", "red metal", "copper demand", "copper supply", "copper output", "copper mine", "copper rally", "copper falls", "copper rises", "copper cathode", "copper inventories"],
    "Natural Gas": ["natural gas", "natgas", "lng", "henry hub", "gas price", "natural gas price", "gas futures", "gas demand", "gas supply", "gas inventories", "gas storage", "nymex gas", "europe gas", "us gas", "gas rally", "gas falls", "ttf gas", "gas exports", "lng tanker", "lng carrier", "lng terminal", "lng exports", "lng imports", "sabine pass", "freeport lng"],
}

GOOGLE_SEARCHES = {
    "Gold": [
        "gold site:bloomberg.com",
        "gold site:ft.com",
        "gold site:argusmedia.com",
        "gold site:kitco.com",
        "gold site:gold.org",
        "gold site:seekingalpha.com",
        "gold site:spglobal.com",
        "gold site:bullionvault.com",
    ],
    "Crude Oil": [
        "crude oil site:bloomberg.com",
        "crude oil site:ft.com",
        "crude oil site:argusmedia.com",
        "crude oil site:rigzone.com",
        "crude oil site:hartenergy.com",
        "crude oil site:spglobal.com",
        "crude oil site:seekingalpha.com",
        "opec site:reuters.com",
    ],
    "Silver": [
        "silver site:bloomberg.com",
        "silver site:ft.com",
        "silver site:kitco.com",
        "silver site:silverinstitute.org",
        "silver site:seekingalpha.com",
        "silver site:spglobal.com",
        "silver site:bullionvault.com",
    ],
    "Copper": [
        "copper site:bloomberg.com",
        "copper site:ft.com",
        "copper site:argusmedia.com",
        "copper site:mining.com",
        "copper site:fastmarkets.com",
        "copper site:spglobal.com",
        "copper lme site:reuters.com",
    ],
    "Natural Gas": [
        "natural gas site:bloomberg.com",
        "natural gas site:ft.com",
        "natural gas site:argusmedia.com",
        "natural gas site:rigzone.com",
        "natural gas site:naturalgasintel.com",
        "natural gas site:hartenergy.com",
        "natural gas site:spglobal.com",
        "lng site:reuters.com",
    ],
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
        "crude_inventory":     "PET.WCRSTUS1.W",
        "crude_production":    "PET.WCRFPUS2.W",
        "natgas_storage":      "NG.NW2_EPG0_SWO_R48_BCF.W",
        "gasoline_inventory":  "PET.WGTSTUS1.W",
        "distillate_inventory":"PET.WDISTUS1.W",
        "refinery_utilization":"PET.WPULEUS2.W",
        # Tanker / trade flow proxies
        "cushing_stocks":      "PET.WCSSTUS1.W",    # Cushing OK hub stocks (WTI pricing)
        "crude_imports_opec":  "PET.WCRIMS2.W",     # OPEC imports = tanker flow signal
        "crude_imports_total": "PET.WCRIMPUS2.W",   # Total US crude imports
        "crude_exports":       "PET.WCREXUS2.W",    # US crude exports
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
        "Gold":        "gc.f",
        "Silver":      "si.f",
        "Crude Oil":   "cl.f",
        "Copper":      "hg.f",
        "Natural Gas": "ng.f",
    }
    prices = {}
    for name, sym in symbols.items():
        try:
            url = f"https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                lines = r.read().decode().strip().split("\n")
            if len(lines) >= 2:
                row   = lines[-1].split(",")
                close = float(row[6])
                open_ = float(row[3])
                prices[name] = {
                    "price":  close,
                    "change": ((close - open_) / open_) * 100,
                }
            else:
                prices[name] = {"price": None, "change": None}
        except:
            prices[name] = {"price": None, "change": None}
    return prices


# ── 6. FRED — DXY, 10Y yield, Industrial Production, CPI ─────────────────────
def fetch_fred_data():
    if not FRED_API_KEY:
        return {}
    series = {
        "dxy":        ("DTWEXBGS",         "US Dollar Index (DXY)"),
        "us10y":      ("DGS10",            "US 10Y Treasury Yield (%)"),
        "us2y":       ("DGS2",             "US 2Y Treasury Yield (%)"),
        "yield_curve":("T10Y2Y",           "US 10Y-2Y Yield Spread (%)"),
        "fedfunds":   ("FEDFUNDS",         "Fed Funds Rate (%)"),
        "indpro":     ("INDPRO",           "US Industrial Production Index"),
        "cpi":        ("CPIAUCSL",         "US CPI (Inflation Index)"),
        "m2":         ("M2SL",             "US M2 Money Supply (Billions USD)"),
        "vix":        ("VIXCLS",           "VIX Volatility Index"),
        "eurusd":     ("DEXUSEU",          "EUR/USD Exchange Rate"),
        "cnyusd":     ("DEXCHUS",          "CNY/USD Exchange Rate"),
        "gold_lbma":  ("GOLDAMGBD228NLBM", "Gold LBMA Price (USD/troy oz)"),
        "wti_spot":   ("DCOILWTICO",       "WTI Crude Oil Spot (USD/bbl)"),
        "natgas_spot":("DHHNGSP",          "Henry Hub Nat Gas Spot (USD/mmBtu)"),
    }
    result = {}
    for key, (series_id, label) in series.items():
        url  = (f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}&api_key={FRED_API_KEY}"
                f"&limit=2&sort_order=desc&file_type=json")
        data = fetch_json(url)
        if data and data.get("observations"):
            obs = [o for o in data["observations"] if o.get("value") not in (".", None, "")]
            if obs:
                try:
                    latest   = float(obs[0]["value"])
                    previous = float(obs[1]["value"]) if len(obs) > 1 else None
                    chg      = round(latest - previous, 3) if previous else None
                    result[key] = {
                        "label":    label,
                        "value":    latest,
                        "change":   chg,
                        "date":     obs[0]["date"],
                    }
                except:
                    pass
    return result


# ── 7. LME copper warehouse stocks — scraped from LME website ─────────────────
def fetch_lme_copper_stocks():
    try:
        req = urllib.request.Request(
            "https://www.lme.com/en/metals/non-ferrous/lme-copper/",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        # Try several patterns LME uses on their page
        for pattern in [
            r'[Ww]arehouse\s+[Ss]tocks?[^0-9]*([\d,]+)\s*[Tt]onnes?',
            r'([\d,]+)\s*[Tt]onnes?\s*(?:in\s+)?(?:LME\s+)?warehouses?',
            r'"onWarrant"\s*:\s*"?([\d,]+)"?',
        ]:
            m = re.search(pattern, html)
            if m:
                return {"value": int(m.group(1).replace(",", "")), "unit": "tonnes"}
    except:
        pass
    return None


# ── 8. Baltic Dry Index — Yahoo Finance with Business Insider fallback ─────────
def fetch_bdi():
    # Primary: Yahoo Finance
    data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/%5EBDI?interval=1d&range=2d")
    if data:
        try:
            meta  = data["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else 0
            return {"value": price, "change": chg}
        except:
            pass
    # Fallback: Business Insider HTML scrape
    try:
        req = urllib.request.Request(
            "https://markets.businessinsider.com/commodities/baltic-dry-index",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        m = re.search(r'"price"\s*:\s*([\d,]+\.?\d*)', html)
        if m:
            return {"value": float(m.group(1).replace(",", "")), "change": None}
    except:
        pass
    return None


# ── Build macro context string for Claude ─────────────────────────────────────
def build_macro_context(eia, cftc, imf, worldbank, fred, lme_copper, bdi, commodity_name):
    lines = []

    # FRED macro indicators — grouped by relevance
    if fred:
        # Rate environment — all commodities
        rate_keys = ["fedfunds", "us10y", "us2y", "yield_curve", "dxy"]
        rate_lines = []
        for k in rate_keys:
            if k in fred:
                d = fred[k]
                chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
                rate_lines.append(f"  {d['label']}: {d['value']}{chg_str} ({d['date']})")
        if rate_lines:
            lines.append("RATE & DOLLAR ENVIRONMENT:")
            lines.extend(rate_lines)

        # Risk sentiment — all commodities
        if "vix" in fred:
            d = fred["vix"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            vix_val = d['value']
            vix_regime = "elevated fear" if vix_val > 25 else "moderate" if vix_val > 18 else "complacency"
            lines.append(f"RISK SENTIMENT: VIX {vix_val}{chg_str} — {vix_regime} ({d['date']})")

        # Inflation / money supply — gold & silver signal
        if commodity_name in ("Gold", "Silver"):
            for k in ["cpi", "m2"]:
                if k in fred:
                    d = fred[k]
                    chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
                    lines.append(f"INFLATION/MONEY: {d['label']}: {d['value']}{chg_str} ({d['date']})")

        # Currency — gold, copper, silver
        if commodity_name in ("Gold", "Silver", "Copper"):
            for k in ["eurusd", "cnyusd"]:
                if k in fred:
                    d = fred[k]
                    chg_str = f" (chg: {d['change']:+.4f})" if d.get("change") is not None else ""
                    lines.append(f"CURRENCY: {d['label']}: {d['value']}{chg_str} ({d['date']})")

        # Direct commodity spot prices from FRED (cross-reference)
        if commodity_name == "Gold" and "gold_lbma" in fred:
            d = fred["gold_lbma"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            lines.append(f"GOLD LBMA FIX: {d['label']}: {d['value']}{chg_str} ({d['date']})")
        if commodity_name == "Crude Oil" and "wti_spot" in fred:
            d = fred["wti_spot"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            lines.append(f"WTI SPOT (FRED): {d['value']}{chg_str} ({d['date']})")
        if commodity_name == "Natural Gas" and "natgas_spot" in fred:
            d = fred["natgas_spot"]
            chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
            lines.append(f"HENRY HUB SPOT (FRED): {d['value']}{chg_str} ({d['date']})")

        # Industrial production — copper & crude
        if commodity_name in ("Copper", "Crude Oil") and "indpro" in fred:
            d = fred["indpro"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            lines.append(f"INDUSTRIAL PRODUCTION: {d['value']}{chg_str} ({d['date']})")

    # Baltic Dry Index
    if bdi:
        chg_str = f" ({bdi['change']:+.2f}%)" if bdi.get("change") is not None else ""
        lines.append(f"BALTIC DRY INDEX: {bdi['value']}{chg_str}")

    # LME Copper warehouse stocks
    if lme_copper and commodity_name in ("Copper", "Gold", "Silver"):
        lines.append(f"LME COPPER WAREHOUSE STOCKS: {lme_copper['value']:,} {lme_copper['unit']}")

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
        if "gasoline_inventory" in eia and commodity_name == "Crude Oil":
            d = eia["gasoline_inventory"]
            chg = ""
            if d.get("latest") and d.get("previous"):
                diff = float(d["latest"]) - float(d["previous"])
                chg = f" (change: {diff:+.0f})"
            lines.append(f"  US Gasoline Inventory: {d['latest']} {d['unit']}{chg} ({d['period']})")
        if "distillate_inventory" in eia and commodity_name == "Crude Oil":
            d = eia["distillate_inventory"]
            chg = ""
            if d.get("latest") and d.get("previous"):
                diff = float(d["latest"]) - float(d["previous"])
                chg = f" (change: {diff:+.0f})"
            lines.append(f"  US Distillate Inventory: {d['latest']} {d['unit']}{chg} ({d['period']})")
        if "refinery_utilization" in eia and commodity_name == "Crude Oil":
            d = eia["refinery_utilization"]
            lines.append(f"  US Refinery Utilization: {d['latest']} {d['unit']} ({d['period']})")
        if "cushing_stocks" in eia and commodity_name == "Crude Oil":
            d = eia["cushing_stocks"]
            chg = ""
            if d.get("latest") and d.get("previous"):
                diff = float(d["latest"]) - float(d["previous"])
                chg = f" (change: {diff:+.0f})"
            lines.append(f"  Cushing OK Crude Stocks: {d['latest']} {d['unit']}{chg} ({d['period']})")
        if commodity_name == "Crude Oil":
            if "crude_imports_opec" in eia:
                d = eia["crude_imports_opec"]
                chg = ""
                if d.get("latest") and d.get("previous"):
                    diff = float(d["latest"]) - float(d["previous"])
                    chg = f" (change: {diff:+.0f}) — {'tanker flow up' if diff > 0 else 'tanker flow down'}"
                lines.append(f"  OPEC Crude Imports (tanker proxy): {d['latest']} {d['unit']}{chg} ({d['period']})")
            if "crude_imports_total" in eia:
                d = eia["crude_imports_total"]
                lines.append(f"  Total US Crude Imports: {d['latest']} {d['unit']} ({d['period']})")
            if "crude_exports" in eia:
                d = eia["crude_exports"]
                lines.append(f"  US Crude Exports: {d['latest']} {d['unit']} ({d['period']})")
    # CFTC
    if cftc and commodity_name in cftc:
        d = cftc[commodity_name]
        lines.append("CFTC POSITIONING (Non-Commercial):")
        lines.append(f"  Long: {d['noncommercial_long']:,} | Short: {d['noncommercial_short']:,} | Net: {d['net_position']:+,} ({d['positioning']}) as of {d['report_date']}")

    return "\n".join(lines) if lines else "No external data available."

# ══════════════════════════════════════════════════════════════════════════════
# NEWS FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def _pub_iso(entry):
    """Return a sortable ISO UTC string from a feedparser entry's publish date."""
    parsed = entry.get("published_parsed")
    if parsed:
        try:
            return datetime.utcfromtimestamp(calendar.timegm(parsed)).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass
    return entry.get("published", "")


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
                    "published": _pub_iso(entry),
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
                elif "gold.org" in search:
                    label = "World Gold Council"
                elif "rigzone.com" in search:
                    label = "Rigzone"
                elif "mining.com" in search:
                    label = "Mining.com"
                elif "seekingalpha.com" in search:
                    label = "Seeking Alpha"
                elif "spglobal.com" in search:
                    label = "S&P Global"
                elif "silverinstitute.org" in search:
                    label = "Silver Institute"
                elif "bullionvault.com" in search:
                    label = "BullionVault"
                elif "fastmarkets.com" in search:
                    label = "Fastmarkets"
                elif "hartenergy.com" in search:
                    label = "Hart Energy"
                elif "naturalgasintel.com" in search:
                    label = "Natural Gas Intelligence"
                else:
                    label = "Google News"
                for entry in feed.entries[:15]:
                    all_articles.append({
                        "title":     entry.get("title", "").strip(),
                        "summary":   entry.get("summary", "").strip(),
                        "url":       entry.get("link", ""),
                        "published": _pub_iso(entry),
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

def fetch_and_store_news():
    """Fetch fresh articles from RSS/Google News and persist to DB. No AI cost."""
    log.info("News fetch started...")
    try:
        all_articles = fetch_all_articles()
        by_commodity = filter_by_commodity(all_articles)
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        # Prune articles older than 7 days
        NewsArticle.query.filter(NewsArticle.fetched_at < cutoff).delete()
        db.session.commit()
        saved = 0
        new_by_commodity = {c: [] for c in COMMODITIES}
        for commodity, articles in by_commodity.items():
            for a in articles:
                key = (a.get("url") or a["title"]) + "|" + commodity
                url_hash = hashlib.md5(key.encode()).hexdigest()
                exists = db.session.query(
                    NewsArticle.query.filter_by(url_hash=url_hash, commodity=commodity).exists()
                ).scalar()
                if not exists:
                    db.session.add(NewsArticle(
                        commodity  = commodity,
                        title      = a["title"],
                        url        = a.get("url", ""),
                        summary    = a.get("summary", ""),
                        source     = a.get("source", ""),
                        published  = a.get("published", ""),
                        impact     = a.get("impact", score_article(a)),
                        fetched_at = now,
                        url_hash   = url_hash,
                    ))
                    new_by_commodity[commodity].append(a)
                    saved += 1
        db.session.commit()
        log.info("News fetch complete: %d new articles saved.", saved)
        # Run AI agent on newly saved articles (signal detection)
        if saved > 0 and ANTHROPIC_API_KEY:
            _process_signals(new_by_commodity)
    except Exception as e:
        log.error("News fetch failed: %s", e)
        try:
            db.session.rollback()
        except Exception:
            pass


def _process_signals(new_by_commodity: dict):
    """Run signal_engine + AI agent on newly fetched articles."""
    try:
        candidates = get_signal_candidates(new_by_commodity)
        if not candidates:
            return
        log.info("Agent: evaluating %d signal candidates...", len(candidates))
        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        signals = run_agent(client, candidates)
        if not signals:
            log.info("Agent: no confirmed signals this cycle.")
            return

        trigger_commodities: set[str] = set()
        for sig in signals:
            row = MarketSignal(
                commodity       = sig.get("commodity", ""),
                event           = sig.get("event", ""),
                impact          = sig.get("impact", "neutral"),
                reason          = sig.get("reason", ""),
                so_what         = sig.get("so_what", ""),
                confidence      = sig.get("confidence", 0),
                signal_strength = sig.get("signal_strength", 0),
                source_title    = sig.get("source_title", ""),
            )
            db.session.add(row)
            if sig.get("confidence", 0) > 80:
                trigger_commodities.add(sig.get("commodity", ""))

        db.session.commit()
        log.info("Agent: %d signal(s) stored.", len(signals))

        # Prune signals older than 48 hours
        old_cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
        MarketSignal.query.filter(MarketSignal.created_at < old_cutoff).delete()
        db.session.commit()

        # Trigger immediate analysis for high-confidence signals
        if trigger_commodities and not analysis_status["running"]:
            log.info(
                "High-confidence signal(s) for %s — triggering immediate analysis.",
                trigger_commodities,
            )
            # Mark the stored signals as having triggered an analysis
            MarketSignal.query.filter(
                MarketSignal.commodity.in_(list(trigger_commodities)),
                MarketSignal.triggered_analysis == False,
            ).update({"triggered_analysis": True}, synchronize_session=False)
            db.session.commit()
            _breaking_commodities.update(trigger_commodities)
            thread = threading.Thread(target=_run_in_context, daemon=True)
            thread.start()

    except Exception as e:
        log.error("Signal processing failed: %s", e)
        try:
            db.session.rollback()
        except Exception:
            pass


def _get_fresh_articles(commodity, hours=24):
    """Return articles for a commodity from the last `hours` hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = (NewsArticle.query
            .filter_by(commodity=commodity)
            .filter(NewsArticle.fetched_at >= cutoff)
            .order_by(NewsArticle.fetched_at.desc())
            .limit(100)
            .all())
    priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    articles = [
        {"title": r.title, "url": r.url, "summary": r.summary,
         "source": r.source, "published": r.published, "impact": r.impact,
         "_ts": r.fetched_at.timestamp() if r.fetched_at else 0}
        for r in rows
    ]
    articles.sort(
        key=lambda a: (a["_ts"], -priority.get(a.get("impact", "LOW"), 2)),
        reverse=True,
    )
    for a in articles:
        del a["_ts"]
    return articles


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

STEP 4 - DRIVER IDENTIFICATION: Identify exactly 5 bullish drivers (up) and 5 risk factors (down) across these categories: Macroeconomic, Supply, Demand, Geopolitical, Financial Positioning, Currency. Each point should be a distinct, specific insight — not generic filler.

STEP 5 - PRICE ACTION CONTEXT: Determine if current price movement confirms, contradicts, or is unclear relative to the identified drivers. Never invent explanations if evidence is weak.

STEP 6 - MARKET SUMMARY: Write a 3-4 sentence institutional research commentary in the style of Goldman Sachs or JPMorgan. Lead with the single dominant price driver. Follow with supply/demand dynamics and current sentiment bias. Be precise and insight-driven — avoid generic statements. Sound authoritative, not descriptive.

STEP 7 - CURRENT BIAS BREAKDOWN: Generate observational insights reflecting current conditions. Focus on what to watch today, what near-term catalysts exist, and what the structural bias is based on current macro and news. Never give buy/sell recommendations. Never imply a specific timeframe — describe the bias as it stands now.

STEP 8 - DRIVER CONFIDENCE: Assign HIGH, MEDIUM or LOW confidence based on number of supporting events, source consistency, and price alignment.

STEP 9 - MARKET NARRATIVE TRACKING: Identify the dominant narrative (e.g. inflation hedge, supply disruption) and whether it is Strengthening, Stable, Weakening or Shifting.

STEP 10 - STRUCTURED OUTPUT: Return ONLY the following valid JSON. All price levels in USD. No markdown, no explanation, no text before or after the JSON:

{
  "market_summary": "3-4 sentence institutional research commentary. Lead with the dominant price driver. Cover supply/demand dynamics and current sentiment bias. Precise and insight-driven like Goldman Sachs research.",
  "sentiment": "STRONG_BULLISH or BULLISH or NEUTRAL or BEARISH or STRONG_BEARISH",
  "drivers": {
    "up": ["driver 1", "driver 2", "driver 3", "driver 4", "driver 5"],
    "down": ["risk 1", "risk 2", "risk 3", "risk 4", "risk 5"]
  },
  "price_action_context": "confirms or contradicts or unclear - one sentence explanation",
  "trader_takeaways": {
    "intraday": "what to monitor in today's session",
    "next_few_days": "near-term catalysts and events to watch",
    "next_few_weeks": "structural bias based on current macro and positioning"
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
    global latest_results, analysis_status, _breaking_commodities
    analysis_status["running"] = True
    breaking_this_run = set(_breaking_commodities)
    _breaking_commodities.clear()
    analysis_status["last_error"] = None
    log.info("Analysis cycle started.")

    try:
        log.info("Fetching external data...")
        eia        = fetch_eia_data()
        cftc       = fetch_cftc_data()
        imf        = fetch_imf_data()
        worldbank  = fetch_worldbank_data()
        fred       = fetch_fred_data()
        lme_copper = fetch_lme_copper_stocks()
        bdi        = fetch_bdi()
        log.info("External data fetched. FRED=%d indicators, LME copper=%s, BDI=%s",
                 len(fred),
                 lme_copper["value"] if lme_copper else "unavailable",
                 bdi["value"] if bdi else "unavailable")

        # Use pre-fetched articles from DB (news poller runs every 15 min)
        # Fall back to live fetch if DB has nothing yet
        news = {c: _get_fresh_articles(c) for c in COMMODITIES}
        total_from_db = sum(len(v) for v in news.values())
        if total_from_db == 0:
            log.info("No articles in DB yet — falling back to live fetch for this run.")
            all_articles = fetch_all_articles()
            news = filter_by_commodity(all_articles)

        results = {}
        for commodity, articles in news.items():
            log.info("Analysing %s (%d articles)...", commodity, len(articles))
            macro_context = build_macro_context(eia, cftc, imf, worldbank, fred, lme_copper, bdi, commodity)
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
            results[commodity] = {
                "analysis":  analysis,
                "articles":  articles[:25],
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count":     len(articles),
                "breaking":  commodity in breaking_this_run,
            }
        latest_results = results
        # Write to DB
        run_at = datetime.now(timezone.utc)
        try:
            prev_sentiments = {}
            for commodity in results:
                prev = (AnalysisRun.query
                        .filter_by(commodity=commodity)
                        .order_by(AnalysisRun.run_at.desc())
                        .first())
                prev_sentiments[commodity] = prev.sentiment if prev else None

            for commodity, payload in results.items():
                row = AnalysisRun(
                    commodity     = commodity,
                    run_at        = run_at,
                    data          = payload,
                    article_count = payload.get("count", 0),
                    sentiment     = payload.get("analysis", {}).get("sentiment", "NEUTRAL"),
                )
                db.session.add(row)
            db.session.commit()
            log.info("Results saved to database.")

            # Fire email alerts for sentiment changes
            for commodity, payload in results.items():
                new_sentiment = payload.get("analysis", {}).get("sentiment", "NEUTRAL")
                old_sentiment = prev_sentiments.get(commodity)
                if old_sentiment and new_sentiment != old_sentiment:
                    _send_commodity_alerts(
                        commodity,
                        new_sentiment,
                        old_sentiment,
                        payload.get("analysis", {}).get("market_summary", ""),
                    )
        except Exception as db_err:
            log.error("DB write failed: %s", db_err)
            db.session.rollback()
        # Fallback file write
        try:
            with open("results.json", "w") as f:
                json.dump(results, f)
        except Exception:
            pass
        analysis_status["last_run"] = run_at.isoformat()
        log.info("Analysis cycle complete.")
        # Send digest emails to users who opted in
        try:
            summaries = {
                c: (results[c].get("analysis", {}).get("sentiment", "NEUTRAL") if c in results else "NEUTRAL")
                for c in COMMODITIES
            }
            subscribers = User.query.filter_by(notify_on_analysis=True, is_active=True).all()
            for u in subscribers:
                send_analysis_notification_email(u.email, summaries)
        except Exception as e:
            log.error("Analysis notification emails failed: %s", e)
    finally:
        analysis_status["running"] = False

# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

def scheduler_loop():
    # 6am / 12pm / 6pm / 12am IST daily (no weekend restriction)
    # IST = UTC+5:30, so: 06:00 IST = 00:30 UTC, 12:00 IST = 06:30 UTC,
    #                     18:00 IST = 12:30 UTC, 00:00 IST = 18:30 UTC
    schedule.every().day.at("00:30").do(run_analysis)
    schedule.every().day.at("06:30").do(run_analysis)
    schedule.every().day.at("12:30").do(run_analysis)
    schedule.every().day.at("18:30").do(run_analysis)
    while True:
        schedule.run_pending()
        time.sleep(30)

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

_last_db_reload = 0

def _get_new_developments(commodity: str) -> dict | None:
    """Compare the two most recent analysis runs for a commodity.
    Returns new drivers that appeared in the latest run vs the previous one."""
    rows = (
        AnalysisRun.query
        .filter_by(commodity=commodity)
        .order_by(AnalysisRun.run_at.desc())
        .limit(2)
        .all()
    )
    if len(rows) < 2:
        return None
    latest_a = (rows[0].data or {}).get("analysis", {})
    prev_a   = (rows[1].data or {}).get("analysis", {})
    new_up   = list(
        set(latest_a.get("drivers", {}).get("up",   []))
        - set(prev_a.get("drivers",   {}).get("up",   []))
    )
    new_down = list(
        set(latest_a.get("drivers", {}).get("down", []))
        - set(prev_a.get("drivers",   {}).get("down", []))
    )
    prev_ts = rows[1].run_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    if not new_up and not new_down:
        return {"status": "none", "since": prev_ts}
    return {"status": "new", "added_bullish": new_up, "added_bearish": new_down, "since": prev_ts}


@app.route("/signals")
@jwt_required()
def get_signals():
    rows = (
        MarketSignal.query
        .order_by(MarketSignal.created_at.desc())
        .limit(10)
        .all()
    )
    def _urgency(strength):
        if strength >= 8: return "HIGH"
        if strength >= 5: return "MEDIUM"
        return "LOW"

    result = [
        {
            "id":                 r.id,
            "commodity":          r.commodity,
            "event":              r.event,
            "impact":             r.impact,
            "reason":             r.reason,
            "confidence":         r.confidence,
            "signal_strength":    r.signal_strength,
            "urgency":            _urgency(r.signal_strength or 0),
            "so_what":            r.so_what or "",
            "triggered_analysis": r.triggered_analysis,
            "created_at":         r.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for r in rows
    ]
    return jsonify(result)


@app.route("/data")
@jwt_required()
def get_data():
    global _last_db_reload
    now = time.time()
    if now - _last_db_reload > 600:  # reload analysis from DB every 10 minutes
        load_latest_from_db()
        _last_db_reload = now

    # Overlay with fresh articles from news poller (last 24h)
    result = {}
    for commodity, payload in latest_results.items():
        fresh = _get_fresh_articles(commodity, hours=24)
        devs  = _get_new_developments(commodity)
        base  = {**payload, "new_developments": devs}
        if fresh:
            result[commodity] = {**base, "articles": fresh[:25], "count": len(fresh)}
        else:
            result[commodity] = base

    response = jsonify(result)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/prices")
@jwt_required()
def get_prices():
    prices = fetch_live_prices()
    response = jsonify(prices)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/run", methods=["POST"])
@jwt_required()
def trigger_run():
    if analysis_status["running"]:
        return jsonify({"error": "Analysis already running."}), 409
    thread = threading.Thread(target=lambda: _run_in_context(), daemon=True)
    thread.start()
    return jsonify({"message": "Analysis started."}), 202


def _run_in_context():
    with app.app_context():
        run_analysis()


@app.route("/history/<commodity>")
@jwt_required()
def get_history(commodity):
    rows = (AnalysisRun.query
            .filter_by(commodity=commodity)
            .order_by(AnalysisRun.run_at.desc())
            .limit(30)
            .all())
    result = [
        {
            "run_at":    r.run_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sentiment": r.sentiment,
            "articles":  r.article_count,
        }
        for r in rows
    ]
    return jsonify(result)


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

def _send_commodity_alerts(commodity, new_sentiment, old_sentiment, summary=""):
    try:
        subs = (UserAlert.query
                .filter_by(commodity=commodity, enabled=True)
                .join(User, User.id == UserAlert.user_id)
                .filter(User.is_active == True, User.email_verified == True)
                .all())
        for sub in subs:
            user = User.query.get(sub.user_id)
            if user:
                send_alert_email(user.email, commodity, new_sentiment, old_sentiment, summary[:400] if summary else "")
        if subs:
            log.info("Sent %d alert(s) for %s: %s → %s", len(subs), commodity, old_sentiment, new_sentiment)
    except Exception as e:
        log.error("Alert send failed for %s: %s", commodity, e)


SENTIMENT_SCORE = {
    "STRONG_BULLISH": 2, "BULLISH": 1, "NEUTRAL": 0,
    "BEARISH": -1, "STRONG_BEARISH": -2,
}

@app.route("/analytics/overview")
@jwt_required()
def analytics_overview():
    days  = int(request.args.get("days", 30))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows  = AnalysisRun.query.filter(AnalysisRun.run_at >= since).all()

    total_runs   = len(rows)
    avg_articles = round(sum(r.article_count or 0 for r in rows) / total_runs, 1) if total_runs else 0

    by_comm = {}
    for r in rows:
        by_comm.setdefault(r.commodity, []).append(SENTIMENT_SCORE.get(r.sentiment, 0))
    avg_scores = {c: round(sum(s) / len(s), 2) for c, s in by_comm.items()}

    dist = {}
    for r in rows:
        dist[r.sentiment] = dist.get(r.sentiment, 0) + 1

    latest = {}
    for c in ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]:
        row = (AnalysisRun.query.filter_by(commodity=c)
               .order_by(AnalysisRun.run_at.desc()).first())
        if row:
            conf = (row.data or {}).get("analysis", {}).get("confidence", "—")
            narr = (row.data or {}).get("analysis", {}).get("dominant_narrative", {})
            latest[c] = {
                "sentiment":  row.sentiment,
                "confidence": conf,
                "narrative":  narr.get("theme", "—"),
                "nar_status": narr.get("status", "—"),
                "articles":   row.article_count,
                "run_at":     row.run_at.strftime("%d %b, %H:%M UTC"),
            }

    return jsonify({
        "total_runs":             total_runs,
        "avg_articles":           avg_articles,
        "most_bullish":           max(avg_scores, key=avg_scores.get) if avg_scores else None,
        "most_bearish":           min(avg_scores, key=avg_scores.get) if avg_scores else None,
        "avg_scores":             avg_scores,
        "sentiment_distribution": dist,
        "latest":                 latest,
    })


@app.route("/analytics/history")
@jwt_required()
def analytics_history():
    days  = int(request.args.get("days", 30))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows  = (AnalysisRun.query
             .filter(AnalysisRun.run_at >= since)
             .order_by(AnalysisRun.run_at.asc())
             .all())
    by_comm = {}
    for r in rows:
        by_comm.setdefault(r.commodity, []).append({
            "date":     r.run_at.strftime("%d %b, %H:%M"),
            "score":    SENTIMENT_SCORE.get(r.sentiment, 0),
            "sentiment":r.sentiment,
            "articles": r.article_count or 0,
            "confidence": (r.data or {}).get("analysis", {}).get("confidence", "LOW"),
        })
    return jsonify(by_comm)


@app.route("/analytics")
def analytics_page():
    return send_from_directory(".", "analytics.html")


@app.route("/")
def landing():
    return send_from_directory(".", "landing.html")


@app.route("/request-access", methods=["POST"])
def request_access():
    data = request.get_json(silent=True) or {}
    name  = (data.get("name")  or "").strip()
    email = (data.get("email") or "").strip()
    rtype = (data.get("type")  or "").strip()
    org   = (data.get("org")   or "").strip()
    if not name or not email:
        return jsonify({"error": "Name and email are required."}), 400
    log.info("ACCESS REQUEST: name=%s email=%s type=%s org=%s", name, email, rtype, org)
    # Persist to a simple log file so no DB schema change needed
    try:
        with open("access_requests.log", "a", encoding="utf-8") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} | {name} | {email} | {rtype} | {org}\n")
    except Exception as ex:
        log.warning("Could not write access request: %s", ex)
    return jsonify({"ok": True}), 200


@app.route("/app")
def index():
    return send_from_directory(".", "dashboard.html")


@app.route("/admin")
def admin_panel():
    return send_from_directory(".", "admin.html")

# ══════════════════════════════════════════════════════════════════════════════
# START
# ══════════════════════════════════════════════════════════════════════════════

def load_latest_from_db():
    """Populate latest_results from the most recent DB row per commodity."""
    global latest_results
    try:
        loaded = {}
        for commodity in ["Gold", "Crude Oil", "Silver", "Copper", "Natural Gas"]:
            row = (AnalysisRun.query
                   .filter_by(commodity=commodity)
                   .order_by(AnalysisRun.run_at.desc())
                   .first())
            if row:
                loaded[commodity] = row.data
        if loaded:
            latest_results = loaded
            log.info("Loaded latest results from database.")
        else:
            raise ValueError("No DB rows found.")
    except Exception as e:
        log.warning("DB load failed (%s), falling back to results.json.", e)
        try:
            with open("results.json", "r") as f:
                latest_results = json.load(f)
            log.info("Loaded cached results from results.json.")
        except Exception:
            log.warning("No cached results found — dashboard will be empty until first analysis.")


# Load latest results from DB at startup (runs for both gunicorn and direct run)
with app.app_context():
    try:
        db.create_all()
        load_latest_from_db()
    except Exception as _e:
        log.warning("Startup DB load failed: %s", _e)

if __name__ == "__main__":
    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY not set.")
    if not EIA_API_KEY:
        log.info("EIA_API_KEY not set — energy data will be skipped.")
    if not FRED_API_KEY:
        log.info("FRED_API_KEY not set — FRED indicators will be skipped.")
    with app.app_context():
        db.create_all()
        log.info("Database tables ready.")
        load_latest_from_db()
        run_analysis()
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    print("Starting Commodex on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, use_reloader=False)