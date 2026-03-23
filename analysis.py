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
from flask import Flask, jsonify, send_from_directory, request, Response
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
CALENDAR_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "calendar.json")
MACRO_CACHE_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "macro_cache.json")

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
jwt = JWTManager(app)
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)

@jwt.token_in_blocklist_loader
def _check_token_revoked(jwt_header, jwt_payload):
    """Revoke all tokens issued before the user's tokens_valid_after timestamp.
    Called on every @jwt_required() request. Prevents old sessions surviving a password reset."""
    try:
        from models import User as _User
        user_id = int(jwt_payload.get("sub", 0))
        iat     = datetime.fromtimestamp(jwt_payload["iat"], tz=timezone.utc)
        user    = _User.query.get(user_id)
        if not user:
            return True
        if user.tokens_valid_after and iat < user.tokens_valid_after:
            return True
        return False
    except Exception:
        return False

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
        # Security upgrade: hashed token storage + session invalidation
        "ALTER TABLE password_reset_tokens ADD COLUMN token_hash VARCHAR(64)",
        "ALTER TABLE password_reset_tokens ADD COLUMN token_prefix VARCHAR(8)",
        "ALTER TABLE password_reset_tokens ADD COLUMN created_by_ip VARCHAR(45)",
        "ALTER TABLE password_reset_tokens ADD COLUMN used_by_ip VARCHAR(45)",
        "ALTER TABLE users ADD COLUMN tokens_valid_after TIMESTAMPTZ",
        # Make legacy plaintext token column nullable (was NOT NULL)
        "ALTER TABLE password_reset_tokens ALTER COLUMN token DROP NOT NULL",
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
    # ── Tier 5 — Precious metals specialist ───────────────────────────────────
    "https://www.gold.org/goldhub/research/feed",                  # World Gold Council research
    "https://agmetalminer.com/feed/",                              # MetalMiner (base & precious metals)
    "https://www.bullionvault.com/gold-news/rss.do",               # BullionVault gold/silver
    # ── Tier 6 — Energy specialist ────────────────────────────────────────────
    "https://www.hartenergy.com/rss",                              # Hart Energy (oil & gas)
    "https://www.energyvoice.com/feed/",                           # Energy Voice (N. Sea, LNG, OPEC)
    "https://www.naturalgasworld.com/feed",                        # Natural Gas World
    "https://www.downstreamtoday.com/rss/news.aspx",               # Downstream Today (refining/products)
    # ── Tier 7 — Mining & base metals ─────────────────────────────────────────
    "https://www.mining-technology.com/feed/",                     # Mining Technology (copper, gold mines)
    "https://www.nsenergybusiness.com/feed/",                      # NS Energy (energy projects, mining)
    # ── Tier 8 — Official / policy feeds ─────────────────────────────────────
    "https://www.federalreserve.gov/feeds/press_all.xml",          # Federal Reserve press releases
    "https://www.opec.org/opec_web/en/press_room/rss.htm",         # OPEC official press releases
    "https://www.saudiaramco.com/en/news-media/news/rss",          # Saudi Aramco official news
    "https://oilprice.com/rss/category/crude-oil",                 # OilPrice crude-specific feed
    "https://www.worldoil.com/rss/news",                           # World Oil (upstream E&P)
    # ── Tier 9 — Silver specialist ────────────────────────────────────────────
    "https://silverinstitute.org/feed/",                           # Silver Institute (supply/demand data)
    "https://www.silverdoctors.com/feed/",                         # Silver Doctors (precious metals)
    "https://www.seia.org/news/rss",                               # SEIA solar (silver demand driver)
    "https://pv-magazine-usa.com/feed/",                           # PV Magazine (solar/silver demand)
    # ── Tier 10 — Natural gas specialist ─────────────────────────────────────
    "https://lngprime.com/feed/",                                  # LNG Prime (tankers, terminals, prices)
    "https://www.icis.com/explore/resources/news/rss/?feed=gas",   # ICIS gas news
    "https://www.rechargenews.com/feed",                           # Recharge News (LNG, renewables impact)
    "https://energymonitor.ai/feed/",                              # Energy Monitor (gas/LNG transitions)
    # ── Tier 10 — Copper specialist ───────────────────────────────────────────
    "https://copperalliance.org/feed/",                            # Copper Alliance (demand, applications)
    "https://www.mining.com/tag/copper/feed/",                     # Mining.com copper tag
    "https://www.fastmarkets.com/commodities/base-metals/copper/feed/", # Fastmarkets copper
    "https://www.cochilco.cl/blog/feed/",                          # Cochilco Chile copper stats
    # ── Tier 11 — Agriculture (grains, softs) ─────────────────────────────────
    "https://www.agri-pulse.com/rss/articles",                     # Agri-Pulse — all ag commodities (VERIFIED)
    "https://grains.org/feed/",                                    # US Grains Council — corn/soy exports (VERIFIED)
    "https://www.nass.usda.gov/rss/reports.xml",                   # USDA NASS — crop progress & stats (VERIFIED)
    "https://www.nass.usda.gov/rss/asb.xml",                       # USDA NASS ASB — acreage, grain stocks (VERIFIED)
    "https://farmdocdaily.illinois.edu/feed",                      # farmdoc daily — corn/soy research (VERIFIED)
    "https://www.farmprogress.com/rss.xml",                        # Farm Progress — crop markets (VERIFIED)
    "https://www.feedstuffs.com/rss.xml",                          # Feedstuffs — grain/feed industry (VERIFIED)
    "https://www.graincentral.com/feed/",                          # Grain Central — global grain trade (VERIFIED)
    "https://www.farmpolicynews.illinois.edu/feed/",               # Farm Policy News — USDA, policy
    "https://brownfieldagnews.com/feed/",                          # Brownfield Ag News — grains, soy
    # ── Wheat ─────────────────────────────────────────────────────────────────
    "https://www.uswheat.org/feed",                                # US Wheat Associates (VERIFIED)
    "https://www.wheatworld.org/feed",                             # National Assoc of Wheat Growers (VERIFIED)
    # ── Soybeans ──────────────────────────────────────────────────────────────
    "https://www.soygrowers.com/feed",                             # American Soybean Association (VERIFIED)
    # ── Coffee ────────────────────────────────────────────────────────────────
    "https://dailycoffeenews.com/feed/",                           # Daily Coffee News — ICE, Brazil (VERIFIED)
    "https://perfectdailygrind.com/feed/",                         # Perfect Daily Grind — coffee trade (VERIFIED)
    "https://www.worldcoffeeportal.com/rss",                       # World Coffee Portal — market data (VERIFIED)
    "https://www.coffeebi.com/feed/",                              # CoffeeBI — arabica futures, Brazil crop (VERIFIED)
    # ── Sugar ─────────────────────────────────────────────────────────────────
    "https://www.sugaronline.com/feed",                            # Sugar Online — ICE #11 (VERIFIED)
    # ── Cross-commodity shipping/trade ────────────────────────────────────────
    "https://www.hellenicshippingnews.com/category/commodities/rss", # Hellenic Shipping — freight/trade (VERIFIED)
]

COMMODITIES = {
    "Gold":        ["gold price", "gold rate", "gold futures", "bullion", "xau", "xauusd", "gold rises", "gold falls", "gold hits", "gold climbs", "gold", "gld etf", "gold etf", "gold miners", "gdx", "central bank gold", "gold reserves", "gold demand", "gold supply", "gold output", "comex gold", "gold lbma", "real rates gold", "tips yield gold", "gold rally", "gold record", "gold all-time high", "gold safe haven", "gold inflation", "gold dollar"],
    "Silver":      ["silver price", "silver rate", "silver futures", "xag", "xagusd", "silver", "comex silver", "lme silver", "silver demand", "silver supply", "silver output", "silver mine", "silver rally", "silver falls", "silver rises", "precious metal", "silver etf", "silver bullion", "slv etf", "silver solar", "photovoltaic silver", "solar panel silver", "silver industrial", "silver semiconductor", "silver ev", "silver deficit", "silver surplus", "gold silver ratio", "silver institute", "silver miners", "sil etf", "pan american silver", "first majestic", "coeur mining", "fresnillo", "silver squeeze", "comex silver stocks", "silver inventory", "silver peru", "silver mexico"],
    "Crude Oil":   ["crude oil", "wti", "usoil", "us oil", "crudeoil", "brent", "west texas", "opec", "petroleum price", "oil price", "oil rises", "oil falls", "vlcc", "supertanker", "tanker", "cushing", "crude imports", "crude exports", "floating storage", "oil tanker", "strait of hormuz", "persian gulf oil", "brent crude", "wti crude", "opec+", "opec production", "oil inventory", "oil supply", "oil demand", "oil rig", "rig count", "shale oil", "permian", "bakken", "saudi aramco", "aramco", "russia oil", "iran oil", "venezuela oil", "oil sanctions", "spr", "strategic petroleum reserve", "refinery", "crack spread", "gasoline demand", "distillate", "diesel", "jet fuel", "contango", "backwardation oil"],
    "Copper":      ["copper price", "copper futures", "lme copper", "comex copper", "copper", "hg futures", "base metal", "industrial metal", "red metal", "copper demand", "copper supply", "copper output", "copper mine", "copper rally", "copper falls", "copper rises", "copper cathode", "copper inventories", "china pmi", "manufacturing pmi", "china manufacturing", "freeport mcmoran", "bhp copper", "antofagasta", "codelco", "chile copper", "copper smelter", "copper concentrate", "copper scrap", "dr copper", "copper warehouse", "copper stocks lme", "comex copper stocks"],
    "Natural Gas": ["natural gas", "natgas", "lng", "henry hub", "gas price", "natural gas price", "gas futures", "gas demand", "gas supply", "gas inventories", "gas storage", "nymex gas", "europe gas", "us gas", "gas rally", "gas falls", "ttf gas", "gas exports", "lng tanker", "lng carrier", "lng terminal", "lng exports", "lng imports", "sabine pass", "freeport lng", "ttf price", "nbp gas", "european gas storage", "norway gas", "gazprom", "russia gas", "calcasieu pass", "corpus christi lng", "cove point lng", "heating degree days", "hdd", "gas feedgas", "lng utilization", "pipeline flow", "winter gas", "summer gas", "gas withdrawal", "gas injection"],
    "Corn":        ["corn price", "corn futures", "cbot corn", "zc futures", "corn rally", "corn falls", "corn rises", "corn", "maize", "maize price", "corn demand", "corn supply", "corn harvest", "corn crop", "corn output", "corn export", "corn import", "us corn", "iowa corn", "illinois corn", "brazil corn", "argentina corn", "china corn", "corn ethanol", "ethanol corn", "corn feed", "corn usda", "wasde corn", "crop progress corn", "corn planting", "corn yield", "corn acreage", "corn drought", "corn weather", "corn basis", "corn cob", "dent corn", "sweet corn market", "grain corn", "grains market", "usda corn", "feed grain", "corn flour", "cornmeal market"],
    "Wheat":       ["wheat price", "wheat futures", "cbot wheat", "zw futures", "wheat rally", "wheat falls", "wheat rises", "wheat", "wheat demand", "wheat supply", "wheat harvest", "wheat crop", "wheat export", "wheat import", "us wheat", "russia wheat", "ukraine wheat", "australia wheat", "canada wheat", "eu wheat", "france wheat", "black sea wheat", "winter wheat", "spring wheat", "hard red wheat", "soft wheat", "durum wheat", "wheat flour", "bread wheat", "milling wheat", "wheat usda", "wasde wheat", "crop progress wheat", "wheat planting", "wheat yield", "wheat acreage", "wheat drought", "wheat weather", "wheat basis", "wheat stocks", "global wheat", "wheat shortage", "wheat surplus", "wheat geopolitics", "usda wheat"],
    "Soybeans":    ["soybean price", "soybean futures", "cbot soybeans", "zs futures", "soy price", "soybeans rally", "soybeans fall", "soybeans", "soybean", "soy", "soybean demand", "soybean supply", "soybean harvest", "soybean crop", "soybean export", "soybean import", "us soybeans", "brazil soybeans", "argentina soybeans", "china soybeans", "soybean crush", "soy crush", "soymeal", "soy meal", "soybean oil", "soy oil", "vegetable oil", "palm oil soy", "soybean usda", "wasde soybeans", "crop progress soybeans", "soybean planting", "soybean yield", "soybean acreage", "soybean drought", "soybean weather", "soy basis", "soybean stocks", "global soy", "oilseed", "soy protein", "soybean fob", "paranagua soy", "santos soy", "soy meal export", "bdti soy"],
    "Coffee":      ["coffee price", "coffee futures", "ice coffee", "kc futures", "arabica coffee", "robusta coffee", "coffee rally", "coffee falls", "coffee rises", "coffee", "coffee demand", "coffee supply", "coffee harvest", "coffee crop", "coffee export", "coffee import", "brazil coffee", "vietnam coffee", "colombia coffee", "ethiopia coffee", "indonesia coffee", "coffee weather", "coffee frost", "coffee drought", "coffee el nino", "coffee cherry", "green coffee", "roasted coffee", "coffee usda", "wasde coffee", "coffee production", "coffee consumption", "coffee stocks", "coffee deficit", "coffee surplus", "ice arabica", "liffe robusta", "coffee certified stocks", "coffee warehouse", "ny coffee", "london coffee", "coffee basis", "coffee differential", "coffee shipment"],
    "Sugar":       ["sugar price", "sugar futures", "ice sugar", "sb futures", "raw sugar", "sugar rally", "sugar falls", "sugar rises", "sugar", "sugar demand", "sugar supply", "sugar harvest", "sugar crop", "sugar export", "sugar import", "brazil sugar", "india sugar", "thailand sugar", "eu sugar", "australia sugar", "sugar cane", "sugarcane", "beet sugar", "white sugar", "refined sugar", "sugar ethanol", "sugar production", "sugar consumption", "sugar stocks", "sugar deficit", "sugar surplus", "ice #11", "ice 11", "liffe sugar", "sugar certified stocks", "sugar warehouse", "ny sugar", "london sugar", "sugar basis", "sugar differential", "sugar shipment", "sugar unica", "unica report", "sugar usda", "wasde sugar", "sugar monsoon", "india monsoon sugar"],
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
    "Corn": [
        "corn futures CBOT price today",
        "corn USDA WASDE report",
        "corn crop progress condition",
        "corn ethanol demand outlook",
        "corn export sales weekly",
        "maize supply demand global",
        "corn harvest yield forecast",
    ],
    "Wheat": [
        "wheat futures CBOT price today",
        "wheat USDA crop report",
        "wheat black sea export shipment",
        "wheat harvest yield forecast",
        "wheat export sales weekly",
        "global wheat supply demand",
        "wheat Russia Ukraine production",
    ],
    "Soybeans": [
        "soybean futures CBOT price today",
        "soybean USDA crush report",
        "soybean Brazil harvest Argentina",
        "soybean export sales weekly",
        "soybean oil meal price",
        "soybean supply demand outlook",
        "soybeans China import demand",
    ],
    "Coffee": [
        "arabica coffee futures ICE price",
        "robusta coffee price Vietnam",
        "coffee Brazil crop harvest forecast",
        "coffee supply deficit surplus",
        "coffee export ICO report",
        "arabica robusta spread",
        "coffee weather frost Brazil",
    ],
    "Sugar": [
        "raw sugar futures ICE price today",
        "sugar Brazil production ethanol",
        "sugar supply deficit surplus",
        "sugar India export production",
        "sugar cane harvest Thailand",
        "raw sugar ISO report",
        "sugar price unica brazil",
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
        "Corn":        "002602",  # CBOT Corn
        "Wheat":       "001602",  # CBOT Wheat
        "Soybeans":    "005602",  # CBOT Soybeans
        "Coffee":      "083731",  # ICE Coffee C (Arabica)
        "Sugar":       "080732",  # ICE Sugar No. 11
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
        "Corn":        "zc.f",
        "Wheat":       "zw.f",
        "Soybeans":    "zs.f",
        "Coffee":      "kc.f",
        "Sugar":       "sb.f",
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
        "gold_lbma":      ("GOLDAMGBD228NLBM", "Gold LBMA Price (USD/troy oz)"),
        "wti_spot":       ("DCOILWTICO",       "WTI Crude Oil Spot (USD/bbl)"),
        "natgas_spot":    ("DHHNGSP",          "Henry Hub Nat Gas Spot (USD/mmBtu)"),
        # Gold-critical: real rates are the #1 driver
        "tips_10y":       ("DFII10",           "10Y TIPS Real Yield (%) — inverse gold driver"),
        "breakeven_10y":  ("T10YIE",           "10Y Breakeven Inflation Rate (%) — gold demand driver"),
        # Silver-specific
        "silver_lbma":    ("SLVPRUSD",         "Silver LBMA Price (USD/troy oz)"),
        # Expanded macro coverage
        "brazil_cpi":     ("BRACPIALLMINMEI",  "Brazil CPI (coffee/sugar exporter inflation)"),
        "housing_starts": ("HOUST",            "US Housing Starts (000s) — copper demand driver"),
        "india_cpi":      ("INDCPIALLMINMEI",  "India CPI — gold demand / inflationary pressure"),
        "stress_index":   ("STLFSI2",          "St. Louis Financial Stress Index (0=normal, +ve=stress)"),
        "unemployment":   ("UNRATE",           "US Unemployment Rate (%)"),
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


# ── 9. COMEX copper warehouse stocks — scraped from CME/Barchart ──────────────
def fetch_comex_copper_stocks():
    """Scrape COMEX copper warehouse stocks (free, no key)."""
    sources = [
        # Barchart COMEX copper stocks page
        ("https://www.barchart.com/futures/quotes/HG*0/historical-download",
         r'(?:COMEX\s+)?[Cc]opper\s+[Ss]tocks?[^0-9]*([\d,]+)',),
        # CME Group copper stocks
        ("https://www.cmegroup.com/trading/metals/base/copper.html",
         r'[Ww]arehouse\s+[Ss]tocks?[^0-9]*([\d,]+)',),
    ]
    for url, pattern in sources:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                html = r.read().decode("utf-8", errors="ignore")
            m = re.search(pattern, html)
            if m:
                return {"value": int(m.group(1).replace(",", "")), "unit": "short tons", "source": "COMEX"}
        except:
            pass
    # Fallback: try Stooq COMEX copper warrant data
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/HG%3DF?interval=1d&range=2d")
        if data:
            meta = data["chart"]["result"][0]["meta"]
            return {
                "price":  meta.get("regularMarketPrice"),
                "prev":   meta.get("previousClose"),
                "change": round(((meta["regularMarketPrice"] - meta["previousClose"]) / meta["previousClose"]) * 100, 2)
                          if meta.get("previousClose") else None,
                "unit":   "USD/lb",
                "source": "COMEX futures",
            }
    except:
        pass
    return None


# ── 10. China Manufacturing PMI — scraped from Trading Economics (no key) ──────
def fetch_china_pmi():
    """Fetch China NBS Manufacturing PMI — key copper demand indicator (no API key)."""
    result = {}
    try:
        # Trading Economics provides free data tables
        req = urllib.request.Request(
            "https://tradingeconomics.com/china/manufacturing-pmi",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html",
            }
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        # Extract current PMI value
        m = re.search(r'"price"\s*:\s*"?([\d.]+)"?.*?"previous"\s*:\s*"?([\d.]+)"?', html)
        if not m:
            m = re.search(r'<td[^>]*>\s*([\d.]+)\s*</td>', html)
        if m:
            val = float(m.group(1))
            if 40 <= val <= 65:  # sanity check for PMI range
                result["china_manufacturing_pmi"] = {
                    "label": "China NBS Manufacturing PMI",
                    "value": val,
                    "signal": "EXPANSION" if val > 50 else "CONTRACTION",
                }
    except Exception as e:
        log.debug("China PMI fetch failed: %s", e)

    # Also try to get US ISM Manufacturing PMI from same source
    try:
        req = urllib.request.Request(
            "https://tradingeconomics.com/united-states/manufacturing-pmi",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        m = re.search(r'"price"\s*:\s*"?([\d.]+)"?', html)
        if m:
            val = float(m.group(1))
            if 40 <= val <= 65:
                result["us_manufacturing_pmi"] = {
                    "label": "US ISM Manufacturing PMI",
                    "value": val,
                    "signal": "EXPANSION" if val > 50 else "CONTRACTION",
                }
    except Exception as e:
        log.debug("US PMI fetch failed: %s", e)

    return result


# ── 11. TTF European natural gas price — Stooq (free, no key) ────────────────
def fetch_ttf_price():
    """Fetch TTF Dutch natural gas price — European benchmark (free, no key)."""
    # TTF is the European gas benchmark; drives LNG export pricing globally
    symbols = {
        "TTF": ("ttf.f",   "TTF Dutch Natural Gas (EUR/MWh)"),
        "NBP": ("nbp.f",   "NBP UK Natural Gas (p/therm)"),
    }
    result = {}
    for name, (sym, label) in symbols.items():
        try:
            url = f"https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                lines = r.read().decode().strip().split("\n")
            if len(lines) >= 2:
                row   = lines[-1].split(",")
                close = float(row[6])
                open_ = float(row[3])
                chg   = round(((close - open_) / open_) * 100, 2) if open_ else None
                result[name] = {"label": label, "price": close, "change": chg}
        except Exception as e:
            log.debug("TTF/NBP fetch failed for %s: %s", name, e)
    return result


# ── 12. ENTSOG — European gas pipeline flows (free API, no key) ───────────────
def fetch_entsog_flows():
    """Fetch European natural gas pipeline flows from ENTSOG transparency platform."""
    result = {}
    try:
        # Get aggregated EU gas flow (physical flow, most recent day)
        url = ("https://transparency.entsog.eu/api/v1/operationaldata"
               "?limit=5&indicator=Physical+Flow&periodType=day"
               "&pointDirection=DE-TSO-0001ITP-00096entry"  # key German entry point
               "&timezone=UTC")
        data = fetch_json(url, timeout=12)
        if data and data.get("operationalData"):
            rows = data["operationalData"]
            if rows:
                latest = rows[0]
                result["eu_pipeline_flow"] = {
                    "label":  "EU Gas Pipeline Flow (DE entry)",
                    "value":  latest.get("value"),
                    "unit":   latest.get("unit", "kWh/d"),
                    "period": latest.get("periodFrom", ""),
                }
    except Exception as e:
        log.debug("ENTSOG fetch failed: %s", e)

    # Also fetch Norway→EU flow (Norway supplies ~30% of EU gas)
    try:
        url = ("https://transparency.entsog.eu/api/v1/operationaldata"
               "?limit=3&indicator=Physical+Flow&periodType=day"
               "&pointDirection=NO-TSO-0001ITP-00482exit"
               "&timezone=UTC")
        data = fetch_json(url, timeout=12)
        if data and data.get("operationalData"):
            rows = data["operationalData"]
            if rows:
                result["norway_eu_flow"] = {
                    "label":  "Norway→EU Gas Flow",
                    "value":  rows[0].get("value"),
                    "unit":   rows[0].get("unit", "kWh/d"),
                    "period": rows[0].get("periodFrom", ""),
                }
    except Exception as e:
        log.debug("ENTSOG Norway flow fetch failed: %s", e)
    return result


# ── 13. DOE LNG export tracking — scraped from EIA (no extra key) ─────────────
def fetch_lng_export_data():
    """Fetch US LNG export capacity utilization from EIA (uses existing EIA key)."""
    result = {}
    if not EIA_API_KEY:
        return result
    # US LNG exports (monthly, bcf/d)
    series = {
        "lng_exports_monthly": "NG.N9133US2.M",   # US LNG exports (MMcf/month)
        "lng_feedgas":         "NG.N9070US2.D",   # LNG feedgas demand (daily proxy for utilization)
    }
    for key, series_id in series.items():
        url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={EIA_API_KEY}&length=2"
        data = fetch_json(url)
        if data:
            rows = data.get("response", {}).get("data", [])
            if rows:
                result[key] = {
                    "latest":   rows[0].get("value"),
                    "previous": rows[1].get("value") if len(rows) > 1 else None,
                    "unit":     rows[0].get("unit", ""),
                    "period":   rows[0].get("period", ""),
                }
    return result


# ── 14. Gold ETF holdings — GLD/IAU scrape (free, no key) ────────────────────
def fetch_gold_etf_holdings():
    """Fetch SPDR GLD and iShares IAU ETF gold holdings (tonnes) — institutional demand signal."""
    result = {}
    # GLD — world's largest gold ETF, published daily by SPDR
    try:
        req = urllib.request.Request(
            "https://www.spdrgoldshares.com/usa/GLD/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        # Look for tonnes held
        for pattern in [
            r'(\d[\d,]+\.?\d*)\s*(?:Tonnes|tonnes)',
            r'Total\s+Gold[^0-9]*([\d,]+\.?\d*)',
            r'"holdings"\s*:\s*"?([\d,]+\.?\d*)"?',
        ]:
            m = re.search(pattern, html)
            if m:
                val = float(m.group(1).replace(",", ""))
                if 500 < val < 2000:  # sanity check: GLD holds ~800-1200 tonnes
                    result["GLD"] = {"label": "SPDR GLD Holdings (tonnes)", "value": val}
                    break
    except Exception as e:
        log.debug("GLD holdings fetch failed: %s", e)

    # GLD — price + daily change via Yahoo Finance
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/GLD?interval=1d&range=5d")
        if data:
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["GLD_price"] = {"label": "SPDR GLD ETF Price", "price": price, "change": chg}
    except Exception as e:
        log.debug("GLD price fetch failed: %s", e)

    # GDX — VanEck Gold Miners ETF price (miner stocks lead gold price)
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/GDX?interval=1d&range=5d")
        if data:
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["GDX"] = {"label": "VanEck Gold Miners ETF (GDX)", "price": price, "change": chg}
    except Exception as e:
        log.debug("GDX fetch failed: %s", e)

    # GDXJ — Junior gold miners (higher beta, early signal)
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/GDXJ?interval=1d&range=5d")
        if data:
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["GDXJ"] = {"label": "Junior Gold Miners ETF (GDXJ)", "price": price, "change": chg}
    except Exception as e:
        log.debug("GDXJ fetch failed: %s", e)

    # USO — United States Oil Fund (retail oil sentiment proxy)
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/USO?interval=1d&range=5d")
        if data:
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["USO"] = {"label": "US Oil Fund ETF (USO)", "price": price, "change": chg}
    except Exception as e:
        log.debug("USO fetch failed: %s", e)

    # UCO — 2x leveraged oil ETF (momentum signal)
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/UCO?interval=1d&range=5d")
        if data:
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["UCO"] = {"label": "2x Leveraged Oil ETF (UCO)", "price": price, "change": chg}
    except Exception as e:
        log.debug("UCO fetch failed: %s", e)

    return result


# ── 15. Brent crude + WTI futures curve — Stooq/Yahoo (free, no key) ────────
def fetch_oil_prices():
    """Fetch Brent price, WTI/Brent spread, and WTI futures curve for contango/backwardation."""
    result = {}

    # Brent crude — global benchmark
    try:
        url = "https://stooq.com/q/l/?s=bco.f&f=sd2t2ohlcv&h&e=csv"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            lines = r.read().decode().strip().split("\n")
        if len(lines) >= 2:
            row   = lines[-1].split(",")
            close = float(row[6])
            open_ = float(row[3])
            result["brent"] = {"label": "Brent Crude (USD/bbl)", "price": close,
                                "change": round(((close - open_) / open_) * 100, 2)}
    except Exception as e:
        log.debug("Brent fetch failed: %s", e)

    # WTI futures curve: CL1/CL2/CL3 — contango = oversupply, backwardation = tight
    wti_curve = {}
    for month, sym in [("M1", "cl.f"), ("M2", "cl2.f"), ("M3", "cl3.f"), ("M6", "cl6.f")]:
        try:
            url = f"https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                lines = r.read().decode().strip().split("\n")
            if len(lines) >= 2:
                row = lines[-1].split(",")
                wti_curve[month] = float(row[6])
        except:
            pass
    if len(wti_curve) >= 2:
        prices = list(wti_curve.values())
        spread = round(prices[-1] - prices[0], 2)
        structure = "CONTANGO (oversupply signal)" if spread > 0.5 else "BACKWARDATION (tight supply)" if spread < -0.5 else "FLAT"
        result["wti_curve"] = {"prices": wti_curve, "m1_m6_spread": spread, "structure": structure}

    # Brent/WTI spread
    if "brent" in result and wti_curve.get("M1"):
        spread = round(result["brent"]["price"] - wti_curve["M1"], 2)
        result["brent_wti_spread"] = {"value": spread, "label": "Brent/WTI Spread (USD/bbl)"}

    return result


# ── 16. Baker Hughes rig count — scraped (free, no key) ───────────────────────
def fetch_rig_count():
    """Fetch Baker Hughes US oil & gas rig count — weekly production outlook signal."""
    result = {}
    try:
        # Baker Hughes publishes rig count data on their website
        req = urllib.request.Request(
            "https://rigcount.bakerhughes.com/na-rig-count",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read().decode("utf-8", errors="ignore")
        # Extract US total rig count
        for pattern in [
            r'U\.S\.\s+Total[^0-9]*([\d,]+)',
            r'Total\s+U\.S\.[^0-9]*([\d,]+)',
            r'"total_us"\s*:\s*(\d+)',
            r'US\s+Total[^0-9]*(\d{3,4})',
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                val = int(m.group(1).replace(",", ""))
                if 300 < val < 1500:  # sanity check
                    result["us_total"] = {"label": "US Total Rig Count (Baker Hughes)", "value": val}
                    break
        # Oil rigs specifically
        for pattern in [
            r'Oil[^0-9]*([\d]+)\s*(?:Gas|Water|Misc)',
            r'"oil_rigs"\s*:\s*(\d+)',
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                val = int(m.group(1))
                if 200 < val < 1200:
                    result["oil_rigs"] = {"label": "US Oil Rigs (Baker Hughes)", "value": val}
                    break
    except Exception as e:
        log.debug("Baker Hughes rig count fetch failed: %s", e)

    # Fallback: try EIA rig count RSS
    if not result:
        try:
            feed = feedparser.parse("https://www.eia.gov/rss/news.xml")
            for entry in feed.entries[:20]:
                if "rig count" in entry.get("title", "").lower():
                    m = re.search(r'(\d{3,4})\s*rigs?', entry.get("summary", ""), re.IGNORECASE)
                    if m:
                        result["us_total"] = {"label": "US Rig Count (EIA news)", "value": int(m.group(1))}
                        break
        except:
            pass
    return result


# ── 17. Baltic Dirty Tanker Index — Yahoo Finance (free, no key) ──────────────
def fetch_tanker_rates():
    """Fetch BDTI and BCTI tanker freight rates — oil trade flow cost signal."""
    result = {}
    for name, ticker, label in [
        ("BDTI", "%5EBDTI", "Baltic Dirty Tanker Index"),
        ("BCTI", "%5EBCTI", "Baltic Clean Tanker Index"),
    ]:
        try:
            data = fetch_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2d")
            if data:
                meta  = data["chart"]["result"][0]["meta"]
                price = meta.get("regularMarketPrice")
                prev  = meta.get("previousClose", price)
                chg   = round(((price - prev) / prev) * 100, 2) if prev else None
                result[name] = {"label": label, "value": price, "change": chg}
        except Exception as e:
            log.debug("Tanker rate fetch failed for %s: %s", name, e)
    return result


# ── 18. EIA SPR stocks — uses existing EIA key ────────────────────────────────
def fetch_spr_data():
    """Fetch US Strategic Petroleum Reserve stocks from EIA."""
    if not EIA_API_KEY:
        return None
    url = "https://api.eia.gov/v2/seriesid/PET.WCSSTUS1.W?api_key={}&length=2".format(EIA_API_KEY)
    # SPR series
    url = f"https://api.eia.gov/v2/seriesid/PET.WCSSTUS1.W?api_key={EIA_API_KEY}&length=2"
    # Actual SPR series ID
    url = f"https://api.eia.gov/v2/seriesid/PET.WSTRSTUS1.W?api_key={EIA_API_KEY}&length=2"
    data = fetch_json(url)
    if data:
        rows = data.get("response", {}).get("data", [])
        if len(rows) >= 2:
            latest   = float(rows[0].get("value", 0))
            previous = float(rows[1].get("value", 0))
            return {
                "latest":   latest,
                "previous": previous,
                "change":   round(latest - previous, 0),
                "unit":     rows[0].get("unit", "Mb"),
                "period":   rows[0].get("period", ""),
            }
    return None


# ── 19. Silver-specific data — SLV ETF, miners, G/S ratio, COMEX stocks ──────
def fetch_silver_data():
    """Fetch silver ETF holdings, miner stocks, gold/silver ratio, COMEX stocks."""
    result = {}

    # Gold/Silver ratio — from live prices via Stooq
    try:
        gold_url   = "https://stooq.com/q/l/?s=gc.f&f=sd2t2ohlcv&h&e=csv"
        silver_url = "https://stooq.com/q/l/?s=si.f&f=sd2t2ohlcv&h&e=csv"
        def _close(url):
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                lines = r.read().decode().strip().split("\n")
            return float(lines[-1].split(",")[6]) if len(lines) >= 2 else None
        gold_px   = _close(gold_url)
        silver_px = _close(silver_url)
        if gold_px and silver_px and silver_px > 0:
            ratio = round(gold_px / silver_px, 2)
            if ratio > 90:
                signal = "SILVER HISTORICALLY CHEAP vs gold (ratio >90 = mean-reversion signal)"
            elif ratio > 80:
                signal = "SILVER UNDERVALUED vs gold (ratio 80-90)"
            elif ratio < 60:
                signal = "SILVER EXPENSIVE vs gold (ratio <60)"
            else:
                signal = "SILVER FAIRLY VALUED vs gold (ratio 60-80)"
            result["gold_silver_ratio"] = {
                "value":  ratio,
                "gold":   gold_px,
                "silver": silver_px,
                "signal": signal,
            }
    except Exception as e:
        log.debug("Gold/silver ratio fetch failed: %s", e)

    # SLV — iShares Silver Trust ETF (price + proxy for institutional demand)
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/SLV?interval=1d&range=5d")
        if data:
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["SLV"] = {"label": "iShares Silver Trust ETF (SLV)", "price": price, "change": chg}
    except Exception as e:
        log.debug("SLV fetch failed: %s", e)

    # SIL — Global X Silver Miners ETF
    try:
        data = fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/SIL?interval=1d&range=5d")
        if data:
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("previousClose", price)
            chg   = round(((price - prev) / prev) * 100, 2) if prev else None
            result["SIL"] = {"label": "Global X Silver Miners ETF (SIL)", "price": price, "change": chg}
    except Exception as e:
        log.debug("SIL fetch failed: %s", e)

    # COMEX silver warehouse stocks — CME website scrape
    try:
        req = urllib.request.Request(
            "https://www.cmegroup.com/trading/metals/precious/silver.html",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode("utf-8", errors="ignore")
        for pattern in [
            r'[Ww]arehouse\s+[Ss]tocks?[^0-9]*([\d,]+)',
            r'([\d,]+)\s*[Tt]roy\s*[Oo]z.*?[Ss]tocks?',
            r'"silver_stocks"\s*:\s*"?([\d,]+)"?',
        ]:
            m = re.search(pattern, html)
            if m:
                val = int(m.group(1).replace(",", ""))
                if val > 100000:  # sanity: COMEX silver stocks are in thousands of troy oz
                    result["comex_silver_stocks"] = {"value": val, "unit": "troy oz", "label": "COMEX Silver Warehouse Stocks"}
                    break
    except Exception as e:
        log.debug("COMEX silver stocks fetch failed: %s", e)

    return result


# ── 20. BLS — CPI and PPI (free public API, no key required) ───────────────────
def fetch_bls_data():
    """Fetch US CPI and PPI from BLS public API v1 (no API key needed)."""
    result = {}
    series_map = {
        "CUUR0000SA0":  "US CPI All Items",
        "CUUR0000SA0L1E": "US Core CPI (ex food & energy)",
        "WPU10":        "US PPI Mining",
        "PCU212221212221": "US PPI Gold & Silver Ores",
    }
    try:
        payload = json.dumps({"seriesid": list(series_map.keys()), "startyear": "2024", "endyear": "2026"}).encode()
        req = urllib.request.Request(
            "https://api.bls.gov/publicAPI/v1/timeseries/data/",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        for series in data.get("Results", {}).get("series", []):
            sid = series["seriesID"]
            label = series_map.get(sid, sid)
            rows = series.get("data", [])
            if len(rows) >= 2:
                latest   = float(rows[0]["value"])
                previous = float(rows[1]["value"])
                result[sid] = {
                    "label":    label,
                    "value":    latest,
                    "change":   round(latest - previous, 3),
                    "period":   f"{rows[0]['periodName']} {rows[0]['year']}",
                }
    except Exception as e:
        log.debug("BLS fetch failed: %s", e)
    return result


# ── 10. Open-Meteo — Weather / Heating Degree Days for Natural Gas ─────────────
def fetch_weather_data():
    """Fetch 7-day temperature forecast for key global hubs (free, no API key)."""
    hubs = {
        "New York":    (40.71,  -74.01),  # NE US heating demand
        "Chicago":     (41.85,  -87.65),  # Midwest gas + corn/wheat belt
        "Houston":     (29.76,  -95.37),  # Gulf Coast LNG exports
        "London":      (51.51,   -0.13),  # European TTF gas hub region
        "Sao Paulo":   (-23.55, -46.63),  # Brazil coffee & sugar production
        "Ho Chi Minh": (10.76,  106.66),  # Vietnam Robusta coffee
        "Minneapolis": (44.98,  -93.27),  # US Northern Plains wheat & corn
        "New Delhi":   (28.68,   77.22),  # India sugar, wheat & gold demand
    }
    result = {}
    for city, (lat, lon) in hubs.items():
        try:
            url = (f"https://api.open-meteo.com/v1/forecast"
                   f"?latitude={lat}&longitude={lon}"
                   f"&daily=temperature_2m_max,temperature_2m_min"
                   f"&temperature_unit=fahrenheit&forecast_days=7&timezone=America%2FNew_York")
            data = fetch_json(url)
            if data and "daily" in data:
                highs = data["daily"].get("temperature_2m_max", [])
                lows  = data["daily"].get("temperature_2m_min", [])
                if highs and lows:
                    avg_temp = round(sum((h + l) / 2 for h, l in zip(highs, lows)) / len(highs), 1)
                    # Heating degree days: base 65°F
                    hdd = round(sum(max(0, 65 - (h + l) / 2) for h, l in zip(highs, lows)), 1)
                    # Cooling degree days: base 65°F
                    cdd = round(sum(max(0, (h + l) / 2 - 65) for h, l in zip(highs, lows)), 1)
                    result[city] = {
                        "avg_temp_f": avg_temp,
                        "hdd_7day":   hdd,
                        "cdd_7day":   cdd,
                        "demand_signal": "HIGH HEATING" if hdd > 70 else "HIGH COOLING" if cdd > 70 else "MODERATE",
                    }
        except Exception as e:
            log.debug("Weather fetch failed for %s: %s", city, e)
    return result


# ── 11. US Treasury — Yield curve direct from Treasury.gov (no API key) ────────
def fetch_treasury_yields():
    """Fetch latest US Treasury yield curve from Treasury.gov XML feed."""
    result = {}
    try:
        from datetime import date
        today = date.today()
        # Try current month, fallback to previous
        for delta in [0, -1]:
            month = today.month + delta
            year  = today.year
            if month <= 0:
                month += 12
                year  -= 1
            url = (f"https://home.treasury.gov/resource-center/data-chart-center"
                   f"/interest-rates/pages/xml?data=daily_treasury_yield_curve"
                   f"&field_tdate_year={year}&field_tdate_month={month:02d}")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                xml = r.read().decode("utf-8", errors="ignore")
            entries = re.findall(r'<entry>(.*?)</entry>', xml, re.DOTALL)
            if entries:
                last = entries[-1]
                def _t(tag):
                    m = re.search(rf'<d:{tag}[^>]*>([\d.]+)</d:{tag}>', last)
                    return float(m.group(1)) if m else None
                result = {
                    "1M":  _t("BC_1MONTH"),
                    "3M":  _t("BC_3MONTH"),
                    "6M":  _t("BC_6MONTH"),
                    "1Y":  _t("BC_1YEAR"),
                    "2Y":  _t("BC_2YEAR"),
                    "5Y":  _t("BC_5YEAR"),
                    "10Y": _t("BC_10YEAR"),
                    "30Y": _t("BC_30YEAR"),
                }
                result = {k: v for k, v in result.items() if v is not None}
                if result:
                    break
    except Exception as e:
        log.debug("Treasury yield fetch failed: %s", e)
    return result


# ── USDA agricultural data (free, no key) ─────────────────────────────────────
def fetch_usda_data():
    """
    Fetch USDA NASS QuickStats and FAS data for grain/soft commodities.
    Uses public USDA APIs — no key required for basic stats.
    Returns structured data for Corn, Wheat, Soybeans, Coffee, Sugar.
    """
    result = {}
    # USDA NASS QuickStats public API (no key for aggregate stats)
    NASS_BASE = "https://quickstats.nass.usda.gov/api/api_GET/?key=DEMO_KEY"

    # Stooq-based nearby futures prices for ag commodities (backup price data)
    ag_symbols = {
        "Corn":     ("zc.f",  "USd/bu"),
        "Wheat":    ("zw.f",  "USd/bu"),
        "Soybeans": ("zs.f",  "USd/bu"),
        "Coffee":   ("kc.f",  "USd/lb"),
        "Sugar":    ("sb.f",  "USd/lb"),
    }
    for comm, (sym, unit) in ag_symbols.items():
        try:
            url = f"https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                lines = r.read().decode().strip().split("\n")
            if len(lines) >= 2:
                row   = lines[-1].split(",")
                close = float(row[6])
                open_ = float(row[3])
                high  = float(row[4])
                low   = float(row[5])
                chg   = ((close - open_) / open_) * 100
                result.setdefault(comm, {})["nearby_futures"] = {
                    "price": close, "open": open_, "high": high, "low": low,
                    "change_pct": round(chg, 2), "unit": unit
                }
        except Exception as e:
            log.debug("USDA/Stooq price fetch failed for %s: %s", comm, e)

    # USDA WASDE (monthly) crop summary — scrape FAS PSD online (no auth)
    # Fetch latest corn, wheat, soy world production estimates from FAS
    fas_commodities = {
        "Corn":     "0440000",  # FAS commodity code
        "Wheat":    "0410000",
        "Soybeans": "2222000",
    }
    for comm, code in fas_commodities.items():
        try:
            url = (f"https://apps.fas.usda.gov/psdonline/api/psd/commodity/{code}"
                   f"?commodityCode={code}&marketYear=2025&countryCode=00&reportId=1")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            if data:
                world = next((d for d in data if d.get("countryCode") == "00"), None)
                if world:
                    result.setdefault(comm, {})["world_production"] = {
                        "value": world.get("value"),
                        "unit": "MMT",
                        "market_year": "2025/26",
                        "label": f"USDA World Production ({comm})"
                    }
        except Exception as e:
            log.debug("USDA FAS fetch failed for %s: %s", comm, e)

    # Brazil UNICA sugar/ethanol (free, no key) — for Sugar commodity
    try:
        url = "https://www.unica.com.br/feed/"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            text = r.read().decode("utf-8", errors="ignore")
        result.setdefault("Sugar", {})["unica_available"] = "UNICA data feed active"
    except:
        pass

    # ICO Coffee statistics (free public data)
    try:
        url = "https://icocoffee.org/feed/"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            text = r.read().decode("utf-8", errors="ignore")
        result.setdefault("Coffee", {})["ico_feed"] = "ICO Coffee feed active"
    except:
        pass

    return result


# ── Build macro context string for Claude ─────────────────────────────────────
def build_macro_context(eia, cftc, imf, worldbank, fred, lme_copper, bdi, commodity_name,
                        bls=None, weather=None, treasury=None, comex_copper=None, pmi=None,
                        ttf=None, entsog=None, lng_exports=None, gold_etf=None,
                        oil_prices=None, rig_count=None, tanker_rates=None, spr=None,
                        silver_data=None, live_prices=None, usda_data=None):
    lines = []

    # LIVE PRICE — always first so Claude sees today's move immediately
    if live_prices and commodity_name in live_prices:
        p = live_prices[commodity_name]
        if p.get("price") and p.get("change") is not None:
            direction = "UP" if p["change"] > 0 else "DOWN"
            strength  = "STRONGLY" if abs(p["change"]) > 2 else "MODESTLY" if abs(p["change"]) > 0.5 else "FLAT"
            lines.append(f"TODAY'S PRICE ACTION: {commodity_name} is {strength} {direction} {p['change']:+.2f}% "
                         f"(price: {p['price']:.2f}) — your sentiment MUST be consistent with this move unless macro/news strongly contradicts it.")

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
        # Real rates — THE primary gold driver (inverse relationship)
        if commodity_name in ("Gold", "Silver"):
            if "tips_10y" in fred:
                d = fred["tips_10y"]
                chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
                regime = "GOLD SUPPORTIVE (negative real rates)" if d["value"] < 0 else "GOLD HEADWIND (positive real rates)"
                lines.append(f"10Y REAL YIELD (TIPS): {d['value']}%{chg_str} — {regime} ({d['date']})")
            if "breakeven_10y" in fred:
                d = fred["breakeven_10y"]
                chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
                lines.append(f"10Y BREAKEVEN INFLATION: {d['value']}%{chg_str} — inflation expectations ({d['date']})")
        if commodity_name == "Silver" and "silver_lbma" in fred:
            d = fred["silver_lbma"]
            chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
            lines.append(f"SILVER LBMA FIX: {d['value']}{chg_str} ({d['date']})")
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

        # Housing starts — key copper demand driver (pipes, wiring)
        if commodity_name == "Copper" and "housing_starts" in fred:
            d = fred["housing_starts"]
            chg_str = f" (chg: {d['change']:+.1f})" if d.get("change") is not None else ""
            lines.append(f"US HOUSING STARTS: {d['value']}k units{chg_str} ({d['date']}) — construction copper demand")

        # Unemployment — broad demand proxy
        if "unemployment" in fred:
            d = fred["unemployment"]
            chg_str = f" (chg: {d['change']:+.1f})" if d.get("change") is not None else ""
            lines.append(f"US UNEMPLOYMENT: {d['value']}%{chg_str} ({d['date']})")

        # Financial stress index — risk-off affects all commodities
        if "stress_index" in fred:
            d = fred["stress_index"]
            regime = "STRESSED (risk-off)" if d["value"] > 1 else "NORMAL" if d["value"] > -1 else "LOOSE (risk-on)"
            lines.append(f"FINANCIAL STRESS INDEX: {d['value']} — {regime} ({d['date']})")

        # Brazil CPI — coffee, sugar, soybeans supply costs
        if commodity_name in ("Coffee", "Sugar", "Soybeans") and "brazil_cpi" in fred:
            d = fred["brazil_cpi"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            lines.append(f"BRAZIL CPI: {d['value']}{chg_str} ({d['date']}) — exporter inflation pressure")

        # India CPI — gold demand, wheat/sugar consumption
        if commodity_name in ("Gold", "Silver", "Wheat", "Sugar") and "india_cpi" in fred:
            d = fred["india_cpi"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            lines.append(f"INDIA CPI: {d['value']}{chg_str} ({d['date']}) — demand pressure")

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

    # BLS CPI/PPI — gold, silver, crude oil
    if bls and commodity_name in ("Gold", "Silver", "Crude Oil"):
        lines.append("BLS INFLATION DATA:")
        for sid, d in bls.items():
            chg_str = f" (chg: {d['change']:+.3f})" if d.get("change") is not None else ""
            lines.append(f"  {d['label']}: {d['value']}{chg_str} ({d['period']})")

    # US Treasury yield curve — gold, silver (rate-sensitive)
    if treasury and commodity_name in ("Gold", "Silver", "Crude Oil"):
        parts = [f"{k}: {v}%" for k, v in sorted(treasury.items(), key=lambda x: ["1M","3M","6M","1Y","2Y","5Y","10Y","30Y"].index(x[0]) if x[0] in ["1M","3M","6M","1Y","2Y","5Y","10Y","30Y"] else 99)]
        if parts:
            lines.append(f"US TREASURY YIELD CURVE: {' | '.join(parts)}")
            # Inversion signal
            if treasury.get("2Y") and treasury.get("10Y"):
                spread = round(treasury["10Y"] - treasury["2Y"], 2)
                inv = "INVERTED (recession signal)" if spread < 0 else "normal"
                lines.append(f"  10Y-2Y Spread: {spread:+.2f}% — {inv}")

    # Weather — relevant cities per commodity
    WEATHER_SCOPE = {
        "Natural Gas": ["New York", "Chicago", "Houston", "London"],
        "Coffee":      ["Sao Paulo", "Ho Chi Minh"],
        "Sugar":       ["Sao Paulo"],
        "Corn":        ["Chicago", "Minneapolis"],
        "Wheat":       ["Chicago", "Minneapolis"],
        "Soybeans":    ["Sao Paulo", "Chicago"],
        "Crude Oil":   ["Houston"],
    }
    if weather and commodity_name in WEATHER_SCOPE:
        relevant = [c for c in WEATHER_SCOPE[commodity_name] if c in weather]
        if relevant:
            lines.append("WEATHER FORECAST (7-DAY):")
            for city in relevant:
                w = weather[city]
                lines.append(f"  {city}: avg {w['avg_temp_f']}°F | HDD: {w['hdd_7day']} | CDD: {w['cdd_7day']} → {w['demand_signal']}")

    # TTF / NBP European gas prices — natural gas
    if ttf and commodity_name == "Natural Gas":
        lines.append("EUROPEAN GAS PRICES:")
        for name, d in ttf.items():
            chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
            lines.append(f"  {d['label']}: {d['price']}{chg}")

    # ENTSOG European pipeline flows — natural gas
    if entsog and commodity_name == "Natural Gas":
        lines.append("EUROPEAN GAS PIPELINE FLOWS (ENTSOG):")
        for key, d in entsog.items():
            if d.get("value"):
                lines.append(f"  {d['label']}: {d['value']:,} {d['unit']} ({d['period']})")

    # LNG export data — natural gas
    if lng_exports and commodity_name == "Natural Gas":
        lines.append("US LNG EXPORTS:")
        if "lng_feedgas" in lng_exports:
            d = lng_exports["lng_feedgas"]
            chg = ""
            if d.get("latest") and d.get("previous"):
                diff = float(d["latest"]) - float(d["previous"])
                chg = f" (change: {diff:+.0f}) — {'utilization up' if diff > 0 else 'utilization down'}"
            lines.append(f"  LNG Feedgas Demand: {d['latest']} {d['unit']}{chg} ({d['period']})")
        if "lng_exports_monthly" in lng_exports:
            d = lng_exports["lng_exports_monthly"]
            lines.append(f"  Monthly LNG Exports: {d['latest']} {d['unit']} ({d['period']})")

    # Oil prices: Brent, WTI curve, spread — crude oil
    if oil_prices and commodity_name == "Crude Oil":
        if "brent" in oil_prices:
            d = oil_prices["brent"]
            chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
            lines.append(f"BRENT CRUDE: {d['price']} USD/bbl{chg}")
        if "brent_wti_spread" in oil_prices:
            d = oil_prices["brent_wti_spread"]
            lines.append(f"BRENT/WTI SPREAD: {d['value']:+.2f} USD/bbl")
        if "wti_curve" in oil_prices:
            d = oil_prices["wti_curve"]
            curve_str = " | ".join(f"{k}: {v}" for k, v in d["prices"].items())
            lines.append(f"WTI FUTURES CURVE: {curve_str}")
            lines.append(f"  Structure: {d['structure']} (M1-M6 spread: {d['m1_m6_spread']:+.2f})")

    # Oil ETF flows — crude oil sentiment proxy
    if gold_etf and commodity_name == "Crude Oil":
        for ticker in ("USO", "UCO"):
            if ticker in gold_etf:
                d = gold_etf[ticker]
                chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
                lines.append(f"{d['label']}: ${d['price']}{chg}")

    # Baker Hughes rig count — crude oil & natural gas
    if rig_count and commodity_name in ("Crude Oil", "Natural Gas"):
        lines.append("BAKER HUGHES RIG COUNT:")
        for key in ("us_total", "oil_rigs"):
            if key in rig_count:
                d = rig_count[key]
                lines.append(f"  {d['label']}: {d['value']}")

    # Tanker rates — crude oil
    if tanker_rates and commodity_name == "Crude Oil":
        lines.append("TANKER FREIGHT RATES:")
        for name, d in tanker_rates.items():
            chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
            lines.append(f"  {d['label']}: {d['value']}{chg}")

    # SPR — crude oil
    if spr and commodity_name == "Crude Oil":
        chg = f" (change: {spr['change']:+.0f})" if spr.get("change") is not None else ""
        lines.append(f"US SPR STOCKS: {spr['latest']} {spr['unit']}{chg} ({spr['period']})")

    # Silver-specific data
    if silver_data and commodity_name == "Silver":
        if "gold_silver_ratio" in silver_data:
            d = silver_data["gold_silver_ratio"]
            lines.append(f"GOLD/SILVER RATIO: {d['value']} (Gold ${d['gold']} / Silver ${d['silver']})")
            lines.append(f"  Signal: {d['signal']}")
        if "SLV" in silver_data:
            d = silver_data["SLV"]
            chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
            lines.append(f"SLV ETF (institutional silver demand): ${d['price']}{chg}")
        if "SIL" in silver_data:
            d = silver_data["SIL"]
            chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
            lines.append(f"SIL Silver Miners ETF: ${d['price']}{chg} — miners lead silver price")
        if "comex_silver_stocks" in silver_data:
            d = silver_data["comex_silver_stocks"]
            lines.append(f"COMEX SILVER WAREHOUSE STOCKS: {d['value']:,} {d['unit']}")

    # Gold ETF holdings + miner stocks — gold & silver
    if gold_etf and commodity_name in ("Gold", "Silver"):
        lines.append("GOLD ETF & MINERS:")
        if "GLD" in gold_etf:
            lines.append(f"  {gold_etf['GLD']['label']}: {gold_etf['GLD']['value']} tonnes")
        for ticker in ("GDX", "GDXJ"):
            if ticker in gold_etf:
                d = gold_etf[ticker]
                chg = f" ({d['change']:+.2f}%)" if d.get("change") is not None else ""
                lines.append(f"  {d['label']}: ${d['price']}{chg} — miners lead gold by 1-2 sessions")

    # PMI — copper & crude oil (manufacturing demand signal)
    if pmi and commodity_name in ("Copper", "Crude Oil"):
        lines.append("MANUFACTURING PMI (key demand indicator):")
        for key, d in pmi.items():
            signal = "▲ EXPANSION" if d["signal"] == "EXPANSION" else "▼ CONTRACTION"
            lines.append(f"  {d['label']}: {d['value']} — {signal} (50 = neutral)")

    # COMEX copper warehouse stocks
    if comex_copper and commodity_name == "Copper":
        if comex_copper.get("price"):
            chg = f" ({comex_copper['change']:+.2f}%)" if comex_copper.get("change") else ""
            lines.append(f"COMEX COPPER PRICE: {comex_copper['price']} {comex_copper['unit']}{chg}")
        elif comex_copper.get("value"):
            lines.append(f"COMEX COPPER WAREHOUSE STOCKS: {comex_copper['value']:,} {comex_copper['unit']}")

    # ── AGRICULTURAL DATA ─────────────────────────────────────────────────────
    AG_COMMS = ("Corn", "Wheat", "Soybeans", "Coffee", "Sugar")
    if commodity_name in AG_COMMS and usda_data:
        comm_data = usda_data.get(commodity_name, {})

        # Nearby futures price from Stooq
        if "nearby_futures" in comm_data:
            f = comm_data["nearby_futures"]
            direction = "UP" if f["change_pct"] > 0 else "DOWN"
            lines.append(f"NEARBY FUTURES ({commodity_name}): {f['price']:.2f} {f['unit']} "
                         f"| Open: {f['open']:.2f} | High: {f['high']:.2f} | Low: {f['low']:.2f} "
                         f"| Change: {f['change_pct']:+.2f}% ({direction})")

        # USDA World Production (grains)
        if "world_production" in comm_data:
            wp = comm_data["world_production"]
            lines.append(f"USDA WORLD PRODUCTION: {wp['label']}: {wp['value']} {wp['unit']} ({wp['market_year']})")

        # CFTC positioning for ag commodities
        if cftc and commodity_name in cftc:
            d = cftc[commodity_name]
            lines.append(f"CFTC COT POSITIONING ({commodity_name}): Net {d['net_position']:+,} contracts — {d['positioning']}")

        # PMI — demand signal for grains (China is world's largest buyer)
        if pmi:
            for key, d in pmi.items():
                signal = "▲ EXPANSION" if d["signal"] == "EXPANSION" else "▼ CONTRACTION"
                lines.append(f"  {d['label']}: {d['value']} — {signal} (China demand signal for agri)")

        # Dollar index — agri commodities USD-denominated, inverse correlation
        if fred and "dxy" in fred:
            d = fred["dxy"]
            chg_str = f" (chg: {d['change']:+.2f})" if d.get("change") is not None else ""
            lines.append(f"USD INDEX (DXY): {d['value']}{chg_str} — strong USD = headwind for agri commodity prices")

        # Weather context (relevant for crop damage risk)
        if weather:
            for loc, w in weather.items():
                if "forecast" in w:
                    f7 = w["forecast"]
                    lines.append(f"WEATHER ({loc}): Temp {f7.get('avg_temp_c', '?')}°C, "
                                 f"precip {f7.get('total_precip_mm', '?')}mm/7d "
                                 f"— crop weather context")

        # Energy/fertilizer cost context for grains
        if commodity_name in ("Corn", "Wheat", "Soybeans") and fred:
            if "natgas_spot" in fred:
                d = fred["natgas_spot"]
                lines.append(f"NATURAL GAS (fertilizer cost driver): {d['value']} USD/mmBtu ({d['date']})")

        # Brazil Real / USD (critical for coffee & sugar — Brazil is world #1 producer)
        if commodity_name in ("Coffee", "Sugar") and fred:
            if "cnyusd" in fred:
                d = fred["cnyusd"]
                lines.append(f"CNY/USD: {d['value']} — China coffee/sugar import demand indicator")

        # Special coffee context
        if commodity_name == "Coffee":
            if comm_data.get("ico_feed"):
                lines.append("ICO COFFEE DATA: Feed active — monitor certified stocks and differential")
            lines.append("KEY COFFEE DRIVERS: Brazil crop size & frost risk, Vietnam robusta output, "
                         "certified ICE stocks, USD/BRL exchange rate, global roaster demand")

        # Special sugar context
        if commodity_name == "Sugar":
            if comm_data.get("unica_available"):
                lines.append("UNICA BRAZIL: Data feed active — monitor crush pace, sugar/ethanol mix, cane availability")
            lines.append("KEY SUGAR DRIVERS: Brazil crushing pace & ethanol parity, India output & export policy, "
                         "Thailand production, crude oil price (ethanol competition), monsoon rains")

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

STEP 4 - DRIVER IDENTIFICATION: Identify exactly 8 bullish drivers (up) and 5 risk factors (down) across these categories: Macroeconomic, Supply, Demand, Geopolitical, Financial Positioning, Currency, Price Action, Positioning. Each point should be a distinct, specific insight — not generic filler.

STEP 5 - PRICE ACTION CONTEXT: The macro context above shows TODAY'S PRICE ACTION. Your sentiment output MUST be consistent with this. If the commodity is up 2%+ today, you should not output BEARISH or NEUTRAL unless there is overwhelming macro/fundamental evidence to the contrary — and you must explicitly explain the contradiction. A commodity up 4% on the day is almost never NEUTRAL.

STEP 6 - MARKET SUMMARY: Write a detailed, multi-paragraph institutional research commentary in the style of a Goldman Sachs or JPMorgan commodity research note. Structure it as follows:
  • Paragraph 1 — Lead with today's dominant price driver and what is moving the market right now. Reference specific data points, levels, or events from the context above.
  • Paragraph 2 — Supply and demand dynamics: cover production, inventory levels, seasonal factors, and any supply-side disruptions or demand shifts relevant to this commodity.
  • Paragraph 3 — Macroeconomic and geopolitical context: discuss how the broader macro environment (USD, rates, inflation, global growth), geopolitical developments, or policy decisions are influencing this commodity.
  • Paragraph 4 — Market positioning and sentiment: describe current trader positioning, ETF flows if relevant, technical levels of note, and the prevailing sentiment bias.
  • Paragraph 5 — Forward-looking view: synthesise the above into a clear directional bias with the key catalysts or risks to watch. Do not give buy/sell recommendations — frame as an observational research view.
  Be precise and insight-driven. Use specific figures and data from the context above wherever possible. Sound authoritative and analytical, not generic or descriptive.

STEP 7 - CURRENT BIAS BREAKDOWN: Generate observational insights reflecting current conditions. Focus on what to watch today, what near-term catalysts exist, and what the structural bias is based on current macro and news. Never give buy/sell recommendations. Never imply a specific timeframe — describe the bias as it stands now.

STEP 8 - DRIVER CONFIDENCE: Assign HIGH, MEDIUM or LOW confidence based on number of supporting events, source consistency, and price alignment.

STEP 9 - MARKET NARRATIVE TRACKING: Identify the dominant narrative (e.g. inflation hedge, supply disruption) and whether it is Strengthening, Stable, Weakening or Shifting.

STEP 10 - STRUCTURED OUTPUT: Return ONLY the following valid JSON. All price levels in USD. No markdown, no explanation, no text before or after the JSON:

{
  "market_summary": "Detailed multi-paragraph institutional research commentary as written in Step 6. Five structured paragraphs: (1) dominant price driver with specific data, (2) supply/demand dynamics, (3) macro and geopolitical context, (4) positioning and sentiment, (5) forward-looking directional view. Authoritative, precise, insight-driven. No buy/sell recommendations.",
  "sentiment": "STRONG_BULLISH or BULLISH or NEUTRAL or BEARISH or STRONG_BEARISH",
  "drivers": {
    "up": ["driver 1", "driver 2", "driver 3", "driver 4", "driver 5", "driver 6", "driver 7", "driver 8"],
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
        max_tokens=4096,
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
        fetch_and_store_news()
        log.info("Fetching external data...")
        eia        = fetch_eia_data()
        cftc       = fetch_cftc_data()
        imf        = fetch_imf_data()
        worldbank  = fetch_worldbank_data()
        fred       = fetch_fred_data()
        lme_copper = fetch_lme_copper_stocks()
        bdi        = fetch_bdi()
        bls          = fetch_bls_data()
        weather      = fetch_weather_data()
        treasury     = fetch_treasury_yields()
        comex_copper  = fetch_comex_copper_stocks()
        pmi           = fetch_china_pmi()
        ttf           = fetch_ttf_price()
        entsog        = fetch_entsog_flows()
        lng_exports   = fetch_lng_export_data()
        gold_etf      = fetch_gold_etf_holdings()
        oil_prices    = fetch_oil_prices()
        rig_count     = fetch_rig_count()
        tanker_rates  = fetch_tanker_rates()
        spr           = fetch_spr_data()
        silver_data   = fetch_silver_data()
        live_prices   = fetch_live_prices()
        usda_data     = fetch_usda_data()
        log.info("External data fetched. FRED=%d, BLS=%d, Treasury=%d, Weather=%d, PMI=%d, TTF=%d, ENTSOG=%d, GoldETF=%d, LME copper=%s, BDI=%s",
                 len(fred), len(bls), len(treasury), len(weather), len(pmi), len(ttf), len(entsog), len(gold_etf),
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
            macro_context = build_macro_context(eia, cftc, imf, worldbank, fred, lme_copper, bdi, commodity,
                                                bls=bls, weather=weather, treasury=treasury,
                                                comex_copper=comex_copper, pmi=pmi,
                                                ttf=ttf, entsog=entsog, lng_exports=lng_exports,
                                                gold_etf=gold_etf, oil_prices=oil_prices,
                                                rig_count=rig_count, tanker_rates=tanker_rates,
                                                spr=spr, silver_data=silver_data,
                                                live_prices=live_prices, usda_data=usda_data)
            try:
                if articles:
                    analysis = analyse_commodity(commodity, articles, macro_context)
                else:
                    analysis = {
                        "market_summary": "Coming soon.",
                        "sentiment": "NEUTRAL",
                        "drivers": {"up": [], "down": []},
                        "price_action_context": "Coming soon",
                        "trader_takeaways": {"intraday": "Coming soon", "next_few_days": "Coming soon", "next_few_weeks": "Coming soon"},
                        "confidence": "LOW",
                        "dominant_narrative": {"theme": "Coming soon", "status": "—"},
                        "takeaway": {"bias": "Neutral", "strategy": "Coming soon", "short_term": "Coming soon", "medium_term": "Coming soon"}
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
        # Save macro snapshot for Key Data Panel
        try:
            macro_snapshot = {"last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
            for c in COMMODITIES:
                macro_snapshot[c] = build_macro_snapshot(
                    c, fred, cftc, eia, bdi, lme_copper,
                    gold_etf, silver_data, oil_prices, rig_count,
                    tanker_rates, spr, pmi, ttf, weather, lng_exports,
                    usda_data=usda_data)
            save_macro_cache(macro_snapshot)
        except Exception as e:
            log.warning("Macro snapshot save failed: %s", e)
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
# MACRO DATA SNAPSHOT (KEY DATA PANEL)
# ══════════════════════════════════════════════════════════════════════════════

def _fmt(val, decimals=2, suffix=""):
    if val is None: return "—"
    try: return f"{float(val):.{decimals}f}{suffix}"
    except: return str(val)

def _chg(val, decimals=2, suffix=""):
    if val is None: return None
    try: return f"{float(val):+.{decimals}f}{suffix}"
    except: return None

def build_macro_snapshot(commodity, fred, cftc, eia, bdi, lme_copper,
                         gold_etf, silver_data, oil_prices, rig_count,
                         tanker_rates, spr, pmi, ttf, weather, lng_exports,
                         usda_data=None):
    """Build a structured key-data snapshot for the frontend Key Data Panel."""
    rows = []  # each row: {label, value, change, signal, signal_color}
    BULL = "#22c55e"; BEAR = "#ef4444"; NEUT = "#fbbf24"; MUTED = "#5d6478"

    def row(label, value, change=None, signal=None, signal_color=None):
        rows.append({"label": label, "value": value,
                     "change": change, "signal": signal, "signal_color": signal_color})

    if commodity == "Gold":
        if "tips_10y" in fred:
            d = fred["tips_10y"]
            sc = BULL if d["value"] < 0 else BEAR
            sig = "SUPPORTIVE" if d["value"] < 0 else "HEADWIND"
            row("10Y Real Yield (TIPS)", f"{_fmt(d['value'], 2)}%", _chg(d.get("change"), 3, "%"), sig, sc)
        if "breakeven_10y" in fred:
            d = fred["breakeven_10y"]
            row("Breakeven Inflation", f"{_fmt(d['value'], 2)}%", _chg(d.get("change"), 3, "%"))
        if "dxy" in fred:
            d = fred["dxy"]
            sc = BEAR if d.get("change", 0) and d["change"] > 0 else BULL
            row("DXY (US Dollar Index)", _fmt(d["value"], 2), _chg(d.get("change"), 2), None, sc)
        if gold_etf and "GLD" in gold_etf:
            row("GLD ETF Holdings", f"{gold_etf['GLD']['value']} t", None, "INSTITUTIONAL DEMAND")
        if silver_data and "gold_silver_ratio" in silver_data:
            d = silver_data["gold_silver_ratio"]
            sc = BULL if d["value"] > 80 else NEUT
            row("Gold / Silver Ratio", _fmt(d["value"], 1), None, d["signal"][:18], sc)
        if cftc and "Gold" in cftc:
            d = cftc["Gold"]
            sc = BULL if d["net_position"] > 0 else BEAR
            row("CFTC Net Position", f"{d['net_position']:+,}", None, d["positioning"], sc)
        if gold_etf and "GDX" in gold_etf:
            d = gold_etf["GDX"]
            sc = BULL if (d.get("change") or 0) > 0 else BEAR
            row("GDX Miners ETF", f"${_fmt(d['price'], 2)}", _chg(d.get("change"), 2, "%"), None, sc)
        if "vix" in fred:
            d = fred["vix"]
            sc = BULL if d["value"] > 20 else MUTED
            sig = "FEAR (gold +)" if d["value"] > 25 else "ELEVATED" if d["value"] > 18 else "CALM"
            row("VIX Volatility", _fmt(d["value"], 1), _chg(d.get("change"), 2), sig, sc)

    elif commodity == "Silver":
        if silver_data and "gold_silver_ratio" in silver_data:
            d = silver_data["gold_silver_ratio"]
            sc = BULL if d["value"] > 80 else NEUT
            row("Gold / Silver Ratio", _fmt(d["value"], 1), None, d["signal"][:20], sc)
        if "tips_10y" in fred:
            d = fred["tips_10y"]
            sc = BULL if d["value"] < 0 else BEAR
            row("10Y Real Yield (TIPS)", f"{_fmt(d['value'], 2)}%", _chg(d.get("change"), 3, "%"),
                "SUPPORTIVE" if d["value"] < 0 else "HEADWIND", sc)
        if silver_data and "SLV" in silver_data:
            d = silver_data["SLV"]
            sc = BULL if (d.get("change") or 0) > 0 else BEAR
            row("SLV ETF", f"${_fmt(d['price'], 2)}", _chg(d.get("change"), 2, "%"))
        if silver_data and "SIL" in silver_data:
            d = silver_data["SIL"]
            sc = BULL if (d.get("change") or 0) > 0 else BEAR
            row("SIL Miners ETF", f"${_fmt(d['price'], 2)}", _chg(d.get("change"), 2, "%"))
        if cftc and "Silver" in cftc:
            d = cftc["Silver"]
            sc = BULL if d["net_position"] > 0 else BEAR
            row("CFTC Net Position", f"{d['net_position']:+,}", None, d["positioning"], sc)
        if pmi and "china_manufacturing_pmi" in pmi:
            d = pmi["china_manufacturing_pmi"]
            sc = BULL if d["signal"] == "EXPANSION" else BEAR
            row("China Mfg PMI", _fmt(d["value"], 1), None, d["signal"], sc)
        if silver_data and "comex_silver_stocks" in silver_data:
            d = silver_data["comex_silver_stocks"]
            row("COMEX Silver Stocks", f"{d['value']:,} oz")

    elif commodity == "Crude Oil":
        if oil_prices and "brent" in oil_prices:
            d = oil_prices["brent"]
            sc = BULL if (d.get("change") or 0) > 0 else BEAR
            row("Brent Crude", f"${_fmt(d['price'], 2)}", _chg(d.get("change"), 2, "%"), None, sc)
        if oil_prices and "brent_wti_spread" in oil_prices:
            d = oil_prices["brent_wti_spread"]
            row("Brent / WTI Spread", f"${d['value']:+.2f}/bbl")
        if oil_prices and "wti_curve" in oil_prices:
            d = oil_prices["wti_curve"]
            sc = BEAR if "CONTANGO" in d["structure"] else BULL
            row("WTI Futures Structure", d["structure"].split("(")[0].strip(),
                f"M1-M6: {d['m1_m6_spread']:+.2f}", None, sc)
        if cftc and "Crude Oil" in cftc:
            d = cftc["Crude Oil"]
            sc = BULL if d["net_position"] > 0 else BEAR
            row("CFTC Net Position", f"{d['net_position']:+,}", None, d["positioning"], sc)
        if rig_count and "oil_rigs" in rig_count:
            row("Baker Hughes Oil Rigs", str(rig_count["oil_rigs"]["value"]), None, "WEEKLY")
        if spr:
            chg = f"{spr['change']:+.0f}" if spr.get("change") else None
            row("US SPR Stocks", f"{spr['latest']:.0f} {spr['unit']}", chg)
        if tanker_rates and "BDTI" in tanker_rates:
            d = tanker_rates["BDTI"]
            sc = BULL if (d.get("change") or 0) > 0 else MUTED
            row("Baltic Dirty Tanker (BDTI)", str(d["value"]), _chg(d.get("change"), 2, "%"), None, sc)
        if eia and "cushing_stocks" in eia:
            d = eia["cushing_stocks"]
            diff = float(d["latest"]) - float(d["previous"]) if d.get("latest") and d.get("previous") else None
            sc = BEAR if diff and diff > 0 else BULL if diff else None
            row("Cushing Stocks", f"{d['latest']} {d['unit']}", f"{diff:+.0f}" if diff else None,
                "BEARISH BUILD" if diff and diff > 0 else "BULLISH DRAW" if diff else None, sc)

    elif commodity == "Copper":
        if pmi and "china_manufacturing_pmi" in pmi:
            d = pmi["china_manufacturing_pmi"]
            sc = BULL if d["signal"] == "EXPANSION" else BEAR
            row("China Mfg PMI", _fmt(d["value"], 1), None, d["signal"], sc)
        if pmi and "us_manufacturing_pmi" in pmi:
            d = pmi["us_manufacturing_pmi"]
            sc = BULL if d["signal"] == "EXPANSION" else BEAR
            row("US ISM PMI", _fmt(d["value"], 1), None, d["signal"], sc)
        if lme_copper:
            row("LME Copper Stocks", f"{lme_copper['value']:,} t")
        if cftc and "Copper" in cftc:
            d = cftc["Copper"]
            sc = BULL if d["net_position"] > 0 else BEAR
            row("CFTC Net Position", f"{d['net_position']:+,}", None, d["positioning"], sc)
        if bdi:
            sc = BULL if (bdi.get("change") or 0) > 0 else MUTED
            row("Baltic Dry Index (BDI)", str(bdi["value"]), _chg(bdi.get("change"), 2, "%"), None, sc)
        if "cnyusd" in fred:
            d = fred["cnyusd"]
            row("CNY / USD", _fmt(d["value"], 4), _chg(d.get("change"), 4))

    elif commodity == "Natural Gas":
        if ttf and "TTF" in ttf:
            d = ttf["TTF"]
            sc = BULL if (d.get("change") or 0) > 0 else BEAR
            row("TTF Dutch Gas", f"{_fmt(d['price'], 2)} EUR/MWh", _chg(d.get("change"), 2, "%"), None, sc)
        if weather:
            ny = weather.get("New York", {})
            ch = weather.get("Chicago", {})
            if ny: row("HDD 7-Day (New York)", str(ny["hdd_7day"]), None, ny["demand_signal"])
            if ch: row("HDD 7-Day (Chicago)",  str(ch["hdd_7day"]), None, ch["demand_signal"])
        if eia and "natgas_storage" in eia:
            d = eia["natgas_storage"]
            diff = float(d["latest"]) - float(d["previous"]) if d.get("latest") and d.get("previous") else None
            sc = BEAR if diff and diff > 0 else BULL if diff else None
            row("EIA Gas Storage", f"{d['latest']} {d['unit']}", f"{diff:+.0f} bcf" if diff else None,
                "INJECTION" if diff and diff > 0 else "WITHDRAWAL" if diff else None, sc)
        if cftc and "Natural Gas" in cftc:
            d = cftc["Natural Gas"]
            sc = BULL if d["net_position"] > 0 else BEAR
            row("CFTC Net Position", f"{d['net_position']:+,}", None, d["positioning"], sc)
        if lng_exports and "lng_feedgas" in lng_exports:
            d = lng_exports["lng_feedgas"]
            row("LNG Feedgas Demand", f"{d['latest']} {d['unit']}", None, "EXPORT UTILIZATION")
        if rig_count and "us_total" in rig_count:
            row("Total US Rig Count", str(rig_count["us_total"]["value"]), None, "BAKER HUGHES")

    # ── AGRICULTURAL COMMODITIES ──────────────────────────────────────────────
    elif commodity in ("Corn", "Wheat", "Soybeans", "Coffee", "Sugar"):
        comm_data = (usda_data or {}).get(commodity, {})

        # Nearby futures price
        if "nearby_futures" in comm_data:
            f = comm_data["nearby_futures"]
            chg_pct = f.get("change_pct")
            sc = BULL if (chg_pct or 0) > 0 else BEAR
            row("Nearby Futures", f"{f['price']:.2f} {f['unit']}",
                f"{chg_pct:+.2f}%" if chg_pct is not None else None, None, sc)
            row("Session Range", f"{f['low']:.2f} – {f['high']:.2f} {f['unit']}")

        # USDA world production (grains)
        if "world_production" in comm_data:
            wp = comm_data["world_production"]
            row("USDA World Output", f"{wp['value']} {wp['unit']}", None, wp["market_year"])

        # CFTC positioning
        if cftc and commodity in cftc:
            d = cftc[commodity]
            sc = BULL if d["net_position"] > 0 else BEAR
            row("CFTC Net Position", f"{d['net_position']:+,}", None, d["positioning"], sc)

        # DXY (dollar inverse signal for all ag)
        if fred and "dxy" in fred:
            d = fred["dxy"]
            sc = BEAR if (d.get("change") or 0) > 0 else BULL
            row("DXY (US Dollar)", _fmt(d["value"], 2), _chg(d.get("change"), 2), None, sc)

        # VIX — risk-off hurts commodity demand
        if fred and "vix" in fred:
            d = fred["vix"]
            sc = BEAR if d["value"] > 25 else NEUT if d["value"] > 18 else BULL
            row("VIX Volatility", _fmt(d["value"], 1), _chg(d.get("change"), 2),
                "FEAR" if d["value"] > 25 else "CALM", sc)

        # PMI — China demand indicator (critical for grains)
        if pmi and commodity in ("Corn", "Wheat", "Soybeans"):
            for key, d in pmi.items():
                sc = BULL if d["signal"] == "EXPANSION" else BEAR
                row(d["label"][:22], _fmt(d["value"], 1), None, d["signal"], sc)

        # Nat gas = fertilizer cost (grains)
        if commodity in ("Corn", "Wheat", "Soybeans") and fred and "natgas_spot" in fred:
            d = fred["natgas_spot"]
            sc = BEAR if (d.get("change") or 0) > 0 else BULL
            row("Nat Gas (Fert. Cost)", f"{d['value']}", _chg(d.get("change"), 3), None, sc)

        # BDI — freight cost for crop exports
        if bdi:
            chg_str = f"{bdi['change']:+.2f}%" if bdi.get("change") is not None else None
            row("Baltic Dry Index", str(bdi["value"]), chg_str, "FREIGHT COST")

        # Special: Coffee certified stocks signal
        if commodity == "Coffee":
            row("Key Driver", "Brazil crop & frost risk", None, "WATCH")
            row("Spread", "Arabica vs Robusta", None, "ICE/LIFFE")

        # Special: Sugar ethanol parity
        if commodity == "Sugar":
            row("Key Driver", "Brazil crush & ethanol mix", None, "UNICA")
            if fred and "wti_spot" in fred:
                d = fred["wti_spot"]
                sc = BULL if (d.get("change") or 0) > 0 else BEAR
                row("WTI (Ethanol Parity)", f"${_fmt(d['value'], 2)}", _chg(d.get("change"), 2), None, sc)

    return rows


def save_macro_cache(snapshot):
    try:
        with open(MACRO_CACHE_FILE, "w") as f:
            json.dump(snapshot, f, indent=2)
    except Exception as e:
        log.error("Failed to save macro_cache.json: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# ECONOMIC CALENDAR ENGINE
# ══════════════════════════════════════════════════════════════════════════════

_CALENDAR_EVENT_FILTERS = [
    # (keyword, country_code, min_impact, affected_commodities)
    # country_code "" means match any country
    ("FOMC",               "USD", "Medium", ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]),
    ("Federal Reserve",    "USD", "Medium", ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]),
    ("Interest Rate",      "USD", "High",   ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]),
    ("Interest Rate",      "EUR", "High",   ["Gold", "Silver", "Crude Oil", "Natural Gas"]),
    ("ECB",                "EUR", "Medium", ["Gold", "Silver", "Crude Oil", "Natural Gas"]),
    ("Powell",             "USD", "Medium", ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]),
    ("CPI",                "USD", "High",   ["Gold", "Silver", "Crude Oil"]),
    ("PPI",                "USD", "Medium", ["Gold", "Silver"]),
    ("Non-Farm",           "USD", "High",   ["Gold", "Silver", "Crude Oil"]),
    ("Unemployment",       "USD", "High",   ["Gold", "Silver"]),
    ("GDP",                "USD", "High",   ["Copper", "Crude Oil"]),
    ("GDP",                "CNY", "High",   ["Copper", "Crude Oil"]),
    ("PMI",                "CNY", "High",   ["Copper", "Crude Oil"]),
    ("Manufacturing PMI",  "USD", "Medium", ["Copper", "Crude Oil"]),
    ("Retail Sales",       "USD", "Medium", ["Crude Oil", "Copper"]),
    ("Crude Oil",          "USD", "Low",    ["Crude Oil"]),
    ("Inventories",        "USD", "Medium", ["Crude Oil"]),
    ("Natural Gas",        "USD", "Low",    ["Natural Gas"]),
    ("OPEC",               "",    "High",   ["Crude Oil"]),
    ("Commitment",         "USD", "Low",    ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]),
    ("COT",                "USD", "Low",    ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]),
    ("Treasury",           "USD", "Medium", ["Gold", "Silver"]),
    ("Inflation",          "USD", "High",   ["Gold", "Silver", "Crude Oil"]),
    ("Inflation",          "EUR", "High",   ["Gold", "Silver"]),
    ("Trade Balance",      "CNY", "Medium", ["Copper", "Crude Oil"]),
    ("Industrial Production","USD","Medium",["Copper", "Crude Oil"]),
    # Agricultural events
    ("USDA",               "USD", "High",   ["Corn", "Wheat", "Soybeans", "Coffee", "Sugar"]),
    ("Crop",               "USD", "Medium", ["Corn", "Wheat", "Soybeans"]),
    ("Grain",              "USD", "Medium", ["Corn", "Wheat", "Soybeans"]),
    ("Agricultural",       "USD", "Medium", ["Corn", "Wheat", "Soybeans", "Coffee", "Sugar"]),
    ("WASDE",              "USD", "High",   ["Corn", "Wheat", "Soybeans"]),
    ("Coffee",             "",    "Medium", ["Coffee"]),
    ("Sugar",              "",    "Medium", ["Sugar"]),
    ("Corn",               "USD", "Medium", ["Corn"]),
    ("Wheat",              "USD", "Medium", ["Wheat"]),
    ("Soybean",            "USD", "Medium", ["Soybeans"]),
]

_IMPACT_ORDER = {"High": 3, "Medium": 2, "Low": 1, "Holiday": 0}

def _parse_event_dt(date_str, time_str):
    """Parse ForexFactory date+time to UTC datetime. Returns None on failure."""
    try:
        dt_str = f"{date_str} {time_str}" if time_str and time_str != "All Day" else f"{date_str} 00:00"
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def fetch_economic_calendar():
    """Fetch this week's economic events from ForexFactory free API (no key needed)."""
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = json.loads(r.read())
    except Exception as e:
        log.warning("ForexFactory calendar fetch failed: %s", e)
        return []

    now    = datetime.now(timezone.utc)
    events = []
    seen   = set()

    for item in raw:
        title   = item.get("title", "")
        country = item.get("country", "USD")
        impact  = item.get("impact", "Low")
        date_s  = item.get("date", "")
        time_s  = item.get("time", "00:00")
        if impact == "Holiday":
            continue
        # Match against filter list
        matched_commodities = []
        matched = False
        for kw, cc, min_imp, comms in _CALENDAR_EVENT_FILTERS:
            kw_match = kw.lower() in title.lower()
            cc_match = (cc == "") or (country.upper() == cc.upper())
            imp_ok   = _IMPACT_ORDER.get(impact, 0) >= _IMPACT_ORDER.get(min_imp, 0)
            if kw_match and cc_match and imp_ok:
                matched = True
                for c in comms:
                    if c not in matched_commodities:
                        matched_commodities.append(c)
        if not matched:
            continue
        dt = _parse_event_dt(date_s, time_s)
        if dt is None:
            continue
        key = f"{title}_{date_s}_{time_s}"
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "id":                   key,
            "event_name":           title,
            "country":              country,
            "date":                 date_s,
            "time":                 time_s,
            "dt_utc":               dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "impact":               impact,
            "forecast":             item.get("forecast", ""),
            "actual":               item.get("actual", ""),
            "previous":             item.get("previous", ""),
            "affected_commodities": matched_commodities,
            "pre_event_briefing":   None,
            "ai_summary":           None,
            "briefing_sent":        False,
            "summary_generated":    False,
        })

    events.sort(key=lambda e: e["dt_utc"])
    return events


def generate_event_summary(event):
    """Generate a 2-3 sentence post-event AI summary using Claude (Haiku for speed)."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        actual   = event.get("actual", "")
        forecast = event.get("forecast", "")
        previous = event.get("previous", "")
        comms    = ", ".join(event.get("affected_commodities", []))
        surprise = ""
        if actual and forecast:
            surprise = f"Actual ({actual}) vs Forecast ({forecast}) — {'beat' if actual > forecast else 'missed'} expectations."
        prompt = f"""You are a commodity market analyst. An economic event just occurred:

Event: {event['event_name']} ({event['country']})
Actual: {actual or 'not yet available'}
Forecast: {forecast or 'no consensus'}
Previous: {previous or 'unknown'}
{surprise}
Affected commodities: {comms}

Write 2-3 sentences in plain English:
1. What the data showed vs expectations
2. Whether it was a surprise or in-line
3. Immediate directional impact on the affected commodities

Be direct and precise. Institutional tone. No buy/sell recommendations."""
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.error("generate_event_summary failed: %s", e)
        return None


def generate_pre_event_briefing(event):
    """Generate a 2-3 sentence 'what to watch' briefing before the event."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        forecast = event.get("forecast", "")
        previous = event.get("previous", "")
        comms    = ", ".join(event.get("affected_commodities", []))
        prompt = f"""You are a commodity market analyst. An important economic event is happening in 2 hours:

Event: {event['event_name']} ({event['country']})
Consensus Forecast: {forecast or 'no consensus'}
Previous: {previous or 'unknown'}
Affected commodities: {comms}

Write 2-3 sentences covering:
1. What the market expects and why it matters
2. What to watch for (beat vs miss scenario)
3. Which commodity is most exposed

Be concise and direct. No buy/sell recommendations."""
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.error("generate_pre_event_briefing failed: %s", e)
        return None


def _load_calendar():
    """Load calendar.json, return dict with upcoming/past lists."""
    try:
        with open(CALENDAR_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"last_updated": None, "upcoming": [], "past": []}


def _save_calendar(data):
    """Save calendar data to calendar.json."""
    try:
        with open(CALENDAR_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.error("Failed to save calendar.json: %s", e)


def refresh_calendar():
    """Fetch fresh calendar data and merge with existing (preserving AI summaries). Runs every 6h."""
    log.info("Refreshing economic calendar...")
    now       = datetime.now(timezone.utc)
    fresh     = fetch_economic_calendar()
    existing  = _load_calendar()

    # Build lookup of existing events by ID to preserve AI content
    existing_map = {e["id"]: e for e in existing.get("upcoming", []) + existing.get("past", [])}

    upcoming = []
    past     = []
    for ev in fresh:
        ev_dt = datetime.fromisoformat(ev["dt_utc"].replace("Z", "+00:00"))
        # Merge existing AI content
        if ev["id"] in existing_map:
            old = existing_map[ev["id"]]
            ev["pre_event_briefing"]  = old.get("pre_event_briefing")
            ev["ai_summary"]          = old.get("ai_summary")
            ev["briefing_sent"]       = old.get("briefing_sent", False)
            ev["summary_generated"]   = old.get("summary_generated", False)
        if ev_dt > now:
            upcoming.append(ev)
        else:
            past.append(ev)

    # Also keep past events from existing that have AI summaries (not in fresh week anymore)
    fresh_ids = {e["id"] for e in fresh}
    for ev in existing.get("past", []):
        if ev["id"] not in fresh_ids and ev.get("ai_summary"):
            past.append(ev)

    # Sort and trim
    upcoming.sort(key=lambda e: e["dt_utc"])
    past.sort(key=lambda e: e["dt_utc"], reverse=True)
    past = past[:10]  # keep last 10 past events

    # Generate post-event summaries for events 30min+ past without one
    for ev in past:
        if ev.get("ai_summary") or ev.get("summary_generated"):
            continue
        ev_dt = datetime.fromisoformat(ev["dt_utc"].replace("Z", "+00:00"))
        if (now - ev_dt).total_seconds() >= 1800:  # 30 min past
            summary = generate_event_summary(ev)
            if summary:
                ev["ai_summary"]        = summary
                ev["summary_generated"] = True
                log.info("Generated post-event summary for: %s", ev["event_name"])

    data = {
        "last_updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "upcoming":     upcoming,
        "past":         past,
    }
    _save_calendar(data)
    log.info("Calendar refreshed: %d upcoming, %d past events.", len(upcoming), len(past))
    return data


def check_upcoming_alerts():
    """Check for HIGH impact events exactly 2h away. Runs every 15 min via scheduler."""
    now  = datetime.now(timezone.utc)
    data = _load_calendar()
    changed = False

    for ev in data.get("upcoming", []):
        if ev.get("impact") != "High":
            continue
        if ev.get("briefing_sent"):
            continue
        ev_dt = datetime.fromisoformat(ev["dt_utc"].replace("Z", "+00:00"))
        mins_away = (ev_dt - now).total_seconds() / 60
        if 105 <= mins_away <= 135:  # 1h45m – 2h15m window
            briefing = generate_pre_event_briefing(ev)
            if briefing:
                ev["pre_event_briefing"] = briefing
                ev["briefing_sent"]      = True
                changed = True
                log.info("Pre-event briefing generated for: %s (in %.0f min)", ev["event_name"], mins_away)
                # Email users who have notifications enabled
                try:
                    from email_utils import send_email
                    users = User.query.filter_by(notify_enabled=True).all()
                    for u in users:
                        html = f"""
                        <div style="background:#0a0908;color:#d4c4a0;font-family:monospace;padding:40px;max-width:560px;margin:0 auto">
                          <div style="font-size:22px;color:#e8d8b0;font-weight:300;margin-bottom:4px">Commodex</div>
                          <div style="font-size:9px;color:#c8a870;letter-spacing:3px;margin-bottom:28px">RESEARCH TERMINAL</div>
                          <div style="font-size:11px;letter-spacing:2px;color:#ef4444;margin-bottom:12px">⚠ HIGH IMPACT EVENT IN ~2 HOURS</div>
                          <div style="font-size:14px;color:#e8d8b0;margin-bottom:8px">{ev['event_name']} ({ev['country']})</div>
                          <div style="font-size:11px;color:#6a5a40;margin-bottom:16px">{ev['date']} {ev['time']} UTC</div>
                          <p style="color:#c4b490;line-height:1.7;font-size:12px">{briefing}</p>
                          <div style="margin-top:16px;font-size:10px;color:#6a5a40">
                            Affected: {', '.join(ev['affected_commodities'])}
                          </div>
                        </div>"""
                        send_email(u.email, f"Commodex · Event Alert: {ev['event_name']} in ~2h", html)
                except Exception as e:
                    log.error("Alert email failed: %s", e)

    if changed:
        _save_calendar(data)


def send_daily_event_digest():
    """Every morning at 07:00 UTC, email all notification-enabled users a one-sentence
    summary of each High/Medium impact economic event happening today."""
    now      = datetime.now(timezone.utc)
    today    = now.date().isoformat()
    data     = _load_calendar()

    # Prevent double-send — store last digest date in calendar.json
    if data.get("daily_digest_date") == today:
        return
    data["daily_digest_date"] = today

    # Collect today's High + Medium events (sorted by time)
    todays_events = [
        ev for ev in data.get("upcoming", [])
        if ev["dt_utc"][:10] == today and ev.get("impact") in ("High", "Medium")
    ]
    todays_events.sort(key=lambda e: e["dt_utc"])

    if not todays_events:
        _save_calendar(data)
        return

    # Build email rows
    rows_html = ""
    for ev in todays_events:
        impact     = ev.get("impact", "Medium")
        dot_color  = "#ef5350" if impact == "High" else "#c8a870"
        comms      = ev.get("affected_commodities") or []
        comm_str   = ", ".join(comms) if comms else "general macro"
        ev_time    = ev.get("time", "") or ""
        sentence   = (
            f"{ev['event_name']} ({ev['country']}) is due today at {ev_time} UTC"
            f" — a key catalyst for {comm_str}."
        )
        rows_html += f"""
        <tr>
          <td style="padding:14px 20px;border-bottom:1px solid #1a1814;vertical-align:top;width:8px">
            <div style="width:6px;height:6px;border-radius:50%;background:{dot_color};margin-top:4px"></div>
          </td>
          <td style="padding:14px 20px 14px 8px;border-bottom:1px solid #1a1814">
            <div style="font-size:12px;color:#d4c4a0;line-height:1.6">{sentence}</div>
          </td>
        </tr>"""

    date_str  = now.strftime("%d %B %Y")
    link      = f"{os.environ.get('APP_URL', 'https://commodex.io')}/app"
    html = f"""
    <div style="background:#0a0908;color:#d4c4a0;font-family:monospace;padding:40px;max-width:560px;margin:0 auto">
      <div style="font-size:22px;color:#e8d8b0;font-weight:300;margin-bottom:4px">Commodex</div>
      <div style="font-size:9px;color:#c8a870;letter-spacing:3px;margin-bottom:28px">RESEARCH TERMINAL</div>
      <div style="font-size:11px;letter-spacing:2px;color:#c8a870;margin-bottom:4px">TODAY'S EVENTS</div>
      <div style="font-size:9px;color:#6a5a40;letter-spacing:1px;margin-bottom:20px">{date_str.upper()}</div>
      <table style="width:100%;border-collapse:collapse;border:1px solid #1e1c18;margin-bottom:24px">
        {rows_html}
      </table>
      <a href="{link}" style="display:inline-block;background:#c8a870;color:#0a0908;padding:11px 28px;text-decoration:none;font-size:11px;letter-spacing:2px">OPEN TERMINAL</a>
      <p style="margin-top:28px;color:#6a5a40;font-size:10px;line-height:1.6">
        You're receiving this because you enabled notifications on Commodex.<br>
        To unsubscribe, open the terminal and toggle off notifications in Settings.
      </p>
    </div>"""

    from email_utils import send_email
    users = User.query.filter_by(notify_on_analysis=True).all()
    sent  = 0
    for u in users:
        if send_email(u.email, f"Commodex · Events Today — {date_str}", html):
            sent += 1
    log.info("Daily event digest sent to %d users (%d events today).", sent, len(todays_events))
    _save_calendar(data)


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
    # Calendar: refresh every 6 hours, check alerts every 15 min, morning digest at 07:00 UTC
    schedule.every(6).hours.do(lambda: refresh_calendar() if not analysis_status["running"] else None)
    schedule.every(15).minutes.do(lambda: check_upcoming_alerts() if not analysis_status["running"] else None)
    schedule.every().day.at("01:30").do(send_daily_event_digest)  # 07:00 IST
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

def _pdf_safe(text: str) -> str:
    """Replace non-latin characters that Helvetica can't render."""
    if not text:
        return ""
    text = (text
        .replace("\u2014", "-").replace("\u2013", "-").replace("\u2019", "'").replace("\u2018", "'")
        .replace("\u201c", '"').replace("\u201d", '"').replace("\u2022", "-").replace("\u00b7", ".")
        .replace("\u25b2", "^").replace("\u25bc", "v").replace("\u2191", "^").replace("\u2193", "v")
        .replace("\u2192", "->").replace("\u2190", "<-").replace("\u2026", "...").replace("\u00a0", " ")
        .replace("\u00b1", "+/-").replace("\u00ae", "(R)").replace("\u00b0", "deg")
        .replace("\u00e9", "e").replace("\u00e8", "e").replace("\u00ea", "e").replace("\u00e0", "a")
    )
    # Strip any remaining non-latin-1 characters safely
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _pdf_page_bg(pdf):
    """Fill page with dark background and gold top/bottom rules."""
    pdf.set_fill_color(12, 14, 20)
    pdf.rect(0, 0, 210, 297, 'F')
    pdf.set_fill_color(200, 168, 112)
    pdf.rect(0, 0, 210, 1.5, 'F')
    pdf.rect(0, 295.5, 210, 1.5, 'F')


def _pdf_footer(pdf, page_num, total):
    pdf.set_xy(14, 287)
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(93, 100, 120)
    pdf.cell(91, 4, "commodex.io - AI-Powered Commodity Research", ln=False)
    pdf.set_x(105)
    pdf.cell(91, 4, f"Page {page_num} of {total} - {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')}", align="R")


def generate_newsletter_pdf(results: dict, prices: dict) -> bytes:
    from fpdf import FPDF
    COMMODITIES_ORDER = ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas",
                         "Corn", "Wheat", "Soybeans", "Coffee", "Sugar"]
    SENTIMENT_LABELS  = {"STRONG_BULLISH": "STRONG BULLISH", "BULLISH": "BULLISH",
                         "NEUTRAL": "NEUTRAL", "BEARISH": "BEARISH", "STRONG_BEARISH": "STRONG BEARISH"}
    SENTIMENT_COLORS  = {"STRONG_BULLISH": (38,166,154), "BULLISH": (38,166,154),
                         "NEUTRAL": (160,168,188), "BEARISH": (239,83,80), "STRONG_BEARISH": (239,83,80)}
    active = [c for c in COMMODITIES_ORDER if c in results]
    total_pages = 1 + len(active)  # cover + one per commodity

    pdf = FPDF()
    pdf.set_auto_page_break(auto=False)

    # ── COVER PAGE ────────────────────────────────────────────────────────────
    pdf.add_page()
    _pdf_page_bg(pdf)
    now_str = datetime.now(timezone.utc).strftime("%d %B %Y")
    week_start = (datetime.now(timezone.utc) - timedelta(days=datetime.now(timezone.utc).weekday())).strftime("%d %b")
    week_end   = (datetime.now(timezone.utc) + timedelta(days=6 - datetime.now(timezone.utc).weekday())).strftime("%d %b %Y")

    pdf.set_xy(14, 38)
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(200, 168, 112)
    pdf.cell(0, 14, "COMMODEX", ln=True)
    pdf.set_x(14)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(93, 100, 120)
    pdf.cell(0, 5, "WEEKLY INTELLIGENCE REPORT - COMMODITY RESEARCH", ln=True)
    pdf.set_x(14)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(160, 168, 188)
    pdf.cell(0, 5, f"{week_start} - {week_end}", ln=True)

    pdf.set_draw_color(30, 35, 54)
    pdf.set_line_width(0.4)
    pdf.line(14, pdf.get_y() + 6, 196, pdf.get_y() + 6)
    pdf.ln(14)

    # Summary table
    pdf.set_x(14)
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(93, 100, 120)
    pdf.cell(60, 5, "COMMODITY", ln=False)
    pdf.cell(45, 5, "SENTIMENT", ln=False)
    pdf.cell(40, 5, "PRICE", ln=False)
    pdf.cell(33, 5, "CHANGE", align="R", ln=True)
    pdf.set_draw_color(30, 35, 54); pdf.set_line_width(0.2)
    pdf.line(14, pdf.get_y(), 196, pdf.get_y())
    pdf.ln(2)

    for commodity in active:
        payload    = results[commodity]
        analysis   = payload.get("analysis", {})
        sent_raw   = analysis.get("sentiment", "NEUTRAL").upper()
        sent_lbl   = SENTIMENT_LABELS.get(sent_raw, sent_raw)
        sent_color = SENTIMENT_COLORS.get(sent_raw, (160,168,188))
        price_data = prices.get(commodity, {})
        price_val  = price_data.get("price")
        change_val = price_data.get("change")
        price_str  = f"${price_val:,.2f}" if price_val is not None else "-"
        change_str = (f"+{change_val:.2f}%" if change_val >= 0 else f"{change_val:.2f}%") if change_val is not None else ""

        y = pdf.get_y()
        pdf.set_fill_color(200, 168, 112)
        pdf.rect(14, y + 1, 1.5, 5, 'F')
        pdf.set_xy(17, y)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(209, 212, 220)
        pdf.cell(57, 7, commodity.upper(), ln=False)
        # Sentiment pill
        pdf.set_fill_color(*sent_color)
        pdf.set_text_color(10, 12, 18)
        pdf.set_font("Helvetica", "B", 6)
        pdf.set_xy(77, y + 1.5)
        pdf.cell(38, 4, f" {sent_lbl} ", align="C", fill=True)
        pdf.set_xy(117, y)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(209, 212, 220)
        pdf.cell(38, 7, price_str, align="R", ln=False)
        if change_val is not None:
            pdf.set_text_color(38,166,154) if change_val >= 0 else pdf.set_text_color(239,83,80)
        else:
            pdf.set_text_color(93,100,120)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_xy(157, y)
        pdf.cell(39, 7, change_str, align="R", ln=True)
        pdf.set_draw_color(20, 24, 36); pdf.set_line_width(0.1)
        pdf.line(14, pdf.get_y(), 196, pdf.get_y())

    pdf.ln(10)
    pdf.set_x(14)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(93, 100, 120)
    pdf.cell(0, 4, "For informational purposes only. Not financial advice. AI-generated - verify before trading.")
    _pdf_footer(pdf, 1, total_pages)

    # ── ONE PAGE PER COMMODITY ────────────────────────────────────────────────
    for page_idx, commodity in enumerate(active, start=2):
        pdf.add_page()
        _pdf_page_bg(pdf)
        payload    = results[commodity]
        analysis   = payload.get("analysis", {})
        sent_raw   = analysis.get("sentiment", "NEUTRAL").upper()
        sent_lbl   = SENTIMENT_LABELS.get(sent_raw, sent_raw)
        sent_color = SENTIMENT_COLORS.get(sent_raw, (160,168,188))
        meta       = payload.get("meta", {})
        price_data = prices.get(commodity, {})
        price_val  = price_data.get("price")
        change_val = price_data.get("change")
        price_str  = f"${price_val:,.2f}" if price_val is not None else "-"
        change_str = (f"+{change_val:.2f}%" if change_val >= 0 else f"{change_val:.2f}%") if change_val is not None else ""
        conf       = analysis.get("confidence", "")
        conf_str   = f"{conf}% confidence" if conf and str(conf).isdigit() else ""

        # Header bar
        pdf.set_fill_color(*sent_color)
        pdf.rect(0, 0, 4, 297, 'F')

        # Commodity name
        pdf.set_xy(14, 14)
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_text_color(209, 212, 220)
        pdf.cell(0, 10, commodity.upper(), ln=True)

        # Ticker / exchange row
        ticker = meta.get("ticker", "") or payload.get("ticker", "")
        exch   = meta.get("exchange", "") or payload.get("exchange", "")
        if ticker:
            pdf.set_x(14)
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(93, 100, 120)
            pdf.cell(0, 4, f"{ticker}  ·  {exch}  ·  {now_str}", ln=True)

        # Sentiment + price row
        pdf.set_xy(14, 38)
        pdf.set_fill_color(*sent_color)
        pdf.set_text_color(10, 12, 18)
        pdf.set_font("Helvetica", "B", 8)
        pdf.cell(48, 6, f"  {sent_lbl}  ", align="C", fill=True)
        if conf_str:
            pdf.set_x(66)
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(93, 100, 120)
            pdf.cell(40, 6, conf_str, ln=False)
        pdf.set_xy(130, 38)
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(209, 212, 220)
        pdf.cell(40, 6, price_str, align="R", ln=False)
        if change_val is not None:
            pdf.set_text_color(38,166,154) if change_val >= 0 else pdf.set_text_color(239,83,80)
        else:
            pdf.set_text_color(93,100,120)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_xy(172, 39)
        pdf.cell(24, 5, change_str, align="R")

        pdf.set_draw_color(30, 35, 54); pdf.set_line_width(0.3)
        pdf.line(14, 48, 196, 48)
        pdf.set_xy(14, 51)

        # AI Analysis summary
        summary = analysis.get("market_summary", "")
        if summary and summary != "Coming soon.":
            pdf.set_font("Helvetica", "B", 7)
            pdf.set_text_color(200, 168, 112)
            pdf.cell(0, 4, "AI ANALYSIS", ln=True)
            pdf.set_x(14)
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(160, 168, 188)
            pdf.multi_cell(182, 4.8, _pdf_safe(summary))
            pdf.ln(4)

        # Drivers — combined flowing paragraph
        drivers   = analysis.get("drivers", {})
        bull_list = drivers.get("up") or []
        bear_list = drivers.get("down") or []
        if bull_list or bear_list:
            parts = []
            if bull_list:
                if len(bull_list) == 1:
                    parts.append(f"The primary bullish case rests on {bull_list[0].rstrip('.')}.")
                else:
                    joined = ", ".join(bull_list[:-1]) + f", and {bull_list[-1].rstrip('.')}"
                    parts.append(f"The bullish case for {commodity} is supported by {joined}.")
            if bear_list:
                if len(bear_list) == 1:
                    parts.append(f"On the downside, {bear_list[0].rstrip('.')} presents a notable headwind.")
                else:
                    joined = ", ".join(bear_list[:-1]) + f", and {bear_list[-1].rstrip('.')}"
                    parts.append(f"Headwinds include {joined}.")
            driver_para = " ".join(parts)
            pdf.set_x(14)
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(160, 168, 188)
            pdf.multi_cell(182, 4.8, _pdf_safe(driver_para))
            pdf.ln(4)

        # Dominant narrative / takeaway
        narrative = analysis.get("dominant_narrative", {})
        takeaway  = (narrative.get("theme", "") if isinstance(narrative, dict) else "") or \
                    (analysis.get("takeaway", {}) or {}).get("strategy", "")
        if takeaway and takeaway not in ("-", "Coming soon"):
            pdf.set_fill_color(200, 168, 112)
            pdf.rect(14, pdf.get_y(), 2, 3.5, 'F')
            pdf.set_x(18)
            pdf.set_font("Helvetica", "I", 9)
            pdf.set_text_color(200, 168, 112)
            pdf.multi_cell(178, 4.8, _pdf_safe(takeaway))
            pdf.ln(4)

        # Signals — flowing paragraph
        signals = analysis.get("signals") or []
        if signals:
            sig_texts = []
            for s in signals[:5]:
                t = s if isinstance(s, str) else (s.get("signal") or s.get("text") or str(s))
                sig_texts.append(t.strip().rstrip("."))
            if sig_texts:
                pdf.set_draw_color(30, 35, 54); pdf.set_line_width(0.2)
                pdf.line(14, pdf.get_y(), 196, pdf.get_y())
                pdf.ln(3)
                pdf.set_x(14)
                pdf.set_font("Helvetica", "B", 7)
                pdf.set_text_color(200, 168, 112)
                pdf.cell(0, 4, "KEY SIGNALS", ln=True)
                pdf.set_x(14)
                sig_para = "; ".join(sig_texts) + "."
                pdf.set_font("Helvetica", "", 9)
                pdf.set_text_color(160, 168, 188)
                pdf.multi_cell(182, 4.8, _pdf_safe(sig_para))

        _pdf_footer(pdf, page_idx, total_pages)

    return bytes(pdf.output())


@app.route("/newsletter")
@jwt_required()
def download_newsletter():
    try:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"Commodex_Weekly_{date_str}.pdf"

        # Serve pre-generated Sunday PDF if fresh (generated within last 7 days)
        weekly_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekly_report.pdf")
        if os.path.exists(weekly_path):
            age_days = (datetime.now(timezone.utc).timestamp() - os.path.getmtime(weekly_path)) / 86400
            if age_days < 7:
                with open(weekly_path, "rb") as f:
                    pdf_bytes = f.read()
                return Response(pdf_bytes, mimetype="application/pdf",
                                headers={"Content-Disposition": f'attachment; filename="{filename}"',
                                         "Content-Length": str(len(pdf_bytes))})

        # Fall back to on-demand generation
        load_latest_from_db()
        if not latest_results:
            return jsonify({"error": "No analysis data available yet. Run analysis first."}), 404
        prices    = fetch_live_prices()
        pdf_bytes = generate_newsletter_pdf(latest_results, prices)
        return Response(pdf_bytes, mimetype="application/pdf",
                        headers={"Content-Disposition": f'attachment; filename="{filename}"',
                                 "Content-Length": str(len(pdf_bytes))})
    except Exception as e:
        log.error("Newsletter generation failed: %s", e, exc_info=True)
        return jsonify({"error": f"Failed to generate newsletter: {str(e)}"}), 500


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


# ── Aria AI Chat Agent ──────────────────────────────────────────────────────────
ARIA_TOOLS = [
    {
        "name": "trigger_analysis",
        "description": "Trigger a fresh analysis run across all commodities. Use this when the user asks to run analysis, refresh data, or update the AI analysis.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "set_alert",
        "description": "Enable or disable a sentiment-change alert for a specific commodity for the current user.",
        "input_schema": {
            "type": "object",
            "properties": {
                "commodity": {"type": "string", "description": "Commodity name e.g. Gold, Silver, Crude Oil, Copper, Natural Gas, Corn, Wheat, Soybeans, Coffee, Sugar"},
                "enabled":   {"type": "boolean", "description": "True to enable the alert, False to disable/remove it"}
            },
            "required": ["commodity", "enabled"]
        }
    },
    {
        "name": "get_sentiment_history",
        "description": "Retrieve the recent sentiment history for a specific commodity to show how its AI rating has changed over time.",
        "input_schema": {
            "type": "object",
            "properties": {
                "commodity": {"type": "string", "description": "Commodity name"},
                "days":      {"type": "integer", "description": "Days of history to fetch (default 7, max 30)"}
            },
            "required": ["commodity"]
        }
    },
]

VALID_ALERT_COMMODITIES = ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas", "Corn", "Wheat", "Soybeans", "Coffee", "Sugar"]

def _aria_execute_tool(tool_name, tool_input, user_id):
    """Execute a tool call from Aria and return a result dict."""
    if tool_name == "trigger_analysis":
        user = User.query.get(user_id)
        if not user or not user.is_admin:
            return {"success": False, "message": "Only admin users can trigger analysis runs."}
        if analysis_status["running"]:
            return {"success": False, "message": "Analysis is already running. Please wait for it to finish."}
        thread = threading.Thread(target=_run_in_context, daemon=True)
        thread.start()
        return {"success": True, "message": "Analysis run started successfully. It will complete in ~60 seconds."}

    elif tool_name == "set_alert":
        commodity = tool_input.get("commodity", "")
        enabled   = bool(tool_input.get("enabled", True))
        if commodity not in VALID_ALERT_COMMODITIES:
            return {"success": False, "message": f"'{commodity}' is not a valid commodity. Valid options: {', '.join(VALID_ALERT_COMMODITIES)}"}
        alert = UserAlert.query.filter_by(user_id=user_id, commodity=commodity).first()
        if alert:
            alert.enabled = enabled
        else:
            alert = UserAlert(user_id=user_id, commodity=commodity, enabled=enabled)
            db.session.add(alert)
        db.session.commit()
        action = "enabled" if enabled else "disabled"
        return {"success": True, "message": f"Alert for {commodity} has been {action}."}

    elif tool_name == "get_sentiment_history":
        commodity = tool_input.get("commodity", "")
        days      = min(int(tool_input.get("days", 7)), 30)
        since     = datetime.now(timezone.utc) - timedelta(days=days)
        rows      = (AnalysisRun.query
                     .filter_by(commodity=commodity)
                     .filter(AnalysisRun.run_at >= since)
                     .order_by(AnalysisRun.run_at.desc())
                     .limit(20).all())
        if not rows:
            return {"commodity": commodity, "history": [], "message": f"No analysis data for {commodity} in the last {days} days."}
        history = [{"date": r.run_at.strftime("%d %b %H:%M UTC"), "sentiment": r.sentiment} for r in rows]
        return {"commodity": commodity, "days": days, "history": history}

    return {"success": False, "message": f"Unknown tool: {tool_name}"}


@app.route("/ai/chat", methods=["POST"])
@jwt_required()
def aria_chat():
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "AI not configured."}), 503

    user_id  = int(get_jwt_identity())
    data     = request.get_json(silent=True) or {}
    messages = data.get("messages", [])
    if not messages:
        return jsonify({"error": "No messages provided."}), 400

    messages = messages[-20:]  # cap history

    # Build commodity context
    all_runs = AnalysisRun.query.order_by(AnalysisRun.run_at.desc()).all()
    seen, rows = set(), []
    for r in all_runs:
        if r.commodity not in seen:
            seen.add(r.commodity)
            rows.append(r)

    price_data = fetch_live_prices()
    ctx_lines  = []
    for r in rows:
        d    = r.data or {}
        an   = d.get("analysis", d)
        sent = an.get("sentiment", r.sentiment or "NEUTRAL")
        summ = an.get("market_summary", "")[:500]
        up   = "; ".join((an.get("drivers") or {}).get("up",   [])[:3])
        dn   = "; ".join((an.get("drivers") or {}).get("down", [])[:2])
        px   = price_data.get(r.commodity, {})
        pstr = f"${px['price']:.2f} ({'+' if px.get('change',0)>=0 else ''}{px.get('change',0):.2f}%)" if px.get("price") else "N/A"
        age  = f" (run {r.run_at.strftime('%d %b %H:%M UTC')})" if r.run_at else ""
        ctx_lines.append(
            f"### {r.commodity}{age}\nSentiment: {sent} | Price: {pstr}\n"
            f"Bullish: {up or 'N/A'}\nBearish risks: {dn or 'N/A'}\nSummary: {summ}"
        )

    # Check if user is admin (used by trigger_analysis tool)
    user    = User.query.get(user_id)
    is_admin = user and user.is_admin

    context = "\n\n".join(ctx_lines) if ctx_lines else "No analysis data available yet."
    today   = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")

    system_prompt = f"""You are Aria, an expert AI commodity analyst for Commodex — a professional commodity research terminal. Today is {today}.
{"You have admin privileges and can trigger analysis runs." if is_admin else "You do not have admin privileges."}

Latest commodity analysis:

{context}

---
Guidelines:
- Be concise but insightful
- When generating trade ideas, be specific: direction, entry, target, stop, thesis, key risk
- Always briefly note "not financial advice" when giving trade ideas
- Cite specific prices and drivers from the data above
- You can perform actions using your tools: trigger analysis runs, set/remove alerts, check sentiment history
- Format responses cleanly with bullet points or numbered lists when appropriate"""

    client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    claude_messages = [{"role": m["role"], "content": m["content"]} for m in messages]
    actions_taken = []

    # Agentic loop — execute tools if Claude calls them (max 4 iterations)
    for _ in range(4):
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=system_prompt,
            tools=ARIA_TOOLS,
            messages=claude_messages
        )

        if response.stop_reason == "end_turn":
            reply = next((b.text for b in response.content if hasattr(b, "text")), "")
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = _aria_execute_tool(block.name, block.input, user_id)
                    actions_taken.append({"tool": block.name, "result": result})
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result)
                    })
            claude_messages.append({"role": "assistant", "content": response.content})
            claude_messages.append({"role": "user",      "content": tool_results})
        else:
            reply = next((b.text for b in response.content if hasattr(b, "text")), "")
            break
    else:
        reply = next((b.text for b in response.content if hasattr(b, "text")), "Something went wrong.")

    return jsonify({"reply": reply, "actions": actions_taken})


# ── Trade Ideas ────────────────────────────────────────────────────────────────
@app.route("/trade-ideas", methods=["POST"])
@jwt_required()
def trade_ideas():
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "AI not configured."}), 503

    # Pull latest run per commodity
    all_runs = AnalysisRun.query.order_by(AnalysisRun.run_at.desc()).all()
    seen, rows = set(), []
    for r in all_runs:
        if r.commodity not in seen:
            seen.add(r.commodity)
            rows.append(r)

    if not rows:
        return jsonify({"error": "No analysis data available. Run analysis first."}), 404

    # Build context summary for each commodity
    price_data = fetch_live_prices()
    lines = []
    for r in rows:
        d    = r.data or {}
        an   = d.get("analysis", d)  # handle both wrapped and flat structures
        sent = an.get("sentiment", r.sentiment or "NEUTRAL")
        conf = an.get("confidence", "LOW")
        summ = an.get("market_summary", "")[:300]
        up   = "; ".join((an.get("drivers") or {}).get("up",   [])[:3])
        dn   = "; ".join((an.get("drivers") or {}).get("down", [])[:2])
        px   = price_data.get(r.commodity, {})
        price_str = f"${px['price']:.2f} ({'+' if px.get('change',0)>=0 else ''}{px.get('change',0):.2f}%)" if px.get("price") else "N/A"
        lines.append(
            f"- {r.commodity}: {sent} | Confidence: {conf} | Price: {price_str}\n"
            f"  Bullish: {up or 'N/A'}\n"
            f"  Risks: {dn or 'N/A'}\n"
            f"  Summary: {summ}"
        )

    context = "\n\n".join(lines)
    today   = datetime.now(timezone.utc).strftime("%d %b %Y")

    prompt = f"""You are a senior commodity strategist at a global macro hedge fund. Today is {today}.

Below is the latest AI-generated analysis for each commodity we track:

{context}

---

Based on this analysis, generate 4-6 specific trade ideas. For each idea:
- Focus on commodities with HIGH or MEDIUM confidence and strong directional bias
- Look for thematic connections across commodities where relevant (e.g. energy complex, soft commodities weather play, USD sensitivity)
- Consider relative value plays where two commodities diverge

For each trade idea return a JSON object with these fields:
- "commodity": the commodity name (or "SPREAD: X vs Y" for relative value)
- "direction": "LONG" or "SHORT" or "SPREAD"
- "thesis": 2-3 sentence rationale drawing on the specific drivers above. Be precise and cite actual data points.
- "entry_note": what level or condition to look for entry
- "watch": the single biggest risk or catalyst to monitor
- "conviction": "HIGH", "MEDIUM", or "LOW"

Return ONLY a valid JSON array of idea objects. No markdown, no text before or after:

[
  {{
    "commodity": "...",
    "direction": "...",
    "thesis": "...",
    "entry_note": "...",
    "watch": "...",
    "conviction": "..."
  }}
]"""

    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = message.content[0].text.strip()
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"): part = part[4:].strip()
            if part.startswith("["): raw = part; break
    start = raw.find("[")
    end   = raw.rfind("]")
    if start != -1 and end != -1:
        raw = raw[start:end+1]
    ideas = json.loads(raw)
    return jsonify({"ideas": ideas, "generated_at": today})


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


@app.route("/macro")
@jwt_required()
def get_macro():
    """Return the latest macro key-data snapshot for the Key Data Panel."""
    try:
        with open(MACRO_CACHE_FILE, "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception:
        return jsonify({"error": "Macro data not yet available. Run analysis first."}), 404


@app.route("/calendar")
@jwt_required()
def get_calendar():
    """Return upcoming and past economic events with AI summaries."""
    data = _load_calendar()
    now  = datetime.now(timezone.utc)
    # If stale (>6h) or empty, refresh inline
    last = data.get("last_updated")
    if not last or (now - datetime.fromisoformat(last.replace("Z", "+00:00"))).total_seconds() > 21600:
        try:
            data = refresh_calendar()
        except Exception as e:
            log.warning("Inline calendar refresh failed: %s", e)
    upcoming = data.get("upcoming", [])[:20]
    # If ForexFactory returned nothing, fall back to next 7 days of scheduled events
    if not upcoming:
        scheduled = _generate_scheduled_events(months=1)
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=7)
        upcoming = [e for e in scheduled if now.strftime("%Y-%m-%d") <= e.get("date","") <= cutoff.strftime("%Y-%m-%d")]
    return jsonify({
        "upcoming_events": upcoming[:20],
        "past_events":     data.get("past", [])[:5],
        "last_updated":    data.get("last_updated"),
    })


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
                .filter(User.is_active == True)
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


# ── PWA static files ──────────────────────────────────────────────────────────
@app.route("/manifest.json")
def pwa_manifest():
    from flask import Response
    return send_from_directory(".", "manifest.json", mimetype="application/manifest+json")


@app.route("/sw.js")
def pwa_sw():
    from flask import Response
    resp = send_from_directory(".", "sw.js", mimetype="application/javascript")
    resp.headers["Service-Worker-Allowed"] = "/"
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/offline")
def pwa_offline():
    return send_from_directory(".", "offline.html")


@app.route("/icon.svg")
def pwa_icon_svg():
    return send_from_directory(".", "icon.svg", mimetype="image/svg+xml")


@app.route("/icon-192.png")
@app.route("/icon-512.png")
def pwa_icon_png():
    """Serve SVG as fallback for PNG icon requests."""
    return send_from_directory(".", "icon.svg", mimetype="image/svg+xml")


# ── 6-month scheduled economic calendar ──────────────────────────────────────
def _generate_scheduled_events(months=6):
    """
    Generate known recurring economic events for the next 6 months.
    Returns list of {date, title, impact, commodities, category}.
    """
    now = datetime.now(timezone.utc)
    events = []

    # FOMC 2026 meeting dates (Fed published schedule)
    fomc_dates = [
        "2026-01-29", "2026-03-19", "2026-05-07", "2026-06-18",
        "2026-07-29", "2026-09-17", "2026-10-29", "2026-12-10",
    ]
    for d in fomc_dates:
        dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if now <= dt <= now + timedelta(days=months * 31):
            events.append({
                "date": d, "title": "FOMC Interest Rate Decision",
                "impact": "HIGH", "category": "central_bank",
                "commodities": ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"],
                "note": "Fed rate decision — major USD & precious metals mover",
            })

    # Helper: find nth weekday in a month (0=Mon..6=Sun per calendar module)
    def nth_weekday(year, month, weekday, n=1):
        """Return date of nth weekday (1-based) in given month."""
        first = datetime(year, month, 1, tzinfo=timezone.utc)
        day_offset = (weekday - first.weekday()) % 7
        result = first + timedelta(days=day_offset + (n - 1) * 7)
        if result.month != month:
            return None
        return result

    # Monthly: iterate over the next 6 months
    start_year, start_month = now.year, now.month
    for i in range(months + 1):
        m = start_month + i
        y = start_year + (m - 1) // 12
        m = ((m - 1) % 12) + 1

        # Non-Farm Payrolls — first Friday of month
        nfp = nth_weekday(y, m, 4, 1)  # weekday 4 = Friday
        if nfp and nfp >= now:
            events.append({
                "date": nfp.strftime("%Y-%m-%d"),
                "title": "US Non-Farm Payrolls",
                "impact": "HIGH", "category": "employment",
                "commodities": ["Gold", "Silver", "Crude Oil"],
                "note": "USD mover — strong jobs = Fed hawkish pressure on gold",
            })

        # US CPI — approx 2nd week, Wednesday (estimate; BLS publishes exact dates)
        cpi = nth_weekday(y, m, 2, 2)  # 2nd Wednesday
        if cpi and cpi >= now:
            events.append({
                "date": cpi.strftime("%Y-%m-%d"),
                "title": "US CPI Inflation (est.)",
                "impact": "HIGH", "category": "inflation",
                "commodities": ["Gold", "Silver", "Crude Oil", "Copper"],
                "note": "High CPI = bullish gold/silver; oil impact via demand",
            })

        # PCE Price Index — last Friday of month
        last_day = calendar.monthrange(y, m)[1]
        last_date = datetime(y, m, last_day, tzinfo=timezone.utc)
        # Find last Friday
        offset = (last_date.weekday() - 4) % 7
        pce = last_date - timedelta(days=offset)
        if pce >= now:
            events.append({
                "date": pce.strftime("%Y-%m-%d"),
                "title": "US PCE Price Index (est.)",
                "impact": "MEDIUM", "category": "inflation",
                "commodities": ["Gold", "Silver"],
                "note": "Fed's preferred inflation gauge",
            })

        # EIA Crude Inventories — every Wednesday (weekly, pick 3rd Wed as representative)
        eia_crude = nth_weekday(y, m, 2, 3)  # 3rd Wednesday
        if eia_crude and eia_crude >= now:
            events.append({
                "date": eia_crude.strftime("%Y-%m-%d"),
                "title": "EIA Crude Oil Inventories",
                "impact": "MEDIUM", "category": "energy",
                "commodities": ["Crude Oil"],
                "note": "Weekly draw/build vs expectations",
            })

        # EIA Natural Gas Storage — every Thursday (3rd Thursday)
        eia_gas = nth_weekday(y, m, 3, 3)  # 3rd Thursday
        if eia_gas and eia_gas >= now:
            events.append({
                "date": eia_gas.strftime("%Y-%m-%d"),
                "title": "EIA Natural Gas Storage",
                "impact": "MEDIUM", "category": "energy",
                "commodities": ["Natural Gas"],
                "note": "Weekly storage change vs expectations",
            })

        # OPEC Monthly Oil Market Report — approx 2nd week
        opec_rep = nth_weekday(y, m, 3, 2)  # 2nd Thursday
        if opec_rep and opec_rep >= now:
            events.append({
                "date": opec_rep.strftime("%Y-%m-%d"),
                "title": "OPEC Monthly Oil Market Report",
                "impact": "MEDIUM", "category": "opec",
                "commodities": ["Crude Oil"],
                "note": "Demand/supply outlook revision",
            })

        # Baker Hughes Rig Count — every Friday (3rd Friday)
        rig = nth_weekday(y, m, 4, 3)  # 3rd Friday
        if rig and rig >= now:
            events.append({
                "date": rig.strftime("%Y-%m-%d"),
                "title": "Baker Hughes Rig Count",
                "impact": "LOW", "category": "energy",
                "commodities": ["Crude Oil", "Natural Gas"],
                "note": "US drilling activity indicator",
            })

        # China PMI — 1st weekday of month (NBS releases around 31st/1st)
        china_pmi = nth_weekday(y, m, 0, 1)  # 1st Monday
        if china_pmi and china_pmi >= now:
            events.append({
                "date": china_pmi.strftime("%Y-%m-%d"),
                "title": "China Manufacturing PMI",
                "impact": "MEDIUM", "category": "pmi",
                "commodities": ["Copper", "Crude Oil", "Natural Gas"],
                "note": "World's largest commodity consumer",
            })

        # US ISM Manufacturing PMI — 1st business day of month
        ism = nth_weekday(y, m, 0, 1)  # 1st Monday
        if ism and ism >= now:
            events.append({
                "date": ism.strftime("%Y-%m-%d"),
                "title": "US ISM Manufacturing PMI",
                "impact": "MEDIUM", "category": "pmi",
                "commodities": ["Copper", "Crude Oil"],
                "note": "US industrial demand indicator",
            })

    # Known OPEC+ production meetings (estimated 2026)
    opec_meetings = ["2026-06-01", "2026-12-01"]
    for d in opec_meetings:
        dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if now <= dt <= now + timedelta(days=months * 31):
            events.append({
                "date": d, "title": "OPEC+ Production Meeting",
                "impact": "HIGH", "category": "opec",
                "commodities": ["Crude Oil"],
                "note": "Output quota decision",
            })

    # Sort by date and deduplicate
    seen = set()
    unique = []
    for e in sorted(events, key=lambda x: x["date"]):
        key = (e["date"], e["title"])
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique


@app.route("/calendar/scheduled")
@jwt_required()
def get_scheduled_calendar():
    """Return 6-month forward-looking calendar of known economic events."""
    events = _generate_scheduled_events(months=6)
    return jsonify({"events": events, "generated_at": datetime.now(timezone.utc).isoformat()})

# ══════════════════════════════════════════════════════════════════════════════
# START
# ══════════════════════════════════════════════════════════════════════════════

def load_latest_from_db():
    """Populate latest_results from the most recent DB row per commodity."""
    global latest_results
    try:
        loaded = {}
        for commodity in COMMODITIES.keys():
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