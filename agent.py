"""
Commodex AI Agent — classifies high-scoring news articles as market signals.
Uses Claude Haiku (fast + cheap) for per-article signal classification.
Only receives articles that already passed signal_engine keyword scoring.
"""

import json
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_PROMPT = """\
You are an AI Commodity Intelligence Agent for COMMODEX.
Determine if the following news article is a MARKET-MOVING event for {commodity}.

Do NOT summarize. Detect genuine market impact only.

ARTICLE:
Title: {title}
Source: {source}
Summary: {summary}

If this IS a market-moving event, return ONLY this JSON (no markdown):
{{
  "signal": true,
  "event": "one sentence describing the specific market-moving event",
  "commodity": "{commodity}",
  "impact": "bullish or bearish or neutral",
  "reason": "1-2 sentences on why this moves {commodity} prices",
  "confidence": 0-100,
  "signal_strength": 1-10,
  "so_what": "one powerful line: SO WHAT does this mean for prices? e.g. Supply tightening → upward pressure on oil"
}}

If this is NOT a market-moving event, return ONLY:
{{"signal": false, "confidence": 0, "signal_strength": 0, "so_what": ""}}

Rules:
- signal=true ONLY if this realistically moves {commodity} prices today or this week
- Routine price updates, analyst opinions, minor data releases = signal=false
- Central bank decisions, supply disruptions, geopolitical events, demand shocks = signal=true
- confidence = certainty that this is genuinely market-moving (0-100)
- signal_strength = magnitude of potential price impact (1=minor mention, 5=notable event, 10=extreme market mover)
"""


def _parse_json(raw: str) -> dict | None:
    """Extract and parse first JSON object from Claude response."""
    raw = raw.strip()
    if "```" in raw:
        for part in raw.split("```"):
            part = part.strip().lstrip("json").strip()
            if part.startswith("{"):
                raw = part
                break
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start == -1 or end <= start:
        return None
    try:
        return json.loads(raw[start:end])
    except json.JSONDecodeError:
        return None


def classify_article(client, article: dict, commodity: str) -> dict | None:
    """
    Run Claude Haiku classification on a single article.
    Returns a signal dict (with added timestamp + source_title) or None.
    """
    try:
        prompt = _PROMPT.format(
            commodity=commodity,
            title=article.get("title", ""),
            source=article.get("source", ""),
            summary=article.get("summary", "")[:400],
        )
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        result = _parse_json(msg.content[0].text)
        if result:
            result["timestamp"]    = datetime.now(timezone.utc).isoformat()
            result["source_title"] = article.get("title", "")
        return result
    except Exception as e:
        log.warning("Agent classify failed [%s]: %s", article.get("title", "")[:60], e)
        return None


def run_agent(client, candidates: list) -> list:
    """
    Process (article, commodity, kw_confidence) candidates through Claude.

    - Capped at 10 articles per run to control API cost.
    - Blends keyword confidence (30%) with AI confidence (70%).
    - Returns confirmed signals with blended confidence >= 60.
    """
    signals = []

    for article, commodity, kw_conf in candidates[:10]:
        result = classify_article(client, article, commodity)
        if not result or not result.get("signal"):
            continue

        ai_conf = max(0, min(100, int(result.get("confidence", 0))))
        blended = round(kw_conf * 0.3 + ai_conf * 0.7)
        result["confidence"] = blended

        if blended >= 60:
            signals.append(result)
            log.info(
                "Signal detected: [%s] %s | %s | conf=%d",
                commodity,
                result.get("event", "")[:70],
                result.get("impact", ""),
                blended,
            )

    return signals
