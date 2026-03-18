"""
Signal Engine — fast keyword-based pre-filter before the AI agent.
Scores articles for market-moving potential with zero AI cost.
Called every 15 minutes alongside news ingestion.
"""

# (pattern, confidence_score) pairs per commodity
_COMMODITY_PATTERNS: dict[str, list[tuple[str, int]]] = {
    "Gold": [
        ("federal reserve", 85), ("rate cut", 85), ("rate hike", 85),
        ("fomc", 90), ("powell", 85), ("inflation data", 80),
        ("gold record", 90), ("all-time high", 90), ("gold rally", 78),
        ("gold crash", 90), ("gold drops", 72), ("gold surges", 82),
        ("safe haven", 74), ("sanctions", 78), ("geopolit", 72),
        ("dollar falls", 78), ("gold etf", 75), ("central bank gold", 82),
        ("rbi gold", 75), ("china gold", 78), ("gold demand", 68),
        ("gold mine", 70), ("gold output", 70), ("war", 72), ("conflict", 68),
    ],
    "Crude Oil": [
        ("opec", 90), ("opec+", 90), ("production cut", 90),
        ("production increase", 88), ("supply cut", 85),
        ("iran", 80), ("russia oil", 85), ("sanctions", 85),
        ("strategic petroleum reserve", 82), ("spr release", 82),
        ("crude inventory", 85), ("oil inventory", 82), ("eia weekly", 85),
        ("oil surplus", 80), ("oil shortage", 85), ("refinery outage", 82),
        ("pipeline disruption", 87), ("hurricane", 78), ("tanker attack", 88),
        ("demand surge", 80), ("demand drop", 80), ("china oil", 75),
        ("oil record", 85), ("oil crash", 90), ("brent rally", 80),
    ],
    "Silver": [
        ("silver record", 87), ("silver rally", 78), ("silver crash", 90),
        ("solar demand", 82), ("solar panel", 78), ("ev demand", 72),
        ("silver shortage", 85), ("silver etf", 72), ("comex silver", 75),
        ("silver mine strike", 88), ("industrial silver", 72),
        ("federal reserve", 78), ("inflation data", 70),
        ("precious metal", 68), ("silver investment", 70),
    ],
    "Copper": [
        ("copper record", 87), ("copper rally", 78), ("copper crash", 90),
        ("lme copper", 82), ("comex copper", 80), ("copper inventory", 82),
        ("copper shortage", 87), ("copper surplus", 75), ("mine strike", 90),
        ("chile copper", 82), ("peru copper", 80), ("copper production", 72),
        ("ev demand", 75), ("infrastructure plan", 72), ("china gdp", 75),
        ("industrial output", 70), ("warehouse stocks", 72),
    ],
    "Natural Gas": [
        ("henry hub", 82), ("ttf gas", 80), ("natural gas record", 87),
        ("gas storage", 85), ("eia gas storage", 88), ("gas inventory", 82),
        ("lng export", 75), ("lng terminal", 78), ("pipeline disruption", 87),
        ("russia gas", 87), ("europe gas", 82), ("gas shortage", 90),
        ("gas surplus", 75), ("cold snap", 82), ("polar vortex", 85),
        ("heatwave demand", 78), ("gas rally", 78), ("gas crash", 90),
        ("winter supply", 80), ("freeze", 72),
    ],
}

_GENERIC_PATTERNS: list[tuple[str, int]] = [
    ("interest rate decision", 88),
    ("emergency rate cut", 92),
    ("bank of england", 72),
    ("ecb rate", 78),
    ("cpi data", 82),
    ("inflation report", 80),
    ("gdp contraction", 82),
    ("recession fears", 78),
    ("market crash", 90),
    ("trade war", 78),
    ("tariff imposed", 75),
    ("dollar index record", 78),
    ("nonfarm payroll", 72),
    ("military action", 82),
    ("sanctions package", 85),
]


def score_article(article: dict, commodity: str) -> int:
    """
    Return a confidence score (0–100) for whether this article is
    a market-moving signal for the given commodity.  0 = not a signal.
    """
    text = (article.get("title", "") + " " + article.get("summary", "")).lower()
    best = 0

    for pattern, score in _COMMODITY_PATTERNS.get(commodity, []):
        if pattern in text:
            best = max(best, score)

    for pattern, score in _GENERIC_PATTERNS:
        if pattern in text:
            best = max(best, score)

    return best


def get_signal_candidates(
    articles_by_commodity: dict,
    threshold: int = 65,
) -> list[tuple[dict, str, int]]:
    """
    Score all articles and return candidates above the threshold.
    Returns (article, commodity, keyword_confidence) tuples sorted desc.
    """
    candidates: list[tuple[dict, str, int]] = []

    for commodity, articles in articles_by_commodity.items():
        for article in articles:
            s = score_article(article, commodity)
            if s >= threshold:
                candidates.append((article, commodity, s))

    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates
