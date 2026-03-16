import feedparser
from datetime import datetime

# All the news websites we pull from
NEWS_SOURCES = [
    "https://economictimes.indiatimes.com/news/international/rssfeeds/2647163.cms",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/business.xml",
    "https://www.thehindu.com/business/Economy/feeder/default.rss",
]

# Keywords for each commodity
COMMODITIES = {
    "Gold":        ["gold", "bullion", "xau"],
    "Crude Oil":   ["crude", "oil", "wti", "brent", "petroleum", "opec"],
    "Silver":      ["silver", "xag"],
    "Copper":      ["copper", "lme"],
    "Natural Gas": ["natural gas", "natgas", "lng"],
}

# Google News searches
GOOGLE_SEARCHES = {
    "Gold":        "gold commodity MCX India price",
    "Crude Oil":   "crude oil WTI MCX India price",
    "Silver":      "silver commodity MCX India price",
    "Copper":      "copper LME MCX India price",
    "Natural Gas": "natural gas MCX India price",
}


def fetch_all_articles():
    all_articles = []

    print("Fetching from Indian news sources...")
    for url in NEWS_SOURCES:
        try:
            feed = feedparser.parse(url)
            name = feed.feed.get("title", url)
            count = 0
            for entry in feed.entries[:30]:
                all_articles.append({
                    "title":     entry.get("title", "").strip(),
                    "summary":   entry.get("summary", "").strip(),
                    "url":       entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "source":    name,
                })
                count += 1
            print(f"  OK  {name}: {count} articles")
        except Exception as e:
            print(f"  FAILED  {url}: {e}")

    print("\nFetching from Google News...")
    for commodity, search in GOOGLE_SEARCHES.items():
        try:
            url = f"https://news.google.com/rss/search?q={search.replace(' ', '+')}&hl=en-IN&gl=IN&ceid=IN:en"
            feed = feedparser.parse(url)
            count = 0
            for entry in feed.entries[:10]:
                all_articles.append({
                    "title":     entry.get("title", "").strip(),
                    "summary":   entry.get("summary", "").strip(),
                    "url":       entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "source":    "Google News",
                })
                count += 1
            print(f"  OK  Google News [{commodity}]: {count} articles")
        except Exception as e:
            print(f"  FAILED  Google News [{commodity}]: {e}")

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


def print_results(news):
    print("\n")
    print("=" * 65)
    print("  COMMODITY NEWS FEED")
    print(f"  {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("=" * 65)

    for commodity, articles in news.items():
        print(f"\n--- {commodity.upper()} --- {len(articles)} articles")

        if not articles:
            print("  No articles found.")
            continue

        for i, a in enumerate(articles[:6], 1):
            print(f"\n  {i}. {a['title']}")
            print(f"     {a['source']}  |  {a['published'][:22] if a['published'] else ''}")

    print("\n" + "=" * 65)
    total = sum(len(v) for v in news.values())
    print(f"  Total: {total} articles found")
    print("=" * 65)


# Run
all_articles = fetch_all_articles()
news         = filter_by_commodity(all_articles)
print_results(news)