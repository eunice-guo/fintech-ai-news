import os, sys, csv, time, hashlib
from datetime import datetime, timezone
import yaml
import requests
import feedparser
import pandas as pd
from dateutil import parser as dtparse

# ---- Helper: safe print
def log(msg):
    print(f"[aggregator] {msg}", flush=True)

# ---- Load feeds.yaml
CFG_PATH = os.getenv("FEEDS_PATH", "config/feeds.yaml")
if not os.path.exists(CFG_PATH):
    log(f"ERROR: {CFG_PATH} not found.")
    sys.exit(1)

with open(CFG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

NITTER_BASE = cfg.get("nitter_base", "https://nitter.net")
NITTER_MIRRORS = cfg.get("nitter_mirrors", [NITTER_BASE])

categories = cfg.get("categories", {})

# ---- Build list of feed URLs (including Twitter via Nitter RSS)
feed_sources = []

# Substack + generic RSS
for item in categories.get("substack", []) + categories.get("fintech_news", []):
    feed_sources.append({
        "source": item["name"],
        "category": "substack" if item in categories.get("substack", []) else "fintech_news",
        "url": item["url"]
    })

# Twitter via Nitter RSS per account handle
for acct in categories.get("twitter_accounts", []):
    handle = acct["handle"].lstrip("@")
    # We'll try mirrors later if primary fails
    feed_sources.append({
        "source": f"Twitter:{handle}",
        "category": "twitter",
        "url": f"{NITTER_BASE}/{handle}/rss",
        "handle": handle
    })

log(f"Loaded {len(feed_sources)} feeds.")

# ---- Fetch and normalize
rows = []
seen = set()

def hash_key(*parts):
    m = hashlib.md5()
    for p in parts:
        m.update((p or "").encode("utf-8"))
    return m.hexdigest()

def parse_entry(entry):
    title = entry.get("title", "").strip()
    link = entry.get("link", "").strip()
    summary = (entry.get("summary") or entry.get("description") or "").strip()

    # Attempt to parse published date
    published = None
    for key in ["published", "updated", "created"]:
        val = entry.get(key) or entry.get(f"{key}_parsed")
        if val:
            try:
                # feedparser returns struct_time; str() ok for dtparse
                published = dtparse.parse(str(val))
                break
            except Exception:
                pass
    if published is None:
        published = datetime.now(timezone.utc)

    # Content/summary cleanup (basic)
    if summary:
        # Strip basic HTML tags—feedparser often already does
        summary = summary.replace("\n", " ").replace("\r", " ").strip()
        if len(summary) > 2000:
            summary = summary[:2000] + "..."

    return title, link, summary, published

def try_fetch(url, timeout=15):
    headers = {
        "User-Agent": "Mozilla/5.0 (RSS Collector; +https://github.com/yourname/fintech-ai-news)"
    }
    return requests.get(url, timeout=timeout, headers=headers)

def fetch_feed(url, handle=None):
    # For Nitter, try mirrors
    if handle and "nitter" in url:
        for base in NITTER_MIRRORS:
            test_url = f"{base}/{handle}/rss"
            try:
                r = try_fetch(test_url)
                if r.status_code == 200 and r.text.strip():
                    return r.text
            except Exception as e:
                log(f"Nitter mirror failed: {base} ({e})")
        return None
    else:
        try:
            r = try_fetch(url)
            if r.status_code == 200 and r.text.strip():
                return r.text
        except Exception as e:
            log(f"Fetch failed: {url} ({e})")
        return None

for src in feed_sources:
    xml = fetch_feed(src["url"], handle=src.get("handle"))
    if not xml:
        log(f"SKIP (unreachable): {src['source']} -> {src['url']}")
        continue

    feed = feedparser.parse(xml)
    if feed.bozo:
        log(f"Warning: parsing issue for {src['source']}")

    for entry in feed.entries[:50]:  # keep it light per-source
        title, link, summary, published = parse_entry(entry)
        if not (title or link):
            continue

        key = hash_key(src["source"], title, link)
        if key in seen:
            continue
        seen.add(key)

        rows.append({
            "source": src["source"],
            "category": src["category"],
            "title": title,
            "link": link,
            "summary": summary,
            "published": published.isoformat(),
            "collected_at": datetime.now(timezone.utc).isoformat()
        })

log(f"Collected {len(rows)} items.")

# ---- Output CSV
os.makedirs("data", exist_ok=True)
date_tag = datetime.now().strftime("%Y-%m-%d")
out_path = f"data/news_raw_{date_tag}.csv"

df = pd.DataFrame(rows, columns=["source","category","title","link","summary","published","collected_at"])
df.sort_values(by=["published"], ascending=False, inplace=True)
df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

log(f"Wrote {out_path} with {len(df)} rows.")
