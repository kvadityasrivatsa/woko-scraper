#!/usr/bin/env python3
"""woko_scraper.py – One-file scraper for WOKO Zürich listings
-----------------------------------------------------------------
Scrapes https://woko.ch/en/zimmer-in-zuerich, appends results to a CSV,
marks vanished posts *INACTIVE*, and pushes Telegram alerts when a
**Tenant wanted** post is younger than a configurable *fresh-window*
(default 5 minutes).

Runtime deps: **requests, beautifulsoup4, pandas** (auto-installed).  Safe
for GitHub Actions every 5 min.

Environment variables (repo *Secrets & vars → Actions*):
  ▸ `TELEGRAM_BOT_TOKEN` – Telegram bot token
  ▸ `TELEGRAM_CHAT_ID`   – chat / channel ID
  ▸ `TIME_WINDOW_MINUTES` (optional) – override fresh window

CLI examples
------------
```bash
python woko_scraper.py                     # default (5 min)
TIME_WINDOW_MINUTES=30 python woko_scraper.py --commit-now
```
"""
from __future__ import annotations

# ── stdlib ──────────────────────────────────────────────────────────────────
import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import logging

# ── lightweight dependency auto-install ─────────────────────────────────────
for _pkg in ("requests", "beautifulsoup4", "pandas"):
    try:
        __import__(_pkg.split("-")[0])
    except ImportError:  # pragma: no cover
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", _pkg])

# ── third-party ──────────────────────────────────────────────────────────────
import requests  # type: ignore  # noqa: E402
from bs4 import BeautifulSoup  # type: ignore  # noqa: E402
import pandas as pd  # type: ignore  # noqa: E402

# ── constants & helpers ─────────────────────────────────────────────────────
URL_OVERVIEW = "https://woko.ch/en/zimmer-in-zuerich"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    )
}
ZURICH_TZ = ZoneInfo("Europe/Zurich")
UTC = ZoneInfo("UTC")

def _env_int(name: str, default: int) -> int:
    """Return integer env var or *default* when unset / blank / invalid."""
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default

DEFAULT_FRESH_WINDOW = _env_int("TIME_WINDOW_MINUTES", 5)

logging.basicConfig(
    format="[%(levelname)s] %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)

# ── scraping helpers ─────────────────────────────────────────────────────────

def _parse_anchor(a) -> dict | None:
    """Extract listing fields from an <a> tag."""
    href = a.get("href", "")
    m_id = re.search(r"/(\d+)$", href)
    if not m_id:
        return None

    text = " ".join(a.get_text(" ", strip=True).split())
    m = re.search(
        r"^(?P<title>.+?)\s+(?P<date>\d{2}\.\d{2}\.\d{4})\s+"
        r"(?P<time>\d{2}:\d{2})\s+(?P<type>(?:Tenant|Sublet))\s+wanted",
        text,
        flags=re.I,
    )
    if not m:
        return None

    local_dt = datetime.strptime(f"{m['date']} {m['time']}", "%d.%m.%Y %H:%M").replace(tzinfo=ZURICH_TZ)

    return {
        "id": int(m_id.group(1)),
        "title": m["title"],
        "posted_at": local_dt.astimezone(UTC).isoformat(),
        "listing_type": m["type"].capitalize(),
        "link": href if href.startswith("http") else f"https://woko.ch{href}",
        "status": "ACTIVE",
    }


def scrape() -> pd.DataFrame:
    logging.info("Fetching %s", URL_OVERVIEW)
    resp = requests.get(URL_OVERVIEW, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select('a[href*="/zimmer-in-zuerich-details/"]')
    df = pd.DataFrame([d for d in (_parse_anchor(a) for a in anchors) if d])
    logging.info("Scraped %d listings", len(df))
    return df

# ── persistence & diffing ───────────────────────────────────────────────────

def merge_with_history(new_df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    if csv_path.exists():
        old_df = pd.read_csv(csv_path, dtype={"id": int})
        vanished = old_df[~old_df["id"].isin(new_df["id"])].copy()
        if not vanished.empty:
            vanished["status"] = "INACTIVE"
            new_df = pd.concat([new_df, vanished], ignore_index=True)
    new_df.sort_values("posted_at", ascending=False, inplace=True)
    return new_df.reset_index(drop=True)

# ── notifications ───────────────────────────────────────────────────────────

def telegram_alerts(df: pd.DataFrame, fresh_minutes: int) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        logging.info("Telegram secrets not set – skipping alerts")
        return

    now = datetime.now(UTC)
    window_start = now - timedelta(minutes=fresh_minutes)
    fresh = df[
        (df["listing_type"] == "Tenant")
        & (pd.to_datetime(df["posted_at"], utc=True) >= window_start)
    ]
    for _, row in fresh.iterrows():
        msg = (
            "URGENT: NEW RENT POSTING ON WOKO\n\n"
            f"{row.title}\nPosted: {row.posted_at}\n{row.link}"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id": chat_id, "text": msg},
                timeout=20,
            ).raise_for_status()
            logging.info("Telegram alert sent for %s", row.id)
        except Exception as exc:
            logging.warning("Telegram alert FAILED for %s: %s", row.id, exc)

# ── utilities ───────────────────────────────────────────────────────────────

def _save_if_changed(df: pd.DataFrame, path: Path) -> bool:
    csv_text = df.to_csv(index=False)
    if path.exists() and path.read_text() == csv_text:
        logging.info("CSV identical – no overwrite")
        return False
    path.write_text(csv_text)
    logging.info("CSV saved → %s", path)
    return True

# ── main entry ──────────────────────────────────────────────────────────────

def cli(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Scrape WOKO listings & maintain CSV history")
    p.add_argument("--csv", default="woko_listings.csv", type=Path, help="Output CSV path")
    p.add_argument("--fresh-window", type=int, default=DEFAULT_FRESH_WINDOW, help="Fresh window (minutes) if env var absent")
    p.add_argument("--commit-now", action="store_true", help="Force git commit regardless of schedule (manual tests)")
    args = p.parse_args(argv)

    fresh_minutes = _env_int("TIME_WINDOW_MINUTES", args.fresh_window)

    live_df = scrape()
    combined_df = merge_with_history(live_df, args.csv)
    changed = _save_if_changed(combined_df, args.csv)

    telegram_alerts(live_df, fresh_minutes)

    # exit codes: 0 = none, 10 = csv changed, 11 = manual commit toggle
    if args.commit_now:
        sys.exit(11)
    sys.exit(10 if changed else 0)

if __name__ == "__main__":
    cli()
