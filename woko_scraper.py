#!/usr/bin/env python3
"""woko_scraper.py – One‑file scraper for WOKO Zürich listings
-----------------------------------------------------------------
Scrapes https://woko.ch/en/zimmer-in-zuerich, appends results to a CSV,
marks vanished posts *INACTIVE*, and pushes Telegram alerts when a
**Tenant wanted** post is younger than an adjustable *fresh‑window*
(default 5 minutes).

Designed for GitHub Actions running every 5 minutes while **committing
changes only occasionally** (e.g. every 14 days) to keep public repo
noise low.

Runtime dependencies: **requests, beautifulsoup4, pandas** (auto‑installed).

Environment variables (add under *Repo ▸ Settings ▸ Secrets & vars ▸ Actions*):
  ▸ ``TELEGRAM_BOT_TOKEN`` – Telegram bot API token
  ▸ ``TELEGRAM_CHAT_ID``  – target chat / channel ID for alerts
  ▸ ``TIME_WINDOW_MINUTES`` (optional) – fresh window override (int)

CLI examples
------------
```
# default 5‑minute window
python woko_scraper.py --csv data/woko_listings.csv

# override with 30‑minute window, force git commit afterwards
TIME_WINDOW_MINUTES=30 python woko_scraper.py --commit-now
```
"""
from __future__ import annotations

# ── standard libs ────────────────────────────────────────────────────────────
import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import logging

# ── lightweight on‑the‑fly dependency check / install ───────────────────────
for _pkg in ("requests", "beautifulsoup4", "pandas"):
    try:
        __import__(_pkg.split("-")[0])
    except ImportError:  # pragma: no cover
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", _pkg])

# ── third‑party ──────────────────────────────────────────────────────────────
import requests  # type: ignore  # noqa: E402
from bs4 import BeautifulSoup  # type: ignore  # noqa: E402
import pandas as pd  # type: ignore  # noqa: E402

# ── constants ────────────────────────────────────────────────────────────────
URL_OVERVIEW = "https://woko.ch/en/zimmer-in-zuerich"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    )
}
ZURICH_TZ = ZoneInfo("Europe/Zurich")
UTC = ZoneInfo("UTC")
DEFAULT_FRESH_WINDOW = int(os.getenv("TIME_WINDOW_MINUTES", "5"))  # overridable

logging.basicConfig(
    format="[%(levelname)s] %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)

# ── scraping helpers ─────────────────────────────────────────────────────────

def _parse_anchor(a) -> dict | None:
    """Extract fields from an <a> within the listings overview."""
    href: str = a.get("href", "")
    m_id = re.search(r"/(\d+)$", href)
    if not m_id:
        return None

    listing_id = int(m_id.group(1))
    text = " ".join(a.get_text(" ", strip=True).split())
    rx = re.compile(
        r"^(?P<title>.+?)\s+(?P<date>\d{2}\.\d{2}\.\d{4})\s+"  # DD.MM.YYYY
        r"(?P<time>\d{2}:\d{2})\s+(?P<type>(?:Tenant|Sublet))\s+wanted",
        re.I,
    )
    m = rx.search(text)
    if not m:
        return None

    local_dt = (
        datetime.strptime(f"{m['date']} {m['time']}", "%d.%m.%Y %H:%M")
        .replace(tzinfo=ZURICH_TZ)
        .astimezone(UTC)
    )

    return {
        "id": listing_id,
        "title": m["title"],
        "posted_at": local_dt.isoformat(),
        "listing_type": m["type"].capitalize(),
        "link": href if href.startswith("http") else f"https://woko.ch{href}",
        "status": "ACTIVE",
    }


def scrape() -> pd.DataFrame:
    logging.info("Fetching %s", URL_OVERVIEW)
    r = requests.get(URL_OVERVIEW, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
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
    fresh_window = now - timedelta(minutes=fresh_minutes)
    fresh = df[
        (df["listing_type"] == "Tenant")
        & (pd.to_datetime(df["posted_at"], utc=True) >= fresh_window)
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

# ── main entry ──────────────────────────────────────────────────────────────

def cli(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Scrape WOKO listings + keep CSV history")
    p.add_argument("--csv", default="woko_listings.csv", type=Path, help="Output CSV path")
    p.add_argument("--fresh-window", type=int, default=DEFAULT_FRESH_WINDOW, help="Fresh window in minutes (env TIME_WINDOW_MINUTES takes precedence)")
    p.add_argument("--commit-now", action="store_true", help="Force git commit even if commit window not elapsed (for manual tests)")
    args = p.parse_args(argv)

    fresh_minutes = int(os.getenv("TIME_WINDOW_MINUTES", str(args.fresh_window)))

    live_df = scrape()
    combined_df = merge_with_history(live_df, args.csv)
    combined_df.to_csv(args.csv, index=False)
    logging.info("Saved %d rows → %s", len(combined_df), args.csv)

    telegram_alerts(live_df, fresh_minutes=fresh_minutes)

    # Return exit code signalling whether to commit (for GitHub step logic)
    if args.commit_now:
        sys.exit(10)  # special code interpreted by workflow as "force commit"

if __name__ == "__main__":
    cli()
