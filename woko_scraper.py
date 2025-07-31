#!/usr/bin/env python3
"""woko_scraper.py – WOKO Zürich listings watcher

▸ Scrapes https://woko.ch/en/zimmer-in-zuerich.
▸ Maintains **woko_listings.csv** (adds new, marks vanished as INACTIVE).
▸ Sends Telegram alerts for *Tenant wanted* postings newer than a configurable
  *fresh‑window* (default 5 min, overridable by env‑var or CLI flag).

Dependencies (`pip install -q pandas requests beautifulsoup4`) are auto‑installed
if missing so the script runs identically on local machines, GitHub Actions,
Kaggle, etc.

Environment variables (set as GitHub *Secrets & vars → Actions* or locally):
--------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN   Telegram bot token (from @BotFather)
TELEGRAM_CHAT_ID     Chat / channel / user ID to receive alerts
TIME_WINDOW_MINUTES  Fresh‑window override (integer, optional)
LOG_LEVEL            Python logging level (INFO, DEBUG…)

CLI usage
---------
python woko_scraper.py                    # default behaviour, 5‑min window
python woko_scraper.py --fresh-window 30  # override via flag

The script **never exits with non‑zero status**, so CI jobs don't fail just
because the CSV changed.  Git commit/push is handled in the GitHub Workflow.
"""
from __future__ import annotations

# ── standard lib ─────────────────────────────────────────────────────────────
import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# ── on‑the‑fly dependency check / install ───────────────────────────────────
for _pkg in ("requests", "beautifulsoup4", "pandas"):
    try:
        __import__(_pkg.split("-")[0])
    except ImportError:  # pragma: no cover
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", _pkg])

# ── third‑party ──────────────────────────────────────────────────────────────
import requests  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
import pandas as pd  # type: ignore

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

# ── helpers ──────────────────────────────────────────────────────────────────

def _env_int(name: str, default: int) -> int:
    """Return integer env‑var *name* or *default* when unset / blank / invalid."""
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default

logging.basicConfig(
    format="[%(levelname)s] %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)

# ── scraping ─────────────────────────────────────────────────────────────────

def _parse_anchor(a) -> dict | None:
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

    local_dt = datetime.strptime(
        f"{m['date']} {m['time']}", "%d.%m.%Y %H:%M"
    ).replace(tzinfo=ZURICH_TZ)

    return {
        "id": int(m_id.group(1)),
        "title": m["title"],
        "posted_at": local_dt.astimezone(UTC).isoformat(),
        "listing_type": m["type"].capitalize(),
        "link": href if href.startswith("http") else f"https://woko.ch{href}",
        "status": "ACTIVE",
    }


def scrape_overview() -> pd.DataFrame:
    logging.info("Fetching %s", URL_OVERVIEW)
    resp = requests.get(URL_OVERVIEW, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select('a[href*="/zimmer-in-zuerich-details/"]')
    df = pd.DataFrame([d for d in (_parse_anchor(a) for a in anchors) if d])
    logging.info("Scraped %d listings", len(df))
    return df

# ── persistence ─────────────────────────────────────────────────────────────

def merge_history(new_df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    if csv_path.exists():
        old_df = pd.read_csv(csv_path, dtype={"id": int})
        vanished = old_df[~old_df["id"].isin(new_df["id"])].copy()
        if not vanished.empty:
            vanished["status"] = "INACTIVE"
            new_df = pd.concat([new_df, vanished], ignore_index=True)
    return new_df.sort_values("posted_at", ascending=False).reset_index(drop=True)


def save_if_changed(df: pd.DataFrame, path: Path) -> bool:
    csv_text = df.to_csv(index=False)
    if path.exists() and path.read_text() == csv_text:
        logging.info("CSV unchanged – nothing to write")
        return False
    path.write_text(csv_text)
    logging.info("CSV written → %s", path)
    return True

# ── alerts ──────────────────────────────────────────────────────────────────

def telegram_alerts(df: pd.DataFrame, fresh_minutes: int) -> None:
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        logging.debug("Telegram secrets not set – skipping alerts")
        return

    now = datetime.now(UTC)
    since = now - timedelta(minutes=fresh_minutes)
    fresh = df[ pd.to_datetime(df["posted_at"], utc=True) >= since ]
    # fresh = df[(df["listing_type"] == "Tenant") & (pd.to_datetime(df["posted_at"], utc=True) >= since)]
    for _, row in fresh.iterrows():
        msg = (
            "URGENT: NEW RENT POSTING ON WOKO\n\n"
            f"TITLE: {row.title}\nTYPE: {row.listing_type} \nTIMESTAMP: {row.posted_at}\nLINK: {row.link}"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id": chat_id, "text": msg},
                timeout=20,
            ).raise_for_status()
            logging.info("Telegram alert sent for ID %s", row.id)
        except Exception as exc:
            logging.warning("Telegram alert FAILED for %s: %s", row.id, exc)

# ── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Scrape WOKO listings & update CSV history")
    p.add_argument("--csv", type=Path, default="data/woko_listings.csv", help="Output CSV path (default data/woko_listings.csv)")
    p.add_argument("--fresh-window", type=int, default=_env_int("TIME_WINDOW_MINUTES", 5), help="Fresh window minutes (default 5 or TIME_WINDOW_MINUTES env)")
    args = p.parse_args(argv)

    live_df = scrape_overview()
    combo_df = merge_history(live_df, args.csv)
    changed = save_if_changed(combo_df, args.csv)

    telegram_alerts(live_df, args.fresh_window)

    logging.info("Done – changed=%s", changed)

if __name__ == "__main__":
    main()
