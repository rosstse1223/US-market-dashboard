#!/usr/bin/env python3
"""
update_rs_ratings.py
--------------------
1. Fetches RS Ratings (1-99) for all tickers in data/indices.json
2. Tracks a 21-day rolling history in data/rs_history.json
3. Detects 1-month RS new highs (rs_1m_new_high: true/false)
4. Injects rs_rating + rs_1m_new_high back into data/indices.json

Primary:  ibd-rs-rating  →  pip install ibd-rs-rating
Fallback: yfinance        →  pip install yfinance scipy pandas
"""

import json
import time
from pathlib import Path
from datetime import datetime

DATA_FILE    = Path("data/indices.json")
HISTORY_FILE = Path("data/rs_history.json")
SECTIONS     = ["indices", "sectors", "sectors_ew", "commodities", "thematic"]
LOOKBACK     = 21   # ~1 month of trading days

# ── Load JSON ──────────────────────────────────────────────────────────────
with open(DATA_FILE) as f:
    data = json.load(f)

# ── Load RS history (rolling 21-day store) ────────────────────────────────
if HISTORY_FILE.exists():
    with open(HISTORY_FILE) as f:
        history = json.load(f)
else:
    history = {}

# ── Collect all unique tickers ─────────────────────────────────────────────
tickers = []
for section in SECTIONS:
    for row in data.get(section, []):
        t = row.get("ticker")
        if t and t not in tickers:
            tickers.append(t)

print(f"Fetching RS Ratings for {len(tickers)} tickers…\n")


# ══════════════════════════════════════════════════════════════════════════
# METHOD 1 — ibd-rs-rating package
# pip install ibd-rs-rating
# True 1-99 rating vs all ~4,600 US stocks, updated daily.
# ══════════════════════════════════════════════════════════════════════════
def fetch_via_package(tickers):
    try:
        from ibd_rs import RsRating
        rs = RsRating()
        ratings = {}
        for ticker in tickers:
            try:
                rating = rs.get_rating(ticker)
                ratings[ticker] = int(rating)
                print(f"  [pkg] {ticker:12s} → {rating}")
            except Exception as e:
                ratings[ticker] = None
                print(f"  [pkg] {ticker:12s} → N/A  ({e})")
            time.sleep(0.2)
        return ratings
    except ImportError:
        return None


# ══════════════════════════════════════════════════════════════════════════
# METHOD 2 — yfinance fallback
# pip install yfinance scipy pandas
#
# Replicates the Pine Script formula exactly:
#   rs_stock = 0.4×ROC(63) + 0.2×ROC(126) + 0.2×ROC(189) + 0.2×ROC(252)
#   rs_ref   = same for SP500
#   score    = (rs_stock / rs_ref) × 100
#   rating   = percentile rank within your watchlist
# ══════════════════════════════════════════════════════════════════════════
def fetch_via_yfinance(tickers):
    try:
        import yfinance as yf
        import pandas as pd
        from scipy.stats import percentileofscore
    except ImportError as e:
        print(f"  ❌ Missing dependency: {e}")
        print("     Run: pip install yfinance scipy pandas")
        return {t: None for t in tickers}

    BENCHMARK = "^GSPC"
    all_syms  = list(set(tickers + [BENCHMARK]))

    print("  Downloading 1-year daily price history via yfinance…")
    raw = yf.download(
        all_syms, period="1y", interval="1d",
        auto_adjust=True, progress=False
    )["Close"]

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    sp = raw.get(BENCHMARK)
    if sp is None:
        print("  ❌ Could not download SP500 benchmark.")
        return {t: None for t in tickers}

    def roc(series, n):
        if series is None or len(series.dropna()) < n + 1:
            return None
        return float(series.dropna().iloc[-1] / series.dropna().iloc[-n])

    def rs_score(ticker):
        try:
            t = raw.get(ticker)
            p = [roc(t,  63), roc(t, 126), roc(t, 189), roc(t, 252)]
            c = [roc(sp, 63), roc(sp,126), roc(sp,189), roc(sp,252)]
            if any(x is None for x in p + c):
                return None
            rs_stock = 0.4*p[0] + 0.2*p[1] + 0.2*p[2] + 0.2*p[3]
            rs_ref   = 0.4*c[0] + 0.2*c[1] + 0.2*c[2] + 0.2*c[3]
            return (rs_stock / rs_ref) * 100
        except Exception:
            return None

    scores = {t: rs_score(t) for t in tickers}
    valid  = [s for s in scores.values() if s is not None]

    ratings = {}
    for ticker, score in scores.items():
        if score is None:
            ratings[ticker] = None
        else:
            pct = percentileofscore(valid, score, kind="rank")
            ratings[ticker] = max(1, min(99, round(pct)))
        print(f"  [yfin] {ticker:12s} → {ratings[ticker]}")

    return ratings


# ── Run: try package first, fall back to yfinance ─────────────────────────
ratings = fetch_via_package(tickers)
if ratings is None:
    print("  ibd-rs-rating not installed — falling back to yfinance.\n"
          "  Note: ratings are relative to your watchlist only.\n"
          "  For true 1-99 vs all US stocks: pip install ibd-rs-rating\n")
    ratings = fetch_via_yfinance(tickers)


# ── Compute 1-month new highs using rolling history ───────────────────────
print("\nComputing RS 1-month new highs…")
new_highs = {}

for ticker, current_rating in ratings.items():
    if current_rating is None:
        new_highs[ticker] = None
        continue

    hist = history.get(ticker, [])

    # Need at least 1 prior data point to compare against
    if len(hist) >= 1:
        prior_window = [h for h in hist[-LOOKBACK:] if h is not None]
        if prior_window:
            max_prior = max(prior_window)
            is_new_high = current_rating >= max_prior
        else:
            is_new_high = False
    else:
        # First time seeing this ticker — not enough history yet
        is_new_high = False

    new_highs[ticker] = is_new_high
    flag = "✦ NEW HIGH" if is_new_high else "—"
    print(f"  {ticker:12s}  current={current_rating:3d}  "
          f"max(21d)={max(hist[-LOOKBACK:]) if hist else 'n/a':>3}  {flag}")

    # Append current rating and keep rolling window
    hist.append(current_rating)
    history[ticker] = hist[-LOOKBACK:]


# ── Save updated history ──────────────────────────────────────────────────
HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
with open(HISTORY_FILE, "w") as f:
    json.dump(history, f, indent=2)
print(f"\n  History saved → {HISTORY_FILE}")


# ── Inject rs_rating + rs_1m_new_high into JSON ───────────────────────────
for section in SECTIONS:
    for row in data.get(section, []):
        t = row.get("ticker")
        if t in ratings:
            row["rs_rating"]      = ratings.get(t)
            row["rs_1m_new_high"] = new_highs.get(t)

data["updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

with open(DATA_FILE, "w") as f:
    json.dump(data, f, indent=2)

print(f"\n✅  Done. rs_rating + rs_1m_new_high injected for {len(ratings)} tickers → {DATA_FILE}")
