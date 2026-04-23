import yfinance as yf
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

OUTPUT_FILE = Path("data/indices.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ── Ticker definitions ────────────────────────────────────────────────────
INDICES = [
    {"ticker": "SPY",  "name": "S&P 500 ETF"},
    {"ticker": "QQQ",  "name": "Nasdaq 100 ETF"},
    {"ticker": "IWM",  "name": "Russell 2000 ETF"},
    {"ticker": "DIA",  "name": "Dow Jones ETF"},
    {"ticker": "VTI",  "name": "Total Market ETF"},
]

SECTORS = [
    {"ticker": "XLK",  "name": "Technology"},
    {"ticker": "XLF",  "name": "Financials"},
    {"ticker": "XLV",  "name": "Health Care"},
    {"ticker": "XLE",  "name": "Energy"},
    {"ticker": "XLI",  "name": "Industrials"},
    {"ticker": "XLY",  "name": "Consumer Discretionary"},
    {"ticker": "XLP",  "name": "Consumer Staples"},
    {"ticker": "XLU",  "name": "Utilities"},
    {"ticker": "XLB",  "name": "Materials"},
    {"ticker": "XLRE", "name": "Real Estate"},
    {"ticker": "XLC",  "name": "Communication Services"},
]

SECTORS_EW = [
    {"ticker": "RSPG", "name": "EW Energy"},
    {"ticker": "RSPT", "name": "EW Technology"},
    {"ticker": "RSPF", "name": "EW Financials"},
    {"ticker": "RSPH", "name": "EW Health Care"},
    {"ticker": "RSPI", "name": "EW Industrials"},
    {"ticker": "RSPS", "name": "EW Consumer Staples"},
    {"ticker": "RSPU", "name": "EW Utilities"},
    {"ticker": "RSPN", "name": "EW Materials"},
    {"ticker": "RSPD", "name": "EW Consumer Disc"},
    {"ticker": "RSPR", "name": "EW Real Estate"},
    {"ticker": "RSPC", "name": "EW Comm Services"},
]

COMMODITIES = [
    {"ticker": "GLD",  "name": "Gold ETF"},
    {"ticker": "SLV",  "name": "Silver ETF"},
    {"ticker": "USO",  "name": "Oil ETF"},
    {"ticker": "UNG",  "name": "Natural Gas ETF"},
    {"ticker": "PDBC", "name": "Commodities ETF"},
    {"ticker": "BTC-USD", "name": "Bitcoin"},
    {"ticker": "ETH-USD", "name": "Ethereum"},
]

THEMATIC = [
    {"ticker": "ARKK", "name": "ARK Innovation"},
    {"ticker": "SOXX", "name": "Semiconductors"},
    {"ticker": "IBB",  "name": "Biotech"},
    {"ticker": "XBI",  "name": "Biotech (EW)"},
    {"ticker": "ICLN", "name": "Clean Energy"},
    {"ticker": "FINX", "name": "FinTech"},
    {"ticker": "HACK", "name": "Cybersecurity"},
    {"ticker": "ROBO", "name": "Robotics & AI"},
]


# ── MA status helper ──────────────────────────────────────────────────────
def ma_status(price: float, ma_val: float, ma_prev: float) -> str:
    """Return above_up / above_down / below_up / below_down."""
    if price >= ma_val:
        return "above_up"   if ma_val >= ma_prev else "above_down"
    else:
        return "below_up"   if ma_val >= ma_prev else "below_down"


# ── EMA helper ────────────────────────────────────────────────────────────
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


# ── VARS histogram helper ─────────────────────────────────────────────────
def vars_histogram(close: pd.Series, window: int = 20, lookback: int = 50) -> list:
    """
    Returns last `window` bars of VARS histogram values + SMA20 of those values.
    VARS = close - SMA(close, lookback)  (simplified linear approximation)
    """
    if len(close) < lookback + window:
        return []
    sma_lb = close.rolling(lookback).mean()
    hist   = close - sma_lb
    sma20  = hist.rolling(20).mean()
    result = []
    for i in range(-window, 0):
        v = hist.iloc[i]
        m = sma20.iloc[i]
        if pd.isna(v) or pd.isna(m):
            continue
        result.append({"v": round(float(v), 4), "m": round(float(m), 4)})
    return result


# ── Core per-ticker calculation ───────────────────────────────────────────
def compute_row(ticker_def: dict, hist: pd.DataFrame) -> dict:
    ticker = ticker_def["ticker"]
    name   = ticker_def["name"]

    close  = hist["Close"].dropna()
    open_  = hist["Open"].dropna()

    if len(close) < 210:
        print(f"  ⚠  {ticker}: not enough data ({len(close)} bars)")
        return None

    price        = round(float(close.iloc[-1]), 4)
    prev_close   = float(close.iloc[-2])
    daily_chg    = round((price / prev_close - 1) * 100, 4)

    # Intraday change: open → close of last bar
    last_open    = float(open_.iloc[-1])
    intraday_chg = round((price / last_open - 1) * 100, 4) if last_open else None

    # 5-day change (5 trading days back)
    chg_5d = round((price / float(close.iloc[-6]) - 1) * 100, 4) if len(close) >= 6 else None

    # ── Moving averages ───────────────────────────────────────────────────
    # EMA 9
    ema9_s     = ema(close, 9)
    ema9_val   = float(ema9_s.iloc[-1])
    ema9_prev  = float(ema9_s.iloc[-2])
    ema9_st    = ma_status(price, ema9_val, ema9_prev)

    # EMA 21
    ema21_s    = ema(close, 21)
    ema21_val  = float(ema21_s.iloc[-1])
    ema21_prev = float(ema21_s.iloc[-2])
    ema21_st   = ma_status(price, ema21_val, ema21_prev)

    # EMA 50
    ema50_s    = ema(close, 50)
    ema50_val  = float(ema50_s.iloc[-1])
    ema50_prev = float(ema50_s.iloc[-2])
    ema50_st   = ma_status(price, ema50_val, ema50_prev)

    # SMA 150
    sma150_s   = close.rolling(150).mean()
    sma150_val = float(sma150_s.iloc[-1])
    sma150_prev= float(sma150_s.iloc[-2])
    sma150_st  = ma_status(price, sma150_val, sma150_prev)

    # SMA 200
    sma200_s   = close.rolling(200).mean()
    sma200_val = float(sma200_s.iloc[-1])
    sma200_prev= float(sma200_s.iloc[-2])
    sma200_st  = ma_status(price, sma200_val, sma200_prev)

    # ── ATR14 / EMA50 multiple ────────────────────────────────────────────
    high  = hist["High"].dropna()
    low_  = hist["Low"].dropna()
    tr    = pd.concat([
        high - low_,
        (high - close.shift(1)).abs(),
        (low_ - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    atr14     = float(tr.rolling(14).mean().iloc[-1])
    atr_mult  = round((price - ema50_val) / atr14, 4) if atr14 else None

    # ── VARS histogram (20 bars × window=20, lookback=50) ─────────────────
    vars_hist = vars_histogram(close, window=20, lookback=50)

    return {
        "ticker":        ticker,
        "name":          name,
        "price":         price,
        "daily_chg":     daily_chg,
        "intraday_chg":  intraday_chg,
        "chg_5d":        chg_5d,
        "ema9":          ema9_st,       # ← was ema10
        "ema21":         ema21_st,      # ← was ema20
        "ema50":         ema50_st,      # ← was sma50
        "sma150":        sma150_st,     # ← new
        "sma200":        sma200_st,
        "atr_multiple":  atr_mult,
        "vars_history":  vars_hist,
    }


# ── Download + process a list of tickers ─────────────────────────────────
def process_section(ticker_defs: list) -> list:
    symbols = [t["ticker"] for t in ticker_defs]
    print(f"  Downloading: {symbols}")

    raw = yf.download(
        symbols,
        period="2y",          # 2 years to cover SMA200 + buffer
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )

    rows = []
    for td in ticker_defs:
        sym = td["ticker"]
        try:
            # Handle single vs multi-ticker yfinance response
            if len(symbols) == 1:
                hist = raw
            else:
                hist = raw[sym] if sym in raw.columns.get_level_values(0) else None

            if hist is None or hist.empty:
                print(f"  ⚠  {sym}: no data returned")
                continue

            row = compute_row(td, hist)
            if row:
                rows.append(row)
                print(f"  ✓  {sym}")
        except Exception as e:
            print(f"  ✗  {sym}: {e}")

    return rows


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Market Dashboard — fetch_data.py")
    print(f"  Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    sections = {
        "indices":     INDICES,
        "sectors":     SECTORS,
        "sectors_ew":  SECTORS_EW,
        "commodities": COMMODITIES,
        "thematic":    THEMATIC,
    }

    output = {"updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}

    for section_name, ticker_defs in sections.items():
        print(f"\n── {section_name.upper()} ──")
        output[section_name] = process_section(ticker_defs)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅  Done → {OUTPUT_FILE}")
    print(f"    Sections: {', '.join(f'{k}: {len(v)} rows' for k, v in output.items() if isinstance(v, list))}")


if __name__ == "__main__":
    main()
