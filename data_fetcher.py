"""
data_fetcher.py
Core data logic for QQQ ETF analyzer:
  - Fetch QQQ holdings with weights (3-layer fallback)
  - Batch-fetch real-time intraday prices via yfinance
  - Calculate each holding's contribution to QQQ's daily move
  - In-memory TTL cache to prevent Yahoo Finance rate-limiting
"""

import time
import datetime
import logging
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import yfinance as yf
import pytz

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
_cache = {
    "data": None,
    "timestamp": 0.0,
    "holdings": None,
    "holdings_ts": 0.0,
    "market_caps": None,
    "market_caps_ts": 0.0,
}
PRICE_CACHE_TTL = 60        # seconds — how long before price data is stale
HOLDINGS_CACHE_TTL = 86400  # 24 hours — holdings rarely change

INVESCO_CSV_URL = (
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/"
    "holdings/0?audienceType=Investor&action=download&ticker=QQQ"
)

# ---------------------------------------------------------------------------
# Static fallback holdings (top 30 QQQ components, approximate weights)
# Updated as of early 2025. Used only when all other sources fail.
# ---------------------------------------------------------------------------
_STATIC_HOLDINGS = [
    {"ticker": "MSFT",  "name": "Microsoft Corp",          "weight": 0.0840, "sector": "Technology"},
    {"ticker": "NVDA",  "name": "NVIDIA Corp",              "weight": 0.0820, "sector": "Technology"},
    {"ticker": "AAPL",  "name": "Apple Inc",                "weight": 0.0790, "sector": "Technology"},
    {"ticker": "AMZN",  "name": "Amazon.com Inc",           "weight": 0.0530, "sector": "Consumer Discretionary"},
    {"ticker": "GOOGL", "name": "Alphabet Inc Class A",     "weight": 0.0390, "sector": "Communication Services"},
    {"ticker": "META",  "name": "Meta Platforms Inc",       "weight": 0.0380, "sector": "Communication Services"},
    {"ticker": "GOOG",  "name": "Alphabet Inc Class C",     "weight": 0.0330, "sector": "Communication Services"},
    {"ticker": "TSLA",  "name": "Tesla Inc",                "weight": 0.0290, "sector": "Consumer Discretionary"},
    {"ticker": "COST",  "name": "Costco Wholesale Corp",    "weight": 0.0250, "sector": "Consumer Staples"},
    {"ticker": "AVGO",  "name": "Broadcom Inc",             "weight": 0.0240, "sector": "Technology"},
    {"ticker": "NFLX",  "name": "Netflix Inc",              "weight": 0.0170, "sector": "Communication Services"},
    {"ticker": "AMD",   "name": "Advanced Micro Devices",   "weight": 0.0160, "sector": "Technology"},
    {"ticker": "ADBE",  "name": "Adobe Inc",                "weight": 0.0130, "sector": "Technology"},
    {"ticker": "QCOM",  "name": "QUALCOMM Inc",             "weight": 0.0120, "sector": "Technology"},
    {"ticker": "PEP",   "name": "PepsiCo Inc",              "weight": 0.0120, "sector": "Consumer Staples"},
    {"ticker": "AMAT",  "name": "Applied Materials Inc",    "weight": 0.0110, "sector": "Technology"},
    {"ticker": "CSCO",  "name": "Cisco Systems Inc",        "weight": 0.0100, "sector": "Technology"},
    {"ticker": "TXN",   "name": "Texas Instruments Inc",    "weight": 0.0090, "sector": "Technology"},
    {"ticker": "INTC",  "name": "Intel Corp",               "weight": 0.0080, "sector": "Technology"},
    {"ticker": "INTU",  "name": "Intuit Inc",               "weight": 0.0080, "sector": "Technology"},
    {"ticker": "AMGN",  "name": "Amgen Inc",                "weight": 0.0070, "sector": "Health Care"},
    {"ticker": "ISRG",  "name": "Intuitive Surgical Inc",   "weight": 0.0070, "sector": "Health Care"},
    {"ticker": "HON",   "name": "Honeywell International",  "weight": 0.0060, "sector": "Industrials"},
    {"ticker": "BKNG",  "name": "Booking Holdings Inc",     "weight": 0.0060, "sector": "Consumer Discretionary"},
    {"ticker": "SBUX",  "name": "Starbucks Corp",           "weight": 0.0060, "sector": "Consumer Discretionary"},
    {"ticker": "GILD",  "name": "Gilead Sciences Inc",      "weight": 0.0050, "sector": "Health Care"},
    {"ticker": "MDLZ",  "name": "Mondelez International",   "weight": 0.0050, "sector": "Consumer Staples"},
    {"ticker": "ADP",   "name": "Automatic Data Processing","weight": 0.0050, "sector": "Technology"},
    {"ticker": "PANW",  "name": "Palo Alto Networks Inc",   "weight": 0.0050, "sector": "Technology"},
    {"ticker": "REGN",  "name": "Regeneron Pharmaceuticals","weight": 0.0040, "sector": "Health Care"},
]


# ---------------------------------------------------------------------------
# Holdings Fetching
# ---------------------------------------------------------------------------

def get_qqq_holdings() -> pd.DataFrame:
    """
    Returns DataFrame: [ticker, name, weight (0.0-1.0), sector]
    Sorted by weight descending. Uses 4-layer fallback.
    """
    now = time.time()
    if _cache["holdings"] is not None and (now - _cache["holdings_ts"]) < HOLDINGS_CACHE_TTL:
        return _cache["holdings"]

    df = _fetch_holdings_invesco()
    if df is None or df.empty:
        logger.warning("Invesco CSV failed, trying Slickcharts")
        df = _fetch_holdings_slickcharts()
    if df is None or df.empty:
        logger.warning("Slickcharts failed, trying yfinance funds_data")
        df = _fetch_holdings_yfinance()
    if df is None or df.empty:
        logger.warning("yfinance funds_data failed, using static fallback")
        df = _get_static_holdings()

    _cache["holdings"] = df
    _cache["holdings_ts"] = now
    logger.info(f"Holdings loaded: {len(df)} stocks")
    return df


def _fetch_holdings_invesco() -> pd.DataFrame | None:
    """Download and parse Invesco's official QQQ holdings CSV."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.invesco.com/qqq-etf/en/about.html",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
    }
    try:
        resp = requests.get(INVESCO_CSV_URL, headers=headers, timeout=20)
        resp.raise_for_status()

        lines = resp.text.splitlines()

        # Invesco CSV has metadata rows before the actual data header.
        # Find the header row by looking for "Ticker" and "Weight".
        header_row_idx = 0
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if ("ticker" in line_lower or "holding" in line_lower) and "weight" in line_lower:
                header_row_idx = i
                break

        csv_text = "\n".join(lines[header_row_idx:])
        df_raw = pd.read_csv(StringIO(csv_text))
        df_raw.columns = [c.strip() for c in df_raw.columns]

        # Normalize column names using flexible matching
        col_map = {}
        for col in df_raw.columns:
            cl = col.lower()
            if "holding ticker" in cl or cl == "ticker":
                col_map[col] = "ticker"
            elif ("security name" in cl or cl == "name" or
                  "holding name" in cl or "description" in cl):
                col_map[col] = "name"
            elif "weight" in cl:
                col_map[col] = "weight_str"
            elif "sector" in cl:
                col_map[col] = "sector"

        df_raw = df_raw.rename(columns=col_map)

        if "ticker" not in df_raw.columns or "weight_str" not in df_raw.columns:
            logger.error(f"Invesco CSV: expected columns not found. Got: {list(df_raw.columns)}")
            return None

        # Clean tickers
        df_raw = df_raw[df_raw["ticker"].notna()].copy()
        df_raw["ticker"] = df_raw["ticker"].str.strip().str.upper()

        # Only real equity tickers (1-5 uppercase letters or with hyphen for BRK-B style)
        df_raw = df_raw[df_raw["ticker"].str.match(r'^[A-Z]{1,5}(-[A-Z])?$')]

        # Parse weight — Invesco stores as "9.8840%" string
        df_raw["weight"] = (
            df_raw["weight_str"]
            .str.replace("%", "", regex=False)
            .str.strip()
            .astype(float) / 100.0
        )

        if "name" not in df_raw.columns:
            df_raw["name"] = df_raw["ticker"]
        if "sector" not in df_raw.columns:
            df_raw["sector"] = ""

        result = df_raw[["ticker", "name", "weight", "sector"]].copy()
        result = result.sort_values("weight", ascending=False).reset_index(drop=True)
        logger.info(f"Invesco CSV: {len(result)} holdings loaded")
        return result

    except Exception as e:
        logger.error(f"Invesco CSV fetch failed: {e}")
        return None


def _fetch_holdings_slickcharts() -> pd.DataFrame | None:
    """Scrape QQQ top-100 holdings from Slickcharts as an intermediate fallback."""
    url = "https://www.slickcharts.com/nasdaq100"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()

        # pandas can parse HTML tables
        tables = pd.read_html(StringIO(resp.text))
        if not tables:
            return None

        # Slickcharts typically has one main table with columns like:
        # #, Company, Symbol, Weight, Price, Chg, % Chg
        df = None
        for t in tables:
            cols_lower = [str(c).lower() for c in t.columns]
            if any("symbol" in c for c in cols_lower) and any("weight" in c for c in cols_lower):
                df = t
                break

        if df is None:
            df = tables[0]  # Take first table as best guess

        df.columns = [str(c).strip() for c in df.columns]
        col_map = {}
        for col in df.columns:
            cl = col.lower()
            if cl in ("symbol", "ticker"):
                col_map[col] = "ticker"
            elif cl == "company":
                col_map[col] = "name"
            elif "weight" in cl:
                col_map[col] = "weight_str"

        df = df.rename(columns=col_map)
        if "ticker" not in df.columns or "weight_str" not in df.columns:
            logger.error(f"Slickcharts: unexpected columns {list(df.columns)}")
            return None

        df = df[df["ticker"].notna()].copy()
        df["ticker"] = df["ticker"].str.strip().str.upper()
        df = df[df["ticker"].str.match(r'^[A-Z]{1,5}(-[A-Z])?$')]

        # Weight stored as string like "8.89" (percent)
        df["weight"] = (
            pd.to_numeric(
                df["weight_str"].astype(str).str.replace("%", "", regex=False).str.strip(),
                errors="coerce"
            ).fillna(0) / 100.0
        )

        if "name" not in df.columns:
            df["name"] = df["ticker"]
        df["sector"] = ""

        result = df[["ticker", "name", "weight", "sector"]].copy()
        result = result[result["weight"] > 0]
        result = result.sort_values("weight", ascending=False).reset_index(drop=True)
        logger.info(f"Slickcharts: {len(result)} holdings loaded")
        return result

    except Exception as e:
        logger.error(f"Slickcharts fetch failed: {e}")
        return None


def _fetch_holdings_yfinance() -> pd.DataFrame | None:
    """Use yfinance funds_data.top_holdings as fallback (returns ~25 holdings)."""
    try:
        qqq = yf.Ticker("QQQ")
        top = qqq.funds_data.top_holdings
        if top is None or top.empty:
            return None
        top = top.reset_index()
        # Typical columns after reset: Symbol, Name/holdingPercent or similar
        # Rename defensively
        renamed = {}
        for col in top.columns:
            cl = col.lower()
            if cl in ("symbol", "ticker"):
                renamed[col] = "ticker"
            elif cl in ("name", "holding name", "description"):
                renamed[col] = "name"
            elif "percent" in cl or "weight" in cl or "holding" in cl:
                renamed[col] = "weight"
        top = top.rename(columns=renamed)

        if "ticker" not in top.columns:
            top.columns = ["ticker", "name", "weight"] + list(top.columns[3:])

        top["weight"] = pd.to_numeric(top["weight"], errors="coerce").fillna(0)
        if top["weight"].max() > 1.5:
            top["weight"] = top["weight"] / 100.0

        top["sector"] = "Unknown"
        result = top[["ticker", "name", "weight", "sector"]].copy()
        result = result.sort_values("weight", ascending=False).reset_index(drop=True)
        logger.info(f"yfinance funds_data: {len(result)} holdings loaded")
        return result

    except Exception as e:
        logger.error(f"yfinance funds_data failed: {e}")
        return None


def _get_static_holdings() -> pd.DataFrame:
    """Return the hardcoded static fallback list."""
    df = pd.DataFrame(_STATIC_HOLDINGS)
    return df.sort_values("weight", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Price Fetching
# ---------------------------------------------------------------------------

def _empty_price() -> dict:
    return {
        "price": None,
        "prev_close": None,
        "change_dollar": None,
        "change_pct": None,
        "valid": False,
    }


def _normalize_ticker(ticker: str) -> str:
    """yfinance uses BRK-B style; Invesco may use BRK.B or BRK/B."""
    return ticker.replace(".", "-").replace("/", "-")


def get_prices_batch(tickers: list) -> dict:
    """
    Fetch intraday current price + previous close for a list of tickers.
    Uses two yf.download() batch calls (one intraday, one daily) to avoid
    per-ticker HTTP requests and reduce rate-limiting risk.

    Returns: {ticker: {price, prev_close, change_dollar, change_pct, valid}}
    """
    if not tickers:
        return {}

    # Normalize tickers for yfinance
    norm_map = {_normalize_ticker(t): t for t in tickers}
    norm_tickers = list(norm_map.keys())
    ticker_str = " ".join(norm_tickers)
    n = len(norm_tickers)

    market = get_market_status()
    is_market_open = market["session"] == "open"

    # --- Intraday data (current price) — only useful when market is open/pre/after ---
    intraday = None
    try:
        intraday = yf.download(
            tickers=ticker_str,
            period="1d",
            interval="2m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        logger.debug(f"Intraday download shape: {getattr(intraday, 'shape', 'None')}")
    except Exception as e:
        logger.error(f"Intraday batch download failed: {e}")

    # Small pause to be gentle with Yahoo's rate limiter
    time.sleep(0.5)

    # --- Daily data (previous close = second-to-last daily bar, last bar = today's close) ---
    daily = None
    try:
        daily = yf.download(
            tickers=ticker_str,
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        logger.debug(f"Daily download shape: {getattr(daily, 'shape', 'None')}")
    except Exception as e:
        logger.error(f"Daily batch download failed: {e}")

    # Build result dict
    results = {}
    for norm_t in norm_tickers:
        original_t = norm_map[norm_t]
        try:
            data = _extract_price(norm_t, intraday, daily, n)
            results[original_t] = data
        except Exception as e:
            logger.warning(f"Price extraction failed for {norm_t}: {e}")
            results[original_t] = _empty_price()

    return results


def _extract_price(ticker: str, intraday, daily, n_tickers: int) -> dict:
    """
    Extract current price and previous close from batch DataFrames.

    When n_tickers > 1: yfinance returns MultiIndex columns (field, ticker).
    When n_tickers == 1: yfinance returns flat columns (field names only).

    During pre-market / after-hours / market closed:
      - intraday data for today may be empty
      - We use daily data: last bar = today's/yesterday's close, second-to-last = prev close
      - This means we show the last available price + previous day's change
    """
    current_price = None
    prev_close = None

    def get_series(df, field):
        """
        Extract a named series from either MultiIndex or flat DataFrame.

        yfinance column order changed across versions:
          - yfinance < 1.0: MultiIndex (field, ticker) e.g. ("Close", "AAPL")
          - yfinance >= 1.0: MultiIndex (ticker, field) e.g. ("AAPL", "Close")
        We try both orderings.
        """
        if df is None or df.empty:
            return None
        try:
            if n_tickers > 1 and hasattr(df.columns, 'levels'):
                levels0 = [str(v) for v in df.columns.get_level_values(0).unique()]
                levels1 = [str(v) for v in df.columns.get_level_values(1).unique()]

                # Detect order: new yfinance puts ticker at level 0
                ticker_at_0 = any(str(ticker).upper() == v.upper() for v in levels0)
                ticker_at_1 = any(str(ticker).upper() == v.upper() for v in levels1)

                if ticker_at_0:
                    # New yfinance: (ticker, field)
                    for t_val in levels0:
                        if t_val.upper() == ticker.upper():
                            for f_val in levels1:
                                if f_val.lower() == field.lower():
                                    s = df[(t_val, f_val)].dropna()
                                    if len(s) > 0:
                                        return s
                if ticker_at_1:
                    # Old yfinance: (field, ticker)
                    for f_val in levels0:
                        if f_val.lower() == field.lower():
                            for t_val in levels1:
                                if t_val.upper() == ticker.upper():
                                    s = df[(f_val, t_val)].dropna()
                                    if len(s) > 0:
                                        return s
            else:
                # Single ticker: flat DataFrame
                for col in [field, field.lower(), field.capitalize(), "Price"]:
                    if col in df.columns:
                        s = df[col].dropna()
                        if len(s) > 0:
                            return s
        except Exception as exc:
            logger.debug(f"get_series({ticker}, {field}): {exc}")
        return None

    # Try intraday for current price (works during market hours)
    s = get_series(intraday, "Close")
    if s is not None and len(s) > 0:
        current_price = float(s.iloc[-1])

    # Daily data for previous close and fallback current price
    s = get_series(daily, "Close")
    if s is not None and len(s) >= 1:
        if len(s) >= 2:
            # During market hours: iloc[-1]=today partial, iloc[-2]=yesterday close
            # After close / pre-market: iloc[-1]=yesterday close, iloc[-2]=day before
            prev_close = float(s.iloc[-2])
        else:
            prev_close = float(s.iloc[-1])

        # If intraday gave nothing (market not yet open), use last daily bar as current
        if current_price is None:
            current_price = float(s.iloc[-1])

    if current_price is None or prev_close is None:
        return _empty_price()

    change_dollar = current_price - prev_close
    change_pct = (change_dollar / prev_close * 100.0) if prev_close != 0 else 0.0

    return {
        "price": round(current_price, 2),
        "prev_close": round(prev_close, 2),
        "change_dollar": round(change_dollar, 2),
        "change_pct": round(change_pct, 4),
        "valid": True,
    }


# ---------------------------------------------------------------------------
# Contribution Calculation
# ---------------------------------------------------------------------------

def calculate_contribution(weight: float, change_pct: float) -> float:
    """
    Approximate contribution of a holding to QQQ's total percentage move.
    contribution = weight (0-1) × change_pct (e.g. 3.5 for +3.5%)
    Result is in percentage points.
    """
    return round(weight * change_pct, 4)


# ---------------------------------------------------------------------------
# Market Cap Fetching
# ---------------------------------------------------------------------------

def _fetch_single_market_cap(ticker: str) -> tuple:
    """Fetch market cap for one ticker. Returns (ticker, market_cap_usd or None)."""
    try:
        mc = yf.Ticker(_normalize_ticker(ticker)).fast_info.market_cap
        return ticker, float(mc) if mc else None
    except Exception:
        return ticker, None


def get_market_caps(tickers: list) -> dict:
    """
    Fetch market cap (USD) for all tickers using parallel requests.
    Cached for 24 hours — market cap is driven mainly by price, which we
    already track separately; this gives us the order-of-magnitude figure.
    Returns: {ticker: market_cap_usd or None}
    """
    now = time.time()
    if (_cache["market_caps"] is not None and
            (now - _cache["market_caps_ts"]) < HOLDINGS_CACHE_TTL):
        return _cache["market_caps"]

    result = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_single_market_cap, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, mc = future.result()
            result[ticker] = mc

    _cache["market_caps"] = result
    _cache["market_caps_ts"] = now
    logger.info(f"Market caps loaded for {len(result)} tickers")
    return result


# ---------------------------------------------------------------------------
# Market Status
# ---------------------------------------------------------------------------

def get_market_status() -> dict:
    """Determine current US market session based on Eastern Time."""
    et = pytz.timezone("America/New_York")
    now = datetime.datetime.now(et)

    base = {
        "session": "closed",
        "label": "Market Closed",
        "refresh_interval": 300,
        "time_et": now.strftime("%I:%M:%S %p ET"),
    }

    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        base["label"] = "Market Closed (Weekend)"
        return base

    t = now.time()
    if datetime.time(4, 0) <= t < datetime.time(9, 30):
        return {**base, "session": "pre",    "label": "Pre-Market",  "refresh_interval": 120}
    if datetime.time(9, 30) <= t < datetime.time(16, 0):
        return {**base, "session": "open",   "label": "Market Open", "refresh_interval": 75}
    if datetime.time(16, 0) <= t < datetime.time(20, 0):
        return {**base, "session": "after",  "label": "After-Hours", "refresh_interval": 120}

    return base


# ---------------------------------------------------------------------------
# Master Data Function
# ---------------------------------------------------------------------------

def get_qqq_data() -> dict:
    """
    Returns the full dataset for the API endpoint.
    Respects the in-memory TTL cache for price data.
    """
    now = time.time()

    # Return cached data if still fresh
    if _cache["data"] is not None and (now - _cache["timestamp"]) < PRICE_CACHE_TTL:
        return _cache["data"]

    market = get_market_status()

    # --- Fetch holdings ---
    holdings_df = get_qqq_holdings()
    tickers = holdings_df["ticker"].tolist()

    # Include QQQ itself in the batch so we get the ETF's own price too
    all_tickers = ["QQQ"] + tickers
    prices = get_prices_batch(all_tickers)

    # --- Fetch market caps (parallel, cached 24h) ---
    market_caps = get_market_caps(tickers)

    # --- QQQ summary ---
    qqq_pd = prices.get("QQQ", _empty_price())

    # --- Build holdings list ---
    holdings_list = []
    total_contribution = 0.0

    for _, row in holdings_df.iterrows():
        ticker = str(row["ticker"])
        weight = float(row["weight"])
        pd_ = prices.get(ticker, _empty_price())

        change_pct = pd_.get("change_pct") or 0.0
        contrib = calculate_contribution(weight, change_pct)
        total_contribution += contrib

        holdings_list.append({
            "ticker": ticker,
            "name": str(row.get("name", ticker)),
            "market_cap": market_caps.get(ticker),   # USD, may be None
            "weight": round(weight * 100, 4),         # stored as % for display (e.g. 9.88)
            "price": pd_.get("price"),
            "change_dollar": pd_.get("change_dollar"),
            "change_pct": change_pct,
            "contribution": contrib,
            "valid": pd_.get("valid", False),
        })

    # Default sort: largest absolute contribution first
    holdings_list.sort(key=lambda x: abs(x["contribution"]), reverse=True)

    result = {
        "qqq": {
            "price": qqq_pd.get("price"),
            "change_dollar": qqq_pd.get("change_dollar"),
            "change_pct": qqq_pd.get("change_pct"),
            "total_contribution": round(total_contribution, 4),
        },
        "holdings": holdings_list,
        "market_status": market,
        "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
    }

    _cache["data"] = result
    _cache["timestamp"] = now
    return result
