#!/usr/bin/env python3
"""
THE HIVE — Market Scanner Agent
Real-time forex + crypto scanning, technical analysis, and signal generation.
Port: 8903 | DB: /home/zero/hivecode_sandbox/markets.db

FREE APIs only:
  - CoinGecko (crypto prices + history, no key)
  - CoinCap v3 (secondary crypto prices for arbitrage, no key)
  - ExchangeRate API (forex rates, no key)
  - Note: Binance API is geo-blocked (451) from this IP — not used

Chris's forex terminology: long = "plant", short = "prune"
"""

import asyncio
import json
import logging
import math
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import uvicorn

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORT = 8903
DB_PATH = "/home/zero/hivecode_sandbox/markets.db"
SCAN_INTERVAL = 300  # 5 minutes
NERVE_URL = "http://100.70.226.103:8200/api/add"  # ZeroQ nerve

LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("market-scanner")

# Pairs we track
CRYPTO_PAIRS = {
    "BTC/USD": {"coingecko_id": "bitcoin", "coincap_id": "bitcoin"},
    "ETH/USD": {"coingecko_id": "ethereum", "coincap_id": "ethereum"},
    "SOL/USD": {"coingecko_id": "solana", "coincap_id": "solana"},
    "XRP/USD": {"coingecko_id": "ripple", "coincap_id": "xrp"},
}

FOREX_PAIRS = ["USD/JPY", "EUR/USD", "GBP/USD", "USD/CHF"]

ALL_PAIRS = list(CRYPTO_PAIRS.keys()) + FOREX_PAIRS

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db():
    """Create tables if they don't exist."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            pair TEXT NOT NULL,
            price REAL NOT NULL,
            volume REAL DEFAULT 0,
            change_24h REAL DEFAULT 0,
            high_24h REAL DEFAULT 0,
            low_24h REAL DEFAULT 0,
            source TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_prices_pair_ts ON prices(pair, ts);

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            pair TEXT NOT NULL,
            direction TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            reasoning TEXT DEFAULT '',
            rsi REAL DEFAULT 0,
            ma_20 REAL DEFAULT 0,
            ma_50 REAL DEFAULT 0,
            ma_200 REAL DEFAULT 0,
            price REAL DEFAULT 0,
            active INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_signals_pair ON signals(pair, ts);

        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            pair TEXT NOT NULL,
            direction TEXT NOT NULL,
            score REAL NOT NULL,
            potential_pct REAL DEFAULT 0,
            reasoning TEXT DEFAULT '',
            active INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_opps_score ON opportunities(score DESC);

        CREATE TABLE IF NOT EXISTS klines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            ts TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_klines_pair_ts ON klines(pair, ts);

        CREATE TABLE IF NOT EXISTS correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            pair_a TEXT NOT NULL,
            pair_b TEXT NOT NULL,
            correlation REAL NOT NULL,
            window INTEGER DEFAULT 24
        );
    """)
    con.close()
    log.info("Database initialized: %s", DB_PATH)


def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


# ---------------------------------------------------------------------------
# Technical Analysis — pure Python
# ---------------------------------------------------------------------------

def calc_rsi(closes: list[float], period: int = 14) -> float:
    """Calculate RSI from a list of closing prices."""
    if len(closes) < period + 1:
        return 50.0  # neutral if not enough data
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-(period):]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0001
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_sma(values: list[float], period: int) -> float:
    """Simple Moving Average over last `period` values."""
    if len(values) < period:
        return sum(values) / len(values) if values else 0.0
    return sum(values[-period:]) / period


def calc_ema(values: list[float], period: int) -> float:
    """Exponential Moving Average."""
    if not values:
        return 0.0
    if len(values) < period:
        return sum(values) / len(values)
    multiplier = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = (v - ema) * multiplier + ema
    return ema


def find_support_resistance(closes: list[float], window: int = 20) -> dict:
    """Find support/resistance levels from recent price action."""
    if len(closes) < window:
        return {"support": [], "resistance": []}
    recent = closes[-window:]
    highs = []
    lows = []
    for i in range(1, len(recent) - 1):
        if recent[i] > recent[i - 1] and recent[i] > recent[i + 1]:
            highs.append(recent[i])
        if recent[i] < recent[i - 1] and recent[i] < recent[i + 1]:
            lows.append(recent[i])
    # Cluster nearby levels (within 0.5%)
    def cluster(levels: list[float], pct: float = 0.005) -> list[float]:
        if not levels:
            return []
        levels.sort()
        clusters = [[levels[0]]]
        for lv in levels[1:]:
            if abs(lv - clusters[-1][-1]) / clusters[-1][-1] < pct:
                clusters[-1].append(lv)
            else:
                clusters.append([lv])
        return [round(sum(c) / len(c), 6) for c in clusters]

    return {
        "support": cluster(lows)[-3:],  # top 3 nearest supports
        "resistance": cluster(highs)[-3:],
    }


def detect_trend(closes: list[float]) -> str:
    """Determine trend direction."""
    if len(closes) < 50:
        return "sideways"
    ma20 = calc_sma(closes, 20)
    ma50 = calc_sma(closes, 50)
    current = closes[-1]
    if current > ma20 > ma50:
        return "bull"
    elif current < ma20 < ma50:
        return "bear"
    else:
        return "sideways"


def calc_volume_spike(volumes: list[float]) -> float:
    """Return ratio of current volume vs average. >2.0 = spike."""
    if len(volumes) < 2:
        return 1.0
    avg = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else volumes[0]
    if avg == 0:
        return 1.0
    return volumes[-1] / avg


def pearson_correlation(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation between two series."""
    n = min(len(xs), len(ys))
    if n < 5:
        return 0.0
    xs, ys = xs[-n:], ys[-n:]
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0:
        return 0.0
    return num / (dx * dy)


# ---------------------------------------------------------------------------
# Signal Generation
# ---------------------------------------------------------------------------

def generate_signal(
    pair: str,
    price: float,
    closes: list[float],
    volumes: list[float],
) -> Optional[dict]:
    """
    Generate a trading signal based on technical indicators.

    BUY (plant):  RSI < 30 + price > MA200 + volume spike
    SELL (prune): RSI > 70 + price < MA200
    STRONG variants when multiple indicators align.
    """
    if len(closes) < 20:
        return None

    rsi = calc_rsi(closes)
    ma20 = calc_sma(closes, 20)
    ma50 = calc_sma(closes, 50)
    ma200 = calc_sma(closes, 200)
    vol_ratio = calc_volume_spike(volumes)
    trend = detect_trend(closes)
    sr = find_support_resistance(closes)

    reasons = []
    confidence = 50
    direction = None

    # --- BUY logic (plant) ---
    buy_score = 0
    if rsi < 30:
        buy_score += 25
        reasons.append(f"RSI oversold ({rsi:.1f})")
    elif rsi < 40:
        buy_score += 10
        reasons.append(f"RSI low ({rsi:.1f})")

    if price > ma200:
        buy_score += 20
        reasons.append("Price above 200 MA (long-term uptrend)")
    if price > ma50:
        buy_score += 10
        reasons.append("Price above 50 MA")
    if price > ma20:
        buy_score += 5

    if vol_ratio > 2.0:
        buy_score += 15
        reasons.append(f"Volume spike ({vol_ratio:.1f}x avg)")
    elif vol_ratio > 1.5:
        buy_score += 8
        reasons.append(f"Above-avg volume ({vol_ratio:.1f}x)")

    if trend == "bull":
        buy_score += 10
        reasons.append("Bullish trend (MA20 > MA50)")

    # Near support?
    if sr["support"]:
        nearest_support = min(sr["support"], key=lambda s: abs(price - s))
        if abs(price - nearest_support) / price < 0.01:
            buy_score += 10
            reasons.append(f"Near support level ({nearest_support:.4f})")

    # --- SELL logic (prune) ---
    sell_score = 0
    sell_reasons = []
    if rsi > 70:
        sell_score += 25
        sell_reasons.append(f"RSI overbought ({rsi:.1f})")
    elif rsi > 60:
        sell_score += 10
        sell_reasons.append(f"RSI elevated ({rsi:.1f})")

    if price < ma200:
        sell_score += 20
        sell_reasons.append("Price below 200 MA (long-term downtrend)")
    if price < ma50:
        sell_score += 10
        sell_reasons.append("Price below 50 MA")
    if price < ma20:
        sell_score += 5

    if trend == "bear":
        sell_score += 10
        sell_reasons.append("Bearish trend (MA20 < MA50)")

    # Near resistance?
    if sr["resistance"]:
        nearest_resist = min(sr["resistance"], key=lambda r: abs(price - r))
        if abs(price - nearest_resist) / price < 0.01:
            sell_score += 10
            sell_reasons.append(f"Near resistance ({nearest_resist:.4f})")

    # --- Determine signal ---
    if buy_score >= 50:
        direction = "STRONG_BUY" if buy_score >= 70 else "BUY"
        confidence = min(95, buy_score + 10)
    elif sell_score >= 50:
        direction = "STRONG_SELL" if sell_score >= 70 else "SELL"
        confidence = min(95, sell_score + 10)
        reasons = sell_reasons
    elif buy_score > sell_score and buy_score >= 30:
        direction = "BUY"
        confidence = buy_score
    elif sell_score > buy_score and sell_score >= 30:
        direction = "SELL"
        confidence = sell_score
        reasons = sell_reasons
    else:
        return None  # No clear signal

    # Chris lingo
    action_word = "plant" if "BUY" in direction else "prune"

    return {
        "pair": pair,
        "direction": direction,
        "action": action_word,
        "confidence": confidence,
        "reasoning": "; ".join(reasons) if reasons else "Mixed signals",
        "rsi": round(rsi, 2),
        "ma_20": round(ma20, 6),
        "ma_50": round(ma50, 6),
        "ma_200": round(ma200, 6),
        "price": price,
        "volume_ratio": round(vol_ratio, 2),
        "trend": trend,
        "support": sr["support"],
        "resistance": sr["resistance"],
    }


# ---------------------------------------------------------------------------
# Data Fetchers (all FREE, no keys)
# ---------------------------------------------------------------------------

class MarketDataFetcher:
    """Fetches market data from free public APIs."""

    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        # Cache for rate limiting
        self._last_fetch: dict[str, float] = {}

    async def start(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=10.0),
            headers={"User-Agent": "HiveMarketScanner/1.0"},
            follow_redirects=True,
        )

    async def stop(self):
        if self.client:
            await self.client.aclose()

    # --- Crypto via CoinGecko ---
    async def fetch_crypto_prices(self) -> dict:
        """Get current crypto prices from CoinGecko."""
        ids = ",".join(v["coingecko_id"] for v in CRYPTO_PAIRS.values())
        url = (
            f"https://api.coingecko.com/api/v3/simple/price"
            f"?ids={ids}&vs_currencies=usd"
            f"&include_24hr_vol=true&include_24hr_change=true"
        )
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            results = {}
            for pair, meta in CRYPTO_PAIRS.items():
                cg = data.get(meta["coingecko_id"], {})
                results[pair] = {
                    "price": cg.get("usd", 0),
                    "volume": cg.get("usd_24h_vol", 0),
                    "change_24h": cg.get("usd_24h_change", 0),
                    "source": "coingecko",
                }
            return results
        except Exception as e:
            log.warning("CoinGecko fetch failed: %s", e)
            return {}

    # --- Crypto klines via Binance (for RSI/MA calculation) ---
    async def fetch_crypto_klines(self, pair: str, interval: str = "1h", limit: int = 200) -> list[dict]:
        """Get OHLCV klines from Binance public API."""
        symbol = CRYPTO_PAIRS.get(pair, {}).get("binance")
        if not symbol:
            return []
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            raw = resp.json()
            klines = []
            for k in raw:
                klines.append({
                    "ts": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc).isoformat(),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                })
            return klines
        except Exception as e:
            log.warning("Binance klines failed for %s: %s", pair, e)
            return []

    # --- Crypto 24h ticker via Binance ---
    async def fetch_binance_ticker(self, pair: str) -> dict:
        """Get 24h ticker stats from Binance."""
        symbol = CRYPTO_PAIRS.get(pair, {}).get("binance")
        if not symbol:
            return {}
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            d = resp.json()
            return {
                "price": float(d.get("lastPrice", 0)),
                "volume": float(d.get("volume", 0)),
                "quote_volume": float(d.get("quoteVolume", 0)),
                "change_24h": float(d.get("priceChangePercent", 0)),
                "high_24h": float(d.get("highPrice", 0)),
                "low_24h": float(d.get("lowPrice", 0)),
                "source": "binance",
            }
        except Exception as e:
            log.warning("Binance ticker failed for %s: %s", pair, e)
            return {}

    # --- Forex via ExchangeRate API ---
    async def fetch_forex_rates(self) -> dict:
        """Get forex rates from free ExchangeRate API."""
        url = "https://open.er-api.com/v6/latest/USD"
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            rates = data.get("rates", {})
            results = {}
            for pair in FOREX_PAIRS:
                base, quote = pair.split("/")
                if base == "USD":
                    # USD/JPY = rate for JPY
                    price = rates.get(quote, 0)
                else:
                    # EUR/USD = 1 / rate for EUR
                    rate = rates.get(base, 0)
                    price = 1.0 / rate if rate else 0
                results[pair] = {
                    "price": round(price, 6),
                    "volume": 0,  # ExchangeRate API doesn't provide volume
                    "change_24h": 0,  # No historical data in free tier
                    "source": "exchangerate-api",
                }
            return results
        except Exception as e:
            log.warning("ExchangeRate fetch failed: %s", e)
            return {}

    # --- Forex klines approximation via stored data ---
    # The free forex API only gives spot rates. We build klines from
    # our own stored price history over time.
    def get_forex_klines_from_db(self, pair: str, limit: int = 200) -> list[dict]:
        """Build pseudo-klines from our stored price history."""
        con = get_db()
        try:
            rows = con.execute(
                "SELECT ts, price, volume FROM prices WHERE pair=? ORDER BY ts DESC LIMIT ?",
                (pair, limit),
            ).fetchall()
            rows = list(reversed(rows))
            return [
                {
                    "ts": r["ts"],
                    "open": r["price"],
                    "high": r["price"],
                    "low": r["price"],
                    "close": r["price"],
                    "volume": r["volume"],
                }
                for r in rows
            ]
        finally:
            con.close()


# ---------------------------------------------------------------------------
# Scanner Engine
# ---------------------------------------------------------------------------

class MarketScanner:
    """Core scanning engine: fetches data, runs analysis, generates signals."""

    def __init__(self):
        self.fetcher = MarketDataFetcher()
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self.last_scan: Optional[str] = None
        self.scan_count = 0
        self.latest_prices: dict = {}
        self.latest_signals: list = []
        self.latest_opportunities: list = []

    async def start(self):
        await self.fetcher.start()
        self.running = True
        self._task = asyncio.create_task(self._scan_loop())
        log.info("Market scanner started (every %ds)", SCAN_INTERVAL)

    async def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.fetcher.stop()

    async def _scan_loop(self):
        """Main scanning loop."""
        # Initial scan immediately
        await self._run_scan()
        while self.running:
            await asyncio.sleep(SCAN_INTERVAL)
            if self.running:
                await self._run_scan()

    async def _run_scan(self):
        """Execute a full market scan."""
        t0 = time.monotonic()
        log.info("=== Scan #%d starting ===", self.scan_count + 1)
        now = datetime.now(timezone.utc).isoformat()

        try:
            # Fetch all data in parallel
            n_crypto = len(CRYPTO_PAIRS)
            results = await asyncio.gather(
                self.fetcher.fetch_crypto_prices(),      # index 0
                self.fetcher.fetch_forex_rates(),         # index 1
                # Binance tickers for each crypto pair   # indices 2 .. 2+n-1
                *[self.fetcher.fetch_binance_ticker(p) for p in CRYPTO_PAIRS],
                # Binance klines for each crypto pair    # indices 2+n .. 2+2n-1
                *[self.fetcher.fetch_crypto_klines(p) for p in CRYPTO_PAIRS],
                return_exceptions=True,
            )

            crypto_prices_cg = results[0]
            forex_rates = results[1]
            binance_tickers = list(results[2:2 + n_crypto])
            crypto_klines = list(results[2 + n_crypto:2 + 2 * n_crypto])

            # Handle exceptions from gather
            if isinstance(crypto_prices_cg, Exception):
                log.error("CoinGecko error: %s", crypto_prices_cg)
                crypto_prices_cg = {}
            if isinstance(forex_rates, Exception):
                log.error("Forex error: %s", forex_rates)
                forex_rates = {}

            all_prices = {}
            all_signals = []
            con = get_db()

            # --- Process crypto ---
            crypto_pair_list = list(CRYPTO_PAIRS.keys())
            for i, pair in enumerate(crypto_pair_list):
                # Merge CoinGecko + Binance data (Binance takes priority for price)
                cg_data = crypto_prices_cg.get(pair, {}) if isinstance(crypto_prices_cg, dict) else {}
                bn_ticker = binance_tickers[i] if i < len(binance_tickers) and not isinstance(binance_tickers[i], Exception) else {}
                bn_klines = crypto_klines[i] if i < len(crypto_klines) and not isinstance(crypto_klines[i], Exception) else []

                price = bn_ticker.get("price") or cg_data.get("price", 0)
                volume = bn_ticker.get("quote_volume") or cg_data.get("volume", 0)
                change = bn_ticker.get("change_24h") or cg_data.get("change_24h", 0)
                high_24h = bn_ticker.get("high_24h", 0)
                low_24h = bn_ticker.get("low_24h", 0)

                if price <= 0:
                    continue

                # Store price
                con.execute(
                    "INSERT INTO prices (ts, pair, price, volume, change_24h, high_24h, low_24h, source) VALUES (?,?,?,?,?,?,?,?)",
                    (now, pair, price, volume, change, high_24h, low_24h, "binance+coingecko"),
                )

                # Store klines
                if bn_klines:
                    # Only store the latest kline to avoid duplicates
                    latest_k = bn_klines[-1]
                    existing = con.execute(
                        "SELECT id FROM klines WHERE pair=? AND ts=?",
                        (pair, latest_k["ts"]),
                    ).fetchone()
                    if not existing:
                        con.execute(
                            "INSERT INTO klines (pair, ts, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                            (pair, latest_k["ts"], latest_k["open"], latest_k["high"],
                             latest_k["low"], latest_k["close"], latest_k["volume"]),
                        )

                # Build closes + volumes for TA
                closes = [k["close"] for k in bn_klines] if bn_klines else []
                volumes = [k["volume"] for k in bn_klines] if bn_klines else []

                # Calculate indicators
                rsi = calc_rsi(closes) if closes else 50.0
                ma20 = calc_sma(closes, 20) if closes else price
                ma50 = calc_sma(closes, 50) if closes else price
                ma200 = calc_sma(closes, 200) if closes else price
                trend = detect_trend(closes) if closes else "sideways"
                sr = find_support_resistance(closes) if closes else {"support": [], "resistance": []}

                all_prices[pair] = {
                    "price": price,
                    "volume": volume,
                    "change_24h": round(change, 2),
                    "high_24h": high_24h,
                    "low_24h": low_24h,
                    "rsi": round(rsi, 2),
                    "ma_20": round(ma20, 2),
                    "ma_50": round(ma50, 2),
                    "ma_200": round(ma200, 2),
                    "trend": trend,
                    "support": sr["support"],
                    "resistance": sr["resistance"],
                    "type": "crypto",
                }

                # Generate signal
                if closes and volumes:
                    sig = generate_signal(pair, price, closes, volumes)
                    if sig:
                        all_signals.append(sig)
                        # Deactivate old signals for this pair
                        con.execute(
                            "UPDATE signals SET active=0 WHERE pair=? AND active=1",
                            (pair,),
                        )
                        con.execute(
                            "INSERT INTO signals (ts, pair, direction, confidence, reasoning, rsi, ma_20, ma_50, ma_200, price, active) VALUES (?,?,?,?,?,?,?,?,?,?,1)",
                            (now, sig["pair"], sig["direction"], sig["confidence"],
                             sig["reasoning"], sig["rsi"], sig["ma_20"], sig["ma_50"],
                             sig["ma_200"], sig["price"]),
                        )

            # --- Process forex ---
            if isinstance(forex_rates, dict):
                for pair, fdata in forex_rates.items():
                    price = fdata.get("price", 0)
                    if price <= 0:
                        continue

                    con.execute(
                        "INSERT INTO prices (ts, pair, price, volume, change_24h, source) VALUES (?,?,?,?,?,?)",
                        (now, pair, price, 0, 0, "exchangerate-api"),
                    )

                    # Get historical closes from our DB for TA
                    db_klines = self.fetcher.get_forex_klines_from_db(pair, 200)
                    closes = [k["close"] for k in db_klines]
                    volumes = [k["volume"] for k in db_klines]

                    # Need at least some history for meaningful TA
                    rsi = calc_rsi(closes) if len(closes) > 14 else 50.0
                    ma20 = calc_sma(closes, 20) if closes else price
                    ma50 = calc_sma(closes, 50) if closes else price
                    ma200 = calc_sma(closes, 200) if closes else price
                    trend = detect_trend(closes) if len(closes) > 50 else "sideways"
                    sr = find_support_resistance(closes) if closes else {"support": [], "resistance": []}

                    all_prices[pair] = {
                        "price": price,
                        "volume": 0,
                        "change_24h": 0,
                        "rsi": round(rsi, 2),
                        "ma_20": round(ma20, 6),
                        "ma_50": round(ma50, 6),
                        "ma_200": round(ma200, 6),
                        "trend": trend,
                        "support": sr["support"],
                        "resistance": sr["resistance"],
                        "type": "forex",
                    }

                    # Forex signals (only if enough history)
                    if len(closes) > 20:
                        sig = generate_signal(pair, price, closes, volumes)
                        if sig:
                            all_signals.append(sig)
                            con.execute(
                                "UPDATE signals SET active=0 WHERE pair=? AND active=1",
                                (pair,),
                            )
                            con.execute(
                                "INSERT INTO signals (ts, pair, direction, confidence, reasoning, rsi, ma_20, ma_50, ma_200, price, active) VALUES (?,?,?,?,?,?,?,?,?,?,1)",
                                (now, sig["pair"], sig["direction"], sig["confidence"],
                                 sig["reasoning"], sig["rsi"], sig["ma_20"], sig["ma_50"],
                                 sig["ma_200"], sig["price"]),
                            )

            # --- Calculate correlations ---
            await self._calc_correlations(con, now)

            # --- Rank opportunities ---
            opportunities = self._rank_opportunities(all_prices, all_signals, now)
            # Deactivate old and insert new
            con.execute("UPDATE opportunities SET active=0 WHERE active=1")
            for opp in opportunities:
                con.execute(
                    "INSERT INTO opportunities (ts, pair, direction, score, potential_pct, reasoning, active) VALUES (?,?,?,?,?,?,1)",
                    (now, opp["pair"], opp["direction"], opp["score"],
                     opp["potential_pct"], opp["reasoning"]),
                )

            con.commit()
            con.close()

            # Update cached state
            self.latest_prices = all_prices
            self.latest_signals = all_signals
            self.latest_opportunities = opportunities
            self.last_scan = now
            self.scan_count += 1

            elapsed = time.monotonic() - t0
            log.info(
                "=== Scan #%d complete (%.1fs) — %d prices, %d signals, %d opportunities ===",
                self.scan_count, elapsed, len(all_prices), len(all_signals), len(opportunities),
            )

            # Feed top signals to nerve (best effort)
            await self._feed_nerve(all_signals, opportunities)

        except Exception as e:
            log.exception("Scan failed: %s", e)

    async def _calc_correlations(self, con: sqlite3.Connection, now: str):
        """Calculate pairwise correlations from recent price history."""
        # Get last 24 data points per pair
        pair_closes: dict[str, list[float]] = {}
        for pair in ALL_PAIRS:
            rows = con.execute(
                "SELECT price FROM prices WHERE pair=? ORDER BY ts DESC LIMIT 24",
                (pair,),
            ).fetchall()
            if rows:
                pair_closes[pair] = [r["price"] for r in reversed(rows)]

        pairs_list = list(pair_closes.keys())
        for i in range(len(pairs_list)):
            for j in range(i + 1, len(pairs_list)):
                pa, pb = pairs_list[i], pairs_list[j]
                corr = pearson_correlation(pair_closes[pa], pair_closes[pb])
                con.execute(
                    "INSERT INTO correlations (ts, pair_a, pair_b, correlation, window) VALUES (?,?,?,?,?)",
                    (now, pa, pb, round(corr, 4), 24),
                )

    def _rank_opportunities(
        self,
        prices: dict,
        signals: list,
        now: str,
    ) -> list[dict]:
        """Rank all pairs by profit potential."""
        opps = []
        for pair, pdata in prices.items():
            score = 0.0
            reasons = []
            direction = "HOLD"

            # Check for active signal
            sig = next((s for s in signals if s["pair"] == pair), None)

            # Volatility = potential profit
            change = abs(pdata.get("change_24h", 0))
            if change > 5:
                score += 30
                reasons.append(f"High volatility ({change:.1f}% 24h)")
            elif change > 2:
                score += 15
                reasons.append(f"Moderate volatility ({change:.1f}% 24h)")

            # Volume matters
            vol = pdata.get("volume", 0)
            if vol > 1_000_000_000:
                score += 20
                reasons.append("Very high volume (>$1B)")
            elif vol > 100_000_000:
                score += 10
                reasons.append("Good volume")

            # Clear trend = easier to trade
            trend = pdata.get("trend", "sideways")
            if trend != "sideways":
                score += 15
                reasons.append(f"Clear {trend} trend")
                direction = "BUY" if trend == "bull" else "SELL"

            # RSI extremes = potential reversal profit
            rsi = pdata.get("rsi", 50)
            if rsi < 25 or rsi > 75:
                score += 20
                reasons.append(f"RSI extreme ({rsi:.0f})")

            # Signal confidence
            if sig:
                score += sig["confidence"] * 0.3
                direction = sig["direction"]
                reasons.append(f"Signal: {sig['direction']} ({sig['confidence']}%)")

            # Crypto gets slight bonus (more volatile = more opportunity)
            if pdata.get("type") == "crypto":
                score += 5

            # USD/JPY bonus (our primary pair)
            if pair == "USD/JPY":
                score += 10
                reasons.append("Primary trading pair")

            # Estimate potential profit % from support/resistance
            potential = 0.0
            if direction in ("BUY", "STRONG_BUY") and pdata.get("resistance"):
                nearest_r = min(pdata["resistance"], key=lambda r: abs(r - pdata["price"]))
                if nearest_r > pdata["price"]:
                    potential = ((nearest_r - pdata["price"]) / pdata["price"]) * 100
            elif direction in ("SELL", "STRONG_SELL") and pdata.get("support"):
                nearest_s = min(pdata["support"], key=lambda s: abs(s - pdata["price"]))
                if nearest_s < pdata["price"]:
                    potential = ((pdata["price"] - nearest_s) / pdata["price"]) * 100

            if potential > 0:
                score += potential * 5
                reasons.append(f"~{potential:.2f}% to next level")

            action = "plant" if "BUY" in direction else "prune" if "SELL" in direction else "watch"

            opps.append({
                "pair": pair,
                "direction": direction,
                "action": action,
                "score": round(score, 1),
                "potential_pct": round(potential, 3),
                "reasoning": "; ".join(reasons),
                "price": pdata["price"],
                "rsi": rsi,
                "trend": trend,
                "type": pdata.get("type", "unknown"),
            })

        opps.sort(key=lambda x: x["score"], reverse=True)
        return opps

    async def _feed_nerve(self, signals: list, opportunities: list):
        """Send top signals to nerve for Hive awareness."""
        if not signals and not opportunities:
            return

        # Build a summary for nerve
        parts = []
        for sig in sorted(signals, key=lambda s: s["confidence"], reverse=True)[:3]:
            action = "plant" if "BUY" in sig["direction"] else "prune"
            parts.append(
                f"{sig['pair']}: {sig['direction']} ({action}) confidence={sig['confidence']}% — {sig['reasoning']}"
            )

        if opportunities:
            top = opportunities[0]
            parts.append(
                f"Top opportunity: {top['pair']} ({top['action']}) score={top['score']} — {top['reasoning']}"
            )

        if not parts:
            return

        payload = {
            "category": "market_signal",
            "content": " | ".join(parts),
            "source": "market_scanner",
            "confidence": max((s["confidence"] for s in signals), default=50) / 100.0,
        }

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(NERVE_URL, json=payload)
                if resp.status_code < 300:
                    log.info("Fed %d signals to nerve", len(parts))
                else:
                    log.debug("Nerve returned %d (may be offline)", resp.status_code)
        except Exception:
            log.debug("Nerve unreachable (ZeroQ may be offline)")

    # --- Public query methods ---

    def get_active_signals(self) -> list[dict]:
        con = get_db()
        try:
            rows = con.execute(
                "SELECT * FROM signals WHERE active=1 ORDER BY confidence DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

    def get_price_history(self, pair: str, limit: int = 100) -> list[dict]:
        con = get_db()
        try:
            rows = con.execute(
                "SELECT ts, price, volume, change_24h FROM prices WHERE pair=? ORDER BY ts DESC LIMIT ?",
                (pair, limit),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]
        finally:
            con.close()

    def get_correlations(self) -> list[dict]:
        con = get_db()
        try:
            rows = con.execute(
                """SELECT pair_a, pair_b, correlation, window
                   FROM correlations
                   WHERE ts = (SELECT MAX(ts) FROM correlations)
                   ORDER BY ABS(correlation) DESC"""
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

    def get_arbitrage_opportunities(self) -> list[dict]:
        """
        Identify crypto arbitrage by comparing CoinGecko vs Binance prices.
        Also flag large spreads between correlated pairs.
        """
        arbs = []
        con = get_db()
        try:
            # Check for price discrepancies between sources
            for pair in CRYPTO_PAIRS:
                rows = con.execute(
                    """SELECT source, price FROM prices
                       WHERE pair=? AND ts=(SELECT MAX(ts) FROM prices WHERE pair=?)""",
                    (pair, pair),
                ).fetchall()
                if len(rows) >= 2:
                    prices_by_source = {r["source"]: r["price"] for r in rows}
                    if len(prices_by_source) >= 2:
                        vals = list(prices_by_source.values())
                        spread_pct = abs(vals[0] - vals[1]) / min(vals) * 100
                        if spread_pct > 0.1:
                            arbs.append({
                                "type": "cross_exchange",
                                "pair": pair,
                                "spread_pct": round(spread_pct, 4),
                                "sources": prices_by_source,
                                "potential": f"~{spread_pct:.3f}% spread",
                            })

            # Check for correlation breaks (pairs that usually move together diverging)
            corrs = self.get_correlations()
            for c in corrs:
                if abs(c["correlation"]) > 0.8:
                    # These pairs are highly correlated — check for divergence
                    pa_price = self.latest_prices.get(c["pair_a"], {})
                    pb_price = self.latest_prices.get(c["pair_b"], {})
                    ca = pa_price.get("change_24h", 0)
                    cb = pb_price.get("change_24h", 0)
                    if abs(ca - cb) > 3:
                        arbs.append({
                            "type": "correlation_divergence",
                            "pair_a": c["pair_a"],
                            "pair_b": c["pair_b"],
                            "correlation": c["correlation"],
                            "change_a": ca,
                            "change_b": cb,
                            "divergence_pct": round(abs(ca - cb), 2),
                            "potential": f"{c['pair_a']} and {c['pair_b']} diverged by {abs(ca-cb):.1f}% despite {c['correlation']:.2f} correlation — mean reversion expected",
                        })
        finally:
            con.close()
        return arbs

    def get_full_analysis(self) -> dict:
        """Compile complete market analysis."""
        signals = self.latest_signals
        buy_signals = [s for s in signals if "BUY" in s.get("direction", "")]
        sell_signals = [s for s in signals if "SELL" in s.get("direction", "")]

        # Market sentiment
        total_change = sum(
            p.get("change_24h", 0) for p in self.latest_prices.values()
        )
        avg_change = total_change / len(self.latest_prices) if self.latest_prices else 0

        if avg_change > 2:
            sentiment = "bullish"
        elif avg_change < -2:
            sentiment = "bearish"
        else:
            sentiment = "neutral"

        return {
            "timestamp": self.last_scan,
            "scan_count": self.scan_count,
            "market_sentiment": sentiment,
            "avg_24h_change": round(avg_change, 2),
            "total_pairs_tracked": len(self.latest_prices),
            "active_buy_signals": len(buy_signals),
            "active_sell_signals": len(sell_signals),
            "top_opportunity": self.latest_opportunities[0] if self.latest_opportunities else None,
            "prices": self.latest_prices,
            "signals": signals,
            "opportunities": self.latest_opportunities,
            "correlations": self.get_correlations(),
            "arbitrage": self.get_arbitrage_opportunities(),
            "chris_summary": self._chris_summary(signals, self.latest_opportunities, sentiment),
        }

    def _chris_summary(self, signals: list, opps: list, sentiment: str) -> str:
        """Plain English summary for Chris."""
        parts = [f"Market is {sentiment}."]

        if opps:
            top = opps[0]
            action = top.get("action", "watch")
            parts.append(
                f"Best move right now: {action} {top['pair']} "
                f"(score {top['score']}, {top['reasoning']})."
            )

        plants = [s for s in signals if s.get("action") == "plant"]
        prunes = [s for s in signals if s.get("action") == "prune"]

        if plants:
            pp = ", ".join(f"{s['pair']}({s['confidence']}%)" for s in plants[:3])
            parts.append(f"Plant signals: {pp}.")
        if prunes:
            pp = ", ".join(f"{s['pair']}({s['confidence']}%)" for s in prunes[:3])
            parts.append(f"Prune signals: {pp}.")

        if not signals:
            parts.append("No strong signals right now — markets are quiet.")

        return " ".join(parts)


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

scanner = MarketScanner()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await scanner.start()
    yield
    await scanner.stop()


app = FastAPI(
    title="Hive Market Scanner",
    description="Real-time forex + crypto market scanning, TA, and signal generation",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "hive-market-scanner",
        "port": PORT,
        "scan_count": scanner.scan_count,
        "last_scan": scanner.last_scan,
        "pairs_tracked": len(scanner.latest_prices),
        "active_signals": len(scanner.latest_signals),
        "uptime_scans": scanner.scan_count,
    }


@app.get("/api/prices")
async def get_prices():
    """Current prices for all tracked pairs with indicators."""
    if not scanner.latest_prices:
        return {"status": "warming_up", "message": "First scan in progress, data will be available shortly."}
    return {
        "timestamp": scanner.last_scan,
        "pairs": scanner.latest_prices,
        "count": len(scanner.latest_prices),
    }


@app.get("/api/signals")
async def get_signals(active_only: bool = True):
    """Active buy/sell signals. Chris lingo: buy=plant, sell=prune."""
    if active_only:
        signals = scanner.latest_signals
    else:
        signals = scanner.get_active_signals()

    # Add Chris-friendly labels
    for s in signals:
        if "action" not in s:
            s["action"] = "plant" if "BUY" in s.get("direction", "") else "prune"

    return {
        "timestamp": scanner.last_scan,
        "signals": signals,
        "count": len(signals),
        "legend": {
            "plant": "BUY / go long",
            "prune": "SELL / go short",
            "STRONG_BUY": "Multiple indicators aligned — high confidence plant",
            "STRONG_SELL": "Multiple indicators aligned — high confidence prune",
        },
    }


@app.get("/api/opportunities")
async def get_opportunities():
    """Ranked profit opportunities across all markets."""
    return {
        "timestamp": scanner.last_scan,
        "opportunities": scanner.latest_opportunities,
        "count": len(scanner.latest_opportunities),
        "best": scanner.latest_opportunities[0] if scanner.latest_opportunities else None,
    }


@app.get("/api/history/{pair:path}")
async def get_history(pair: str, limit: int = Query(default=100, le=1000)):
    """Price history for a specific pair. Use format like BTC/USD or USD/JPY."""
    pair = pair.upper()
    if pair not in ALL_PAIRS:
        raise HTTPException(404, f"Unknown pair: {pair}. Available: {ALL_PAIRS}")
    history = scanner.get_price_history(pair, limit)
    return {
        "pair": pair,
        "history": history,
        "count": len(history),
    }


@app.get("/api/analysis")
async def get_analysis():
    """Full market analysis with all indicators, signals, correlations, and opportunities."""
    return scanner.get_full_analysis()


@app.get("/api/correlations")
async def get_correlations():
    """Pairwise correlations between all tracked instruments."""
    corrs = scanner.get_correlations()
    return {
        "timestamp": scanner.last_scan,
        "correlations": corrs,
        "count": len(corrs),
        "note": "Values near +1.0 = move together, near -1.0 = move opposite, near 0 = independent",
    }


@app.get("/api/arbitrage")
async def get_arbitrage():
    """Crypto arbitrage opportunities (cross-exchange + correlation divergence)."""
    arbs = scanner.get_arbitrage_opportunities()
    return {
        "timestamp": scanner.last_scan,
        "opportunities": arbs,
        "count": len(arbs),
    }


@app.get("/api/pair/{pair:path}")
async def get_pair_detail(pair: str):
    """Detailed view of a single pair: price, indicators, signal, history."""
    pair = pair.upper()
    if pair not in ALL_PAIRS:
        raise HTTPException(404, f"Unknown pair: {pair}. Available: {ALL_PAIRS}")

    price_data = scanner.latest_prices.get(pair, {})
    signal = next((s for s in scanner.latest_signals if s["pair"] == pair), None)
    opp = next((o for o in scanner.latest_opportunities if o["pair"] == pair), None)
    history = scanner.get_price_history(pair, 50)

    return {
        "pair": pair,
        "current": price_data,
        "signal": signal,
        "opportunity": opp,
        "history_last_50": history,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting Hive Market Scanner on port %d", PORT)
    uvicorn.run(
        "market_scanner:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        reload=False,
    )
