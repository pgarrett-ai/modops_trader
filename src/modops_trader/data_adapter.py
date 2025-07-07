"""
modops_trader.data_adapter

Fetch tick data via Charles Schwab WebSocket or yfinance fallback,
compute fluid-style features, and backfill multi-year history.

Author: pgrrt
"""

from __future__ import annotations
import os
import time
import asyncio
import logging
import datetime
from dataclasses import dataclass
from typing import Iterator, AsyncIterator, List

import numpy as np
import pandas as pd
import yfinance as yf  # fallback data source 
from tenacity import retry, stop_after_attempt, wait_exponential  # retry logic 

# Schwabdev imports
from schwab.client import Client as SchwabClient  # core API client 
from schwab.streaming import StreamClient         # websockets feed 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

FeatureFrame = pd.DataFrame


@dataclass(frozen=True)
class AdapterConfig:
    symbols: List[str]
    depth: int = 5
    backfill_years: int = 6
    retry_attempts: int = 3
    max_backoff: int = 30  # seconds


class DataAdapter:
    """Orchestrates streaming, feature computation, and history backfill."""

    def __init__(self, cfg: AdapterConfig) -> None:
        self._cfg = cfg
        key = os.getenv("SW_APP_KEY", "")
        secret = os.getenv("SW_APP_SECRET", "")
        if key and secret and SchwabClient:
            logger.info("Using Charles Schwab API")  # :contentReference[oaicite:9]{index=9}
            self._client = SchwabClient(key, secret)
            self._stream = StreamClient(self._client)
            self._use_schwab = True
        else:
            logger.info("Falling back to yfinance")  # :contentReference[oaicite:10]{index=10}
            self._use_schwab = False

        np.random.seed(0)

    async def _stream_schwab(self, duration: int) -> AsyncIterator[FeatureFrame]:
        """Async generator: subscribe & yield features per tick."""
        await self._stream.login()
        # subscribe to quotes + book depth
        await self._stream.subscribe(
            self._cfg.symbols,
            fields=["quote", "bookDepth"]
        )
        start = time.time()
        async for msg in self._stream.stream():
            now = pd.Timestamp.now()
            q = msg.get("quote", {})
            depth = msg.get("bookDepth", [])
            # build a single-row DataFrame
            df = pd.DataFrame(
                {
                    "Open":   [q.get("last")],
                    "High":   [q.get("last")],
                    "Low":    [q.get("last")],
                    "Close":  [q.get("last")],
                    "Bid":    [q.get("bid")],
                    "Ask":    [q.get("ask")],
                    "Depth":  [depth[: self._cfg.depth]]
                },
                index=[now],
            )
            yield self.compute_features(df)
            if time.time() - start > duration:
                break

    def stream_features(self, duration: int = 60) -> Iterator[FeatureFrame]:
        """Synchronous wrapper around async stream or HTTP polling."""
        if self._use_schwab:
            loop = asyncio.get_event_loop()
            agen = self._stream_schwab(duration)
            return loop.run_until_complete(_collect_async(agen))
        else:
            return self._poll_yfinance(duration)

    def _poll_yfinance(self, duration: int) -> Iterator[FeatureFrame]:
        """HTTP-poll last‐row every second from yfinance."""
        end = time.time() + duration
        while time.time() < end:
            df = yf.download(
                tickers=self._cfg.symbols,
                period="1d",
                interval="1m",
                progress=False,
            )
            row = df.iloc[[-1]]
            row["Bid"] = row["Close"] * 0.999
            row["Ask"] = row["Close"] * 1.001
            yield self.compute_features(row)
            time.sleep(1.0)

    @retry(
        stop=stop_after_attempt(AdapterConfig.retry_attempts),
        wait=wait_exponential(multiplier=1, max=AdapterConfig.max_backoff),
        reraise=True,
    )
    def fetch_historical(self, symbol: str) -> pd.DataFrame:
        """Fetch historical bars up to backfill_years."""
        end = datetime.date.today()
        start = end - datetime.timedelta(days=365 * self._cfg.backfill_years)
        if self._use_schwab:
            try:
                df = self._client.get_historical(
                    symbol, start.isoformat(), end.isoformat(), interval="1d"
                )
                logger.info("Schwab historical OK for %s", symbol)
                return pd.DataFrame(df)
            except Exception:
                logger.warning("Schwab historical failed for %s; falling back", symbol)
        # Yahoo intraday limit: 1m for last 7 days, <1d for 60 days 
        df_daily = yf.download(symbol, start=start, end=end, interval="1d", progress=False)
        df_minute = yf.download(symbol, period="7d", interval="1m", progress=False)
        return pd.concat([df_daily, df_minute]).drop_duplicates().sort_index()

    def backfill_all(self) -> dict[str, pd.DataFrame]:
        """One-time multi‐symbol stock/ETF backfill."""
        history = {}
        for s in self._cfg.symbols:
            history[s] = self.fetch_historical(s)
        return history

    def backfill_options(self, symbol: str) -> pd.DataFrame:
        """Fetch current option chain; true historical not available."""
        tk = yf.Ticker(symbol)
        chains = []
        for exp in tk.options:
            calls = tk.option_chain(exp).calls.assign(expiration=exp)
            puts  = tk.option_chain(exp).puts.assign(expiration=exp)
            chains.append(pd.concat([calls, puts]))
        df = pd.concat(chains)
        logger.warning("Live-only options chain for %s; historical unsupported", symbol)
        return df

    def compute_features(self, df: pd.DataFrame) -> FeatureFrame:
        """Compute log-ret, EWMA vol, imbalance, Reynolds, vorticity, dissipation."""
        close = df["Close"].squeeze()
        log_ret = np.log(close).diff().fillna(0.0)
        ewma_vol = log_ret.ewm(span=20).std().fillna(0.0)
        imbalance = (df["Depth"].apply(len).fillna(0) / self._cfg.depth).clip(0,1)
        reynolds = log_ret.abs() / (ewma_vol + 1e-6)
        vorticity = log_ret.diff().fillna(0.0)
        dissipation = log_ret.pow(2)
        feats = pd.DataFrame(
            {
                "log_ret":    log_ret,
                "ewma_vol":   ewma_vol,
                "imbalance":  imbalance,
                "reynolds":   reynolds,
                "vorticity":  vorticity,
                "dissipation": dissipation,
            },
            index=df.index,
        )
        return feats


def _collect_async(agen: AsyncIterator[FeatureFrame]) -> List[FeatureFrame]:
    """Helper: collect async generator into list."""
    items = []
    async def _run():
        async for x in agen:
            items.append(x)
        return items
    return asyncio.get_event_loop().run_until_complete(_run())
