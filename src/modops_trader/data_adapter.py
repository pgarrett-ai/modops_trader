"""
modops_trader.data_adapter   • production-ready

Streams or back-fills market data, computes fluid-style features, and
hands the results to downstream ingestion pipelines.

Author: pgarrett  • Last updated: 2025-07-07
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import time
from pathlib import Path
from typing import AsyncIterator, Iterator, List

import numpy as np
import pandas as pd
import yaml
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential   # :contentReference[oaicite:7]{index=7}
import yfinance as yf                                              # :contentReference[oaicite:8]{index=8}

# Optional Schwab import (fails gracefully in test environments)
try:
    from schwab.client import Client as SchwabClient
    from schwab.streaming import StreamClient
except ModuleNotFoundError:                                        # unit-tests / CI
    SchwabClient = StreamClient = None                             # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ---------------------------------------------------------------------------#
# 1.  Configuration (YAML → Pydantic)                                        #
# ---------------------------------------------------------------------------#
class AdapterSettings(BaseModel):
    symbols: List[str] = Field(..., description="Tickers to subscribe to")
    depth: int = 5
    backfill_years: int = 6
    retry_attempts: int = 3
    max_backoff: int = 30              # seconds
    kb_root: Path = Path("D:/modops_kb")
    parquet_engine: str = "pyarrow"
    parquet_compress: str = "zstd"

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AdapterSettings":
        with open(path, "r", encoding="utf-8") as fh:
            return cls(**yaml.safe_load(fh))


# ---------------------------------------------------------------------------#
# 2.  Main adapter object                                                    #
# ---------------------------------------------------------------------------#
class DataAdapter:
    """Orchestrates streaming, feature computation, and history back-fill."""

    def __init__(self, cfg: AdapterSettings) -> None:
        self.cfg = cfg
        self._rng = np.random.default_rng(seed=0)

        # Attempt Schwab; fall back to yfinance
        key, secret = os.getenv("SW_APP_KEY"), os.getenv("SW_APP_SECRET")
        if key and secret and SchwabClient:
            logger.info("✅ Using Charles Schwab streaming API")
            self._cli = SchwabClient(key, secret)
            self._stream = StreamClient(self._cli)
            self._use_schwab = True
        else:
            logger.warning("⚠️  Falling back to yfinance polling")
            self._use_schwab = False

    # ------------  STREAMING  ------------------------------------------------
    async def _a_stream_schwab(self, duration: int) -> AsyncIterator[pd.DataFrame]:
        await self._stream.login()
        await self._stream.subscribe(self.cfg.symbols, fields=["quote", "bookDepth"])
        start = time.time()

        async for msg in self._stream.stream():
            ts = pd.Timestamp.utcnow()
            q, depth = msg.get("quote", {}), msg.get("bookDepth", [])
            row = pd.DataFrame(
                {
                    "Open": [q.get("last")],
                    "High": [q.get("last")],
                    "Low": [q.get("last")],
                    "Close": [q.get("last")],
                    "Bid": [q.get("bid")],
                    "Ask": [q.get("ask")],
                    "Depth": [depth[: self.cfg.depth]],
                },
                index=[ts],
            )
            yield self._compute_features(row)

            if time.time() - start > duration:
                break

    def stream_features(self, duration: int = 60) -> Iterator[pd.DataFrame]:
        """Sync wrapper so ingestion scripts can `for df in adapter.stream_features()`."""
        if self._use_schwab:
            return asyncio.run(self._collect_async(duration))
        return self._poll_yfinance(duration)

    async def _collect_async(self, duration: int) -> List[pd.DataFrame]:
        out: List[pd.DataFrame] = []
        async for df in self._a_stream_schwab(duration):
            out.append(df)
        return out

    def _poll_yfinance(self, duration: int) -> Iterator[pd.DataFrame]:
        end = time.time() + duration
        while time.time() < end:
            df = yf.download(
                tickers=self.cfg.symbols,
                period="1d",
                interval="1m",
                progress=False,
            ).iloc[[-1]]
            df["Bid"] = df["Close"] * 0.999
            df["Ask"] = df["Close"] * 1.001
            yield self._compute_features(df)
            time.sleep(1)

    # ------------  HISTORICAL  ----------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=30),
        reraise=True,
    )
    def _fetch_historical_one(self, symbol: str) -> pd.DataFrame:
        end = dt.date.today()
        start = end - dt.timedelta(days=365 * self.cfg.backfill_years)
        if self._use_schwab:
            try:
                df = self._cli.get_historical(
                    symbol, start.isoformat(), end.isoformat(), interval="1d"
                )
                logger.info("Schwab historical ✓ %s", symbol)
                return pd.DataFrame(df)
            except Exception as ex:  # noqa: BLE001
                logger.warning("Schwab historical failed %s → %s", symbol, ex)
        # Yahoo fallback (1m ≤ 7 days) :contentReference[oaicite:9]{index=9}
        df_day = yf.download(symbol, start=start, end=end, interval="1d", progress=False)
        df_min = yf.download(symbol, period="7d", interval="1m", progress=False)
        return pd.concat([df_day, df_min]).drop_duplicates().sort_index()

    def backfill_all(self) -> dict[str, pd.DataFrame]:
        return {s: self._fetch_historical_one(s) for s in self.cfg.symbols}

    # ------------  FEATURE ENGINEERING  -------------------------------------
    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        close = df["Close"].squeeze()
        log_ret = np.log(close).diff().fillna(0.0)
        ewma_vol = log_ret.ewm(span=20).std().fillna(0.0)     # EWMA volatility :contentReference[oaicite:10]{index=10}
        imbalance = (df["Depth"].apply(len) / self.cfg.depth).clip(0, 1)
        reynolds = log_ret.abs() / (ewma_vol + 1e-6)
        vorticity = log_ret.diff().fillna(0.0)
        dissipation = log_ret.pow(2)

        feats = pd.DataFrame(
            {
                "log_ret": log_ret,
                "ewma_vol": ewma_vol,
                "imbalance": imbalance,
                "reynolds": reynolds,
                "vorticity": vorticity,
                "dissipation": dissipation,
            },
            index=df.index,
        )
        return feats