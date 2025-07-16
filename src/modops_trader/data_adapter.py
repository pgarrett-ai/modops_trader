"""
modops_trader.data_adapter   • production-ready (logging & tz-aware)

Streams or back-fills market data, computes fluid-style features, and
hands the results to downstream ingestion pipelines.
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
from tenacity import retry, stop_after_attempt, wait_exponential
import yfinance as yf

from schwab.auth import easy_client
from schwab.streaming import StreamClient

# ────────────────────────────────────────────────────────────────────────────
# 1.  Load global config for retry/backoff defaults
# ────────────────────────────────────────────────────────────────────────────
_CONFIG_PATH = Path("/mnt/d/modops_kb/configs/adapter.yaml")
with open(_CONFIG_PATH, "r", encoding="utf-8") as _fh:
    _GLOBAL_CFG_DICT = yaml.safe_load(_fh)

GLOBAL_SYMBOLS      = _GLOBAL_CFG_DICT["symbols"]
GLOBAL_RETRY_COUNT  = _GLOBAL_CFG_DICT.get("retry_attempts", 3)
GLOBAL_MAX_BACKOFF  = _GLOBAL_CFG_DICT.get("max_backoff", 30)

# ────────────────────────────────────────────────────────────────────────────
# 2.  Configuration (YAML → Pydantic)
# ────────────────────────────────────────────────────────────────────────────
class AdapterSettings(BaseModel):
    symbols:        List[str] = Field(..., description="Tickers to subscribe to")
    depth:          int       = 8
    backfill_years: int       = 6
    retry_attempts: int       = GLOBAL_RETRY_COUNT
    max_backoff:    int       = GLOBAL_MAX_BACKOFF  # seconds
    kb_root:        Path      = Path("/mnt/d/modops_kb")
    parquet_engine: str       = "pyarrow"
    parquet_compress: str     = "zstd"

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AdapterSettings":
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return cls(**data)

# ────────────────────────────────────────────────────────────────────────────
# 3.  Logger setup (fixed format)
# ────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y%m%d %H:%M:%S",
)

# ────────────────────────────────────────────────────────────────────────────
# 4.  Main adapter object
# ────────────────────────────────────────────────────────────────────────────
class DataAdapter:
    """Orchestrates streaming, feature computation, and history back-fill."""

    _DEFAULT_RETRIES     = GLOBAL_RETRY_COUNT
    _DEFAULT_MAX_BACKOFF = GLOBAL_MAX_BACKOFF

    def __init__(self, cfg: AdapterSettings) -> None:
        self.cfg = cfg

        key    = os.getenv("SW_APP_KEY")
        secret = os.getenv("SW_APP_SECRET")
        # Allow override via env, else default to localhost callback and a user‐home token path
        callback_url = os.getenv("SW_CALLBACK_URL", "https://127.0.0.1:8182")
        token_path   = os.getenv(
            "SW_TOKEN_PATH",
            str(Path.home() / ".schwab" / "token.json")
        )

        if key and secret:
            try:
                logger.info("✅ Using Schwab streaming API")
                # Use easy_client to get a properly authenticated Client with session
                self._cli = easy_client(
                    api_key=key,
                    app_secret=secret,
                    callback_url=callback_url,
                    token_path=token_path
                )  # :contentReference[oaicite:3]{index=3}
                self._stream = StreamClient(self._cli)
                self._use_schwab = True
            except Exception as e:
                logger.warning(
                    "⚠️ Failed to initialize Schwab client, falling back to yfinance: %s",
                    e
                )
                self._use_schwab = False
        else:
            logger.warning("⚠️ No Schwab creds—falling back to yfinance polling")
            self._use_schwab = False

    # ── STREAMING ─────────────────────────────────────────────────────────────
    async def _a_stream_schwab(self, duration: int) -> AsyncIterator[pd.DataFrame]:
        await self._stream.login()
        await self._stream.subscribe(self.cfg.symbols, fields=["quote","bookDepth"])
        start = time.time()
        async for msg in self._stream.stream():
            ts = pd.Timestamp.now(tz="UTC")
            q, depth = msg.get("quote", {}), msg.get("bookDepth", [])
            row = pd.DataFrame({
                "Open":  [q.get("last")],
                "High":  [q.get("last")],
                "Low":   [q.get("last")],
                "Close": [q.get("last")],
                "Bid":   [q.get("bid")],
                "Ask":   [q.get("ask")],
                "Depth": [depth[: self.cfg.depth]],
            }, index=[ts])
            yield self._compute_features(row)
            if time.time() - start > duration:
                break

    def stream_features(self, duration: int = 60) -> Iterator[pd.DataFrame]:
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
            df.index = df.index.tz_localize("UTC")
            yield self._compute_features(df)
            time.sleep(1)

    # ── HISTORICAL ───────────────────────────────────────────────────────────
    @retry(
        stop=stop_after_attempt(_DEFAULT_RETRIES),
        wait=wait_exponential(multiplier=1, max=_DEFAULT_MAX_BACKOFF),
        reraise=True,
    )
    def _fetch_historical_one(self, symbol: str) -> pd.DataFrame:
        end   = dt.date.today()
        start = end - dt.timedelta(days=365 * self.cfg.backfill_years)

        if self._use_schwab:
            try:
                df = self._cli.get_historical(
                    symbol, start.isoformat(), end.isoformat(), interval="1d"
                )
                df = pd.DataFrame(df)
                logger.info("🗂 Schwab historical ✓ %s", symbol)
            except Exception as e:
                logger.warning("❌ Schwab failed %s: %s", symbol, e)
                df = yf.download(symbol, start=start, end=end, interval="1d", progress=False)
        else:
            df = yf.download(symbol, start=start, end=end, interval="1d", progress=False)

        df.index = df.index.tz_localize("UTC")
        return df

    def backfill_all(self) -> dict[str, pd.DataFrame]:
        return {s: self._fetch_historical_one(s) for s in self.cfg.symbols}

    # ── FEATURE ENGINEERING ─────────────────────────────────────────────────
    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        close       = df["Close"]
        log_ret     = np.log(close).diff().fillna(0.0)
        ewma_vol    = log_ret.ewm(span=20).std().fillna(0.0)
        imbalance   = (df["Depth"].apply(len) / self.cfg.depth).clip(0,1)
        reynolds    = log_ret.abs() / (ewma_vol + 1e-6)
        vorticity   = log_ret.diff().fillna(0.0)
        dissipation = log_ret.pow(2)

        return pd.DataFrame({
            "log_ret":     log_ret,
            "ewma_vol":    ewma_vol,
            "imbalance":   imbalance,
            "reynolds":    reynolds,
            "vorticity":   vorticity,
            "dissipation": dissipation,
        }, index=df.index)

FeatureFrame = pd.DataFrame