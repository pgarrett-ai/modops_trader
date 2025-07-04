"""
modops_trader.data_adapter

Week-1 scaffold for the lightweight trading agent.

Author: Codex — generated scaffold.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterator

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except Exception:  # pragma: no cover - if yfinance missing
    yf = None

np.random.seed(0)

FeatureFrame = pd.DataFrame


@dataclass
class DataAdapter:
    """Fetch tick data and compute features.

    Args:
        ticker: Symbol to download.
        period: Historical lookback period.
        interval: Data interval.
    """

    ticker: str
    period: str = "1d"
    interval: str = "1m"

    def fetch_ticks(self) -> pd.DataFrame:
        """Download tick data via ``yfinance``.

        Returns:
            Raw OHLC dataframe.
        """
        if yf is None:
            return self._dummy_ticks()
        try:
            df = yf.download(
                tickers=self.ticker,
                period=self.period,
                interval=self.interval,
                progress=False,
            )
        except Exception:
            df = self._dummy_ticks()
        return df

    def _dummy_ticks(self) -> pd.DataFrame:
        freq = "1min" if self.interval.endswith("m") else self.interval
        idx = pd.date_range(
            end=pd.Timestamp.now(), periods=10, freq=freq
        )
        data = np.random.random((len(idx), 4))
        return pd.DataFrame(data, columns=["Open", "High", "Low", "Close"], index=idx)

    def compute_features(self, df: pd.DataFrame) -> FeatureFrame:
        """Compute feature frame.

        Args:
            df: Raw OHLC data.

        Returns:
            FeatureFrame with engineered columns.
        """
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        log_ret = np.log(close).diff().fillna(0.0)
        ewma_vol = log_ret.ewm(span=20).std().fillna(0.0)
        imbalance = pd.Series(0.0, index=df.index)
        reynolds = log_ret.abs() / (ewma_vol + 1e-6)
        vorticity = log_ret.diff().fillna(0.0)
        dissipation = log_ret.pow(2)
        features = pd.DataFrame(
            {
                "log_ret": log_ret,
                "ewma_vol": ewma_vol,
                "imbalance": imbalance,
                "reynolds": reynolds,
                "vorticity": vorticity,
                "dissipation": dissipation,
            }
        )
        return features

    def get_feature_frame(self) -> FeatureFrame:
        """Convenience wrapper to fetch ticks and compute features."""
        raw = self.fetch_ticks()
        return self.compute_features(raw)

    def stream_features(
        self, duration: int = 60, sleep_s: float = 1.0
    ) -> Iterator[FeatureFrame]:
        """Yield features for ``duration`` seconds."""
        end = time.time() + duration
        while time.time() < end:
            yield self.get_feature_frame()
            time.sleep(sleep_s)

