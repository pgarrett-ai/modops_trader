"""
modops_trader

Lightweight trading agent package initialization.

Author: Phillip Garrett
Last updated: 2025-07-07
"""

import sys
from pathlib import Path

# ── Make D:/modops_kb/vector_store importable under WSL ────────────────
sys.path.insert(0, "/mnt/d/modops_kb")

# ── Core adapter and DataFrame alias ──────────────────────────────────
from .data_adapter import DataAdapter, FeatureFrame

# ── Relocated VectorStore ─────────────────────────────────────────────
from vector_store.vector_store import VectorStore  # type: ignore

__all__ = [
    "DataAdapter",
    "FeatureFrame",
    "VectorStore",
]