"""
modops_trader.vector_store

Week-1 scaffold for the lightweight trading agent.

Author: Codex — generated scaffold.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import numpy as np

np.random.seed(0)


@dataclass
class VectorEntry:
    id: str
    vector: np.ndarray
    metadata: Dict | None = None


@dataclass
class SimpleVectorStore:
    """In-memory vector store using cosine similarity."""

    entries: List[VectorEntry] = field(default_factory=list)

    def insert(self, id: str, vector: np.ndarray, metadata: Dict | None = None) -> None:
        self.entries.append(VectorEntry(id=id, vector=vector, metadata=metadata))

    def query(self, vector: np.ndarray, k: int = 5) -> List[Tuple[str, Dict | None, float]]:
        if not self.entries:
            return []
        matrix = np.stack([e.vector for e in self.entries])
        norms = np.linalg.norm(matrix, axis=1) * (np.linalg.norm(vector) + 1e-12)
        sims = matrix @ vector / norms
        idx = np.argsort(sims)[::-1][:k]
        return [
            (self.entries[i].id, self.entries[i].metadata, float(sims[i]))
            for i in idx
        ]

