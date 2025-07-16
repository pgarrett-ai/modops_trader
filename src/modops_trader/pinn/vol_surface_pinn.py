"""
modops_trader.pinn.vol_surface_pinn

Week-1 scaffold for the lightweight trading agent.

Author: Codex — generated scaffold.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Tuple

import numpy as np
import torch
from torch import nn

np.random.seed(0)
torch.manual_seed(0)


def _dupire_residual(model: nn.Module, strike: torch.Tensor, maturity: torch.Tensor) -> torch.Tensor:
    inputs = torch.stack([strike, maturity], dim=1).requires_grad_(True)
    sigma = model(inputs)
    grad = torch.autograd.grad(sigma, inputs, torch.ones_like(sigma), create_graph=True)[0]
    d_sigma_dk = grad[:, 0]
    d_sigma_dt = grad[:, 1]
    grad2 = torch.autograd.grad(d_sigma_dk, inputs, torch.ones_like(d_sigma_dk), create_graph=True)[0]
    d2_sigma_dk2 = grad2[:, 0]
    residual = d_sigma_dt - 0.5 * strike ** 2 * d2_sigma_dk2
    return residual


@dataclass
class VolSurfacePINN:
    """Single hidden-layer PINN for implied volatility."""

    hidden_dim: int = 10

    def __post_init__(self) -> None:
        self.model = nn.Sequential(
            nn.Linear(2, self.hidden_dim),
            nn.Tanh(),
            nn.Linear(self.hidden_dim, 1),
            nn.Softplus(),
        )

    def forward(self, strike: torch.Tensor, maturity: torch.Tensor) -> torch.Tensor:
        inputs = torch.stack([strike, maturity], dim=1)
        return self.model(inputs).squeeze(-1)

    def fit(
        self,
        strikes: Iterable[float],
        maturities: Iterable[float],
        vols: Iterable[float],
        lr: float = 1e-3,
        epochs: int = 100,
    ) -> float:
        """Fit to option quotes and PDE residual."""
        strikes_t = torch.tensor(list(strikes), dtype=torch.float32)
        mats_t = torch.tensor(list(maturities), dtype=torch.float32)
        vols_t = torch.tensor(list(vols), dtype=torch.float32)

        opt = torch.optim.Adam(self.model.parameters(), lr=lr)
        for _ in range(epochs):
            opt.zero_grad()
            pred = self.forward(strikes_t, mats_t)
            mse_loss = nn.functional.mse_loss(pred, vols_t)
            pde_loss = _dupire_residual(
                lambda inp: self.forward(inp[:, 0], inp[:, 1]),
                strikes_t,
                mats_t,
            ).pow(2).mean()
            loss = mse_loss + pde_loss
            loss.backward()
            opt.step()
        return float(loss.detach().cpu().item())


if __name__ == "__main__":
    pinn = VolSurfacePINN()
    strikes = np.linspace(80, 120, 10)
    maturities = np.linspace(0.1, 1, 10)
    vols = 0.2 * np.ones_like(strikes)
    loss = pinn.fit(strikes, maturities, vols)
    print(f"Loss: {loss:.4f}")