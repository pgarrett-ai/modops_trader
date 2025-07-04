"""
Tests for VolSurfacePINN.
"""

import numpy as np
from modops_trader.pinn.vol_surface_pinn import VolSurfacePINN


def test_fit_produces_finite_loss() -> None:
    strikes = np.array([90, 100, 110], dtype=float)
    maturities = np.array([0.1, 0.2, 0.3], dtype=float)
    vols = 0.2 + 0.1 * (strikes - 100) / 100 + 0.05 * maturities
    pinn = VolSurfacePINN(hidden_dim=4)
    loss = pinn.fit(strikes, maturities, vols, epochs=10)
    assert np.isfinite(loss)

