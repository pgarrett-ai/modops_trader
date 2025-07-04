"""
Tests for DataAdapter.
"""

from modops_trader import DataAdapter


def test_feature_frame() -> None:
    adapter = DataAdapter("AAPL")
    df = adapter.get_feature_frame()
    assert not df.empty
    for col in ["log_ret", "ewma_vol", "reynolds"]:
        assert col in df.columns
    assert df.isna().sum().sum() == 0

