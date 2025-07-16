import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from modops_trader.data_adapter import DataAdapter, AdapterSettings

class TestDataAdapter(unittest.TestCase):

    def setUp(self):
        self.settings = AdapterSettings(
            symbols=["AAPL"],
            depth=5,
            backfill_years=1,
        )
        self.adapter = DataAdapter(self.settings)

    @patch('yfinance.download')
    def test_poll_yfinance(self, mock_download):
        import pandas as pd

    @patch('yfinance.download')
    def test_poll_yfinance(self, mock_download):
        # Create a sample DataFrame to be returned by the mock
        mock_df = pd.DataFrame({
            'Open': [100], 'High': [105], 'Low': [99], 'Close': [102],
            'Volume': [1000], 'Depth': [[]]
        }, index=[pd.to_datetime('2023-01-01')])
        mock_download.return_value = mock_df

        features_iterator = self.adapter._poll_yfinance(duration=1)
        features = next(features_iterator)

        self.assertIsNotNone(features)
        mock_download.assert_called()

if __name__ == '__main__':
    unittest.main()