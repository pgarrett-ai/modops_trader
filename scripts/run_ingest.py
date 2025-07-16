"""
Run tick ingestion and feature streaming.
"""

from modops_trader.data_adapter import AdapterSettings, DataAdapter


def main() -> None:
    settings = AdapterSettings(symbols=["AAPL"])
    adapter = DataAdapter(settings)
    for features in adapter.stream_features(duration=60):
        print(features.tail(1))


if __name__ == "__main__":
    main()

