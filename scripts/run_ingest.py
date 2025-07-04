"""
Run tick ingestion and feature streaming.
"""

from modops_trader import DataAdapter


def main() -> None:
    adapter = DataAdapter("AAPL")
    for features in adapter.stream_features(duration=60, sleep_s=5):
        print(features.tail(1))


if __name__ == "__main__":
    main()

