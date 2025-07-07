# modops_trader

Week-1 scaffold for the lightweight trading agent.

## Setup

```bash
pip install -e .[development]
pre-commit install
```

## Quick Start

```bash
python scripts/run_ingest.py
```

The ingestion script streams dummy ticks for 60 seconds and prints the
latest feature row.
