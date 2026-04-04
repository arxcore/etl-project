# ETL-Project

A multi-source economic data ETL pipeline built in Python. Fetches, parses, validates, and stores macroeconomic indicators from multiple government APIs into PostgreSQL.

---

## What It Does

- Fetches economic indicators from BLS, FRED, and BEA APIs
- Parses and validates data using Pydantic v2 models
- Calculates derived metrics: MoM, YoY, QoQ
- Stores results in PostgreSQL
- Structured logging with monitoring support

---

## Tech Stack

| Component | Library |
|---|---|
| Language | Python 3.11+ (Poetry, Pyright strict) |
| Validation | Pydantic v2 |
| Database | PostgreSQL via psycopg2-binary |
| Retry logic | Tenacity |
| Testing | pytest |
| APIs | BLS, FRED, BEA |

---

## Project Structure
```
src/
├── config/
│   ├── constants/        # Shared constants and utils
│   ├── metadata/         # Per-country indicator metadata
│   │   ├── usa/          # labour, price, trade, money, business, consumer
│   │   └── uk/           # labour, price, trade, business, consumer
│   └── settings.py
├── monitoring/
│   └── base_logging/     # Structured logger
├── pipeline/
│   ├── calculation/      # MoM, YoY, QoQ, annualized calculations
│   ├── parsers/          # Per-provider parse layer (BEA, BLS, FRED)
│   ├── processors/       # Data, date, and indicator processors
│   ├── routing/          # RoutingFetcher and parser dispatch
│   └── orchestrator.py
├── providers/            # API fetch + Pydantic models per source
│   ├── bea/
│   ├── bls/
│   └── fred/
└── upload/
    └── postgres/         # PostgreSQL insert logic
tests/
└── unit/                 # Parser unit tests
main.py                   # Entry point / CLI
```

---

## Indicators Covered

### USA

| Category | Indicators |
|---|---|
| Labour | Non-Farm Payrolls, Unemployment Rate, Average Hourly Earnings |
| Price | Core CPI, CPI |
| Money | Fed Funds Rate |
| Trade | Balance of Current Account (BEA) |
| Consumer | Retail Sales |

### UK *(coming soon)*

| Category | Status |
|---|---|
| Labour | configured |
| Price | configured |
| Trade | configured |
| Business | configured |
| Consumer | configured |

---

## Architecture Notes

**Routing pattern** — dict-based dispatch avoids if/else chains:
```python
RoutingFetcher({"bls": BLSFetch(...), "fred": FREDFetch(...), "bea": BEAFetch(...)})
```

**Validation layers (BLS example):**
```
BlsFootnotes → BlsRawData → BlsResult → BlsSeries → BlsRawResponseData
```

**Standardized output across all sources:**
```python
StandardizedItems(name, source, date, value, frequency, processed)
```

**Custom exception hierarchy:**
```
ETLError → FetchError, ParseError, ValidationError
         → BlsError, FredError, BeaError, FilterError, CalculatedError
```

---

## Setup
```bash
# Clone
git clone https://github.com/arxcore/etl-project.git
cd etl-project

# Install dependencies
poetry install

# Set environment variables
cp .env.example .env
# Add BLS_API_KEY, FRED_API_KEY, BEA_API_KEY, and PostgreSQL credentials

# Help
python src/main.py -h

# Run all indicators
python src/main.py

# Run specific indicator
python src/main.py --run single --name US_NFP

# Debug mode
python src/main.py --log debug

# Upload to PostgreSQL
python src/main.py --upload
```

---

## Validated Against Real Data

| Indicator | Source | Status |
|---|---|---|
| Core CPI YoY | BLS | 35 rows |
| Non-Farm Payrolls | BLS | 48 rows |
| Unemployment Rate | BLS | ✓ |
| Average Hourly Earnings MoM | BLS | ✓ |
| Fed Funds Rate | FRED | ✓ |
| Retail Sales MoM | FRED | ✓ |
| Balance of Current Account | BEA | ✓ |

---

