
ETL-Project 

A multi-source economic data ETL pipeline built in Python. Fetches, parses, validates, and stores macroeconomic indicators from multiple government APIs into PostgreSQL.


---

What It Does

Fetches economic indicators from BLS, FRED, and BEA APIs

Parses and validates data using Pydantic v2 models

Calculates derived metrics: MoM, YoY, QoQ

Stores results in PostgreSQL

Structured logging with monitoring support



---

Tech Stack

Language: Python (Poetry, Pyright strict, ruff)

Validation: Pydantic v2

Database: PostgreSQL via psycopg2-binary

Retry logic: Tenacity

APIs: BLS (Bureau of Labor Statistics), FRED (Federal Reserve), BEA (Bureau of Economic Analysis)



---

Project Structure

src/  
├── config/  
│   ├── constants/        # Shared constants and utils  
│   ├── metadata/         # Per-country indicator metadata  
│   │   ├── usa/          # US indicators: labour, price, trade, money, business, consumer  
│   │   └── uk/           # UK indicators: labour, price, trade, business, consumer  
│   └── settings.py  
│  
├── monitoring/  
│   └── base_logging/     # Structured logger  
│  
├── pipeline/  
│   ├── calculation/      # MoM, YoY, QoQ, annualized calculations  
│   ├── parsers/          # Per-provider parse layer (BEA, BLS, FRED)  
│   ├── processors/       # Data, date, and indicator processors  
│   ├── routing/          # RoutingFetcher and parser dispatch  
│   │   ├── fetch_manage.py  
│   │   ├── parser_manage.py  
│   │   └── model.py  
│   └── orchestrator.py  
│  
├── providers/            # API fetch + Pydantic models per source  
│   ├── bea/  
│   ├── bls/  
│   └── fred/  
│  
└── upload/  
    └── posegres/        # PostgreSQL insert logic  
        └── psql.py  
tests/
└── unit/            # Parser unit tests 

main.py                   # Entry point / CLI interface


---

Indicators Covered

USA

Category	Indicators

Labour	Non-Farm Payrolls, Unemployment Rate, Average Hourly Earnings
Price	Core CPI, CPI 
Money	Fed Interest Rate
Trade	Retail  Balance of Current Account (BEA)
Consumer	Retail Sales 

# Coming Soon
UK 

Category	Indicators

Labour	(configured)
Price	(configured)
Trade	(configured)
Business	(configured)
Consumer	(configured)



---

# Architecture Notes

Routing pattern — dict-based dispatch avoids if/else chains:

RoutingFetcher({"bls": BLSFetch(...), "fred": FREDFetch(...), "bea": BEAFetch(...)})

Validation layers (BLS example):

BlsFootnotes → BlsRawData → BlsResult → BlsSeries → BlsRawResponseData

Standardized output across all sources:

StandardizedItems(name, source, date, value, frequency, processed)

Custom exception hierarchy:

ETLError → FetchError, ParseError, ValidationError  
         → BlsError, FredError, BeaError, FilterError, CalculatedError...


---

# Setup
```bash
# Clone  
git clone https://github.com/arxcore/etl-project.git  
cd etl-project 
  
# Install dependencies  
poetry install  
  
# Set environment variables  
cp .env.example .env  
# Add your BLS_API_KEY, FRED_API_KEY, BEA_API_KEY, and PostgreSQL credentials  

# Help   
python src/main.py -h

# Run ALL 
python src/main.py 

# Run Specifik Indicator 
python src/main.py --run single --name US_NFP

# Debug
python src/main.py --log debug 

# Upload 
python src/main.py --upload 
```

## Validated Against Real Data

# Indicator	Source	Rows Validated

Core CPI YoY	BLS	35
Non-Farm Payrolls	BLS	48
Unemployment Rate	BLS	✓
Average Hourly Earnings MoM	BLS	✓
Fed Funds Rate	FRED	✓
Retail Sales MoM	FRED	✓
Balance of Current Account	BEA	✓



---
