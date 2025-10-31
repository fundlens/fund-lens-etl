# FundLens ETL Pipeline

A production-grade ETL pipeline for campaign finance analysis, extracting and transforming Federal Election Commission (FEC) data using modern data engineering practices.

## Architecture

Implements a **medallion architecture** (Bronze → Silver → Gold) with:
- **Bronze Layer**: Raw FEC API data with full lineage tracking
- **Silver Layer**: Cleaned, validated, and enriched analytical data
- **Gold Layer**: Aggregated metrics and business-ready analytics

### Key Features

**Incremental Extraction with Lookback**
- Intelligent state tracking with 90-day lookback window to catch late filings and amendments
- Configurable full refresh for reconciliation
- Per-committee extraction state management

**Scalable Design**
- Page-by-page streaming for large datasets (contributions)
- FEC rate limit compliance (1000 calls/hour)
- Fault-tolerant with per-committee retry logic
- UPSERT patterns for idempotent loads

**Data Quality**
- Comprehensive validation and cleaning transformations
- Cross-layer data enrichment (committee/candidate JOINs)
- Automated data quality checks at each layer
- Type-safe models with SQLAlchemy 2.0

## Tech Stack

- **Orchestration**: Prefect 3.0 flows with task-level retry and timeout configuration
- **Database**: PostgreSQL with SQLAlchemy ORM
- **API Client**: Custom FEC API client with token bucket rate limiting
- **Data Processing**: Pandas transformations with incremental processing
- **Configuration**: Pydantic settings with environment validation
- **Development**: Jupyter notebooks for exploration and testing

## Data Pipeline

```
FEC API → Bronze (raw) → Silver (clean) → Gold (aggregated)
           ↓               ↓                ↓
        JSONB           Validated        Analytics
        Full History    + Enriched       Ready
```

### Extraction Strategy

**Entities Extracted**:
- Committees (state/cycle filtered)
- Candidates (with election year validation)
- Individual Contributions (Schedule A, all donors nationwide)

**Incremental Logic**:
```python
# Default: Incremental with lookback
bronze_ingestion_flow(state=USState.MD, election_cycle=2026)

# Monthly: Full refresh for reconciliation
bronze_ingestion_flow(..., full_refresh=True)
```

### Transformation Layers

**Bronze → Silver**
- Text normalization and trimming
- ZIP code standardization (5 digits)
- State code uppercasing
- NaN → NULL conversion for database compatibility
- Committee/candidate enrichment via JOINs
- Duplicate removal by source ID

**Silver → Gold**
- Donor aggregations (contribution counts, totals, averages)
- Committee aggregations (fundraising metrics)
- Time-series aggregations (daily/monthly trends)
- Donor concentration analysis (top donors, diversity metrics)

## Project Structure

```
fund_lens_etl/
├── clients/           # API clients with rate limiting
├── extractors/        # Data extraction from FEC API
├── loaders/           # Database loaders with UPSERT logic
├── transformers/      # Bronze→Silver→Gold transformations
├── flows/             # Prefect orchestration flows
├── models/            # SQLAlchemy ORM models (bronze/silver/gold)
├── utils/             # Rate limiters, extraction state management
└── config.py          # Pydantic settings and validation
```

## Design Highlights

**Separation of Concerns**
- Extractors focus on API pagination and data retrieval
- Loaders handle database operations and conflict resolution
- Transformers implement business logic and data quality rules
- Flows orchestrate end-to-end pipeline execution

**Error Handling**
- Graceful degradation (skip empty committees, continue on errors)
- Comprehensive logging at each pipeline stage
- Extraction state tracking for resume capability
- Configurable retry policies per task type

**Performance Optimizations**
- Streaming extraction (page-by-page) to handle large result sets
- Batch loading with configurable batch sizes
- Incremental extraction reduces API calls by 80-95%
- Token bucket rate limiting prevents API throttling

## Data Quality Features

- **Input Validation**: Pydantic models for configuration
- **Schema Enforcement**: SQLAlchemy typed columns with constraints
- **Data Cleaning**: Standardized text, normalized codes, validated dates
- **Completeness Checks**: Validation tasks track record counts and skip rates
- **Lineage**: Full raw JSON preservation + extraction timestamps

## Scalability

Built to scale beyond initial Maryland 2026 scope:
- State-agnostic extraction (supports all 50 states)
- Multi-cycle support (2020-2030)
- Parallel committee processing capability
- Incremental extraction enables daily updates
- Modular design allows easy addition of new FEC endpoints

## Development

Notebook-driven development approach with 9 exploration/testing notebooks covering:
- API client testing and rate limit validation
- Extractor/loader unit testing
- Transformation logic validation
- End-to-end flow testing
- Data quality verification

---

**Portfolio Project**: Demonstrates modern data engineering practices including medallion architecture, incremental processing, rate limiting, data quality validation, and production-grade orchestration with Prefect.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
