# FundLens ETL Pipeline

A production-grade ETL pipeline for campaign finance analysis, extracting and transforming Federal Election Commission (FEC) data using modern data engineering practices with event-driven orchestration.

## Architecture

Implements a **medallion architecture** (Bronze → Silver → Gold) with event-driven triggers:

```
Daily (M-F @ 1 AM ET):
  API Extraction → Bronze → Silver → Gold

Monthly (1st Sat @ 1 AM ET):
  Bulk Reconciliation → Bronze → Silver → Gold
```

### Three-Layer Design

- **Bronze Layer**: Raw FEC data (API + bulk files) with full lineage tracking
- **Silver Layer**: Cleaned, validated, and enriched analytical data
- **Gold Layer**: Aggregated metrics and business-ready analytics

### Hybrid Update Strategy

**Daily Incremental** (Weekdays)
- Fast API extraction with 1-day lookback
- Captures recent contributions and updates
- Runtime: ~30-60 min (Bronze → Silver → Gold)

**Monthly Reconciliation** (First Saturday)
- Complete bulk file download and UPSERT
- Catches amendments, corrections, and late filings
- Runtime: ~4 hours (Bulk → Silver → Gold)

**Result**: Fast daily updates + comprehensive monthly reconciliation

## Key Features

### Event-Driven Pipeline
- **Triggered transformations**: Silver triggers on Bronze/Bulk completion, Gold triggers on Silver completion
- **Guaranteed ordering**: Each layer completes before next begins
- **Dynamic timing**: No fixed delays, optimal resource utilization
- See: [`docs/TRIGGERED_PIPELINE.md`](docs/TRIGGERED_PIPELINE.md)

### Intelligent Extraction
- **State management**: Per-committee extraction state with automatic checkpointing
- **Smart lookback**: 1-day lookback for daily runs, monthly reconciliation for completeness
- **Resume capability**: Automatic recovery from failures with page-level checkpointing
- See: [`docs/EXTRACTION_STATE_FIX.md`](docs/EXTRACTION_STATE_FIX.md)

### Bulk Data Integration
- **Hybrid approach**: API for incremental + bulk files for reconciliation
- **UPSERT optimization**: Fast duplicate detection, only inserts new records
- **Automatic state updates**: Extraction states synchronized after bulk loads
- See: [`docs/MONTHLY_RECONCILIATION.md`](docs/MONTHLY_RECONCILIATION.md)

### Production-Ready
- **Rate limiting**: FEC API compliance (1000 calls/hour)
- **Fault tolerance**: Retry logic at task and flow levels
- **Data quality**: Validation, cleaning, and enrichment at each layer
- **Monitoring**: Comprehensive logging and Prefect UI integration

## Tech Stack

- **Orchestration**: Prefect 3.0 with event-driven triggers
- **Database**: PostgreSQL with SQLAlchemy 2.0 ORM
- **Data Processing**: Pandas with streaming/chunked processing
- **Infrastructure**: Azure (VM, PostgreSQL, CI/CD via GitHub Actions)
- **Configuration**: Pydantic settings with environment validation

## Quick Start

### View Pipeline Schedule

```bash
poetry run python scripts/deploy.py --summary
```

### Deploy All Flows

```bash
cd /opt/fund-lens-etl
poetry run python scripts/deploy.py --all
```

See: [`docs/REDEPLOY_GUIDE.md`](docs/REDEPLOY_GUIDE.md) for detailed deployment instructions.

## Pipeline Schedule

| Flow | Schedule | Trigger | Runtime |
|------|----------|---------|---------|
| Bronze Ingestion | M-F 1 AM ET | Cron | ~2-4 hours |
| Monthly Bulk | 1st Sat 1 AM ET | Cron | ~2-3 hours |
| Silver Transform | After Bronze/Bulk | Triggered | ~30-60 min |
| Gold Transform | After Silver | Triggered | ~30-60 min |

**Complete pipeline**: Bronze/Bulk → Silver → Gold (all automatic)

See: [`docs/FINAL_PIPELINE_ARCHITECTURE.md`](docs/FINAL_PIPELINE_ARCHITECTURE.md) for complete details.

## Project Structure

```
fund_lens_etl/
├── clients/              # FEC API client with rate limiting
├── extractors/
│   ├── api/             # API extractors (committees, candidates, contributions)
│   └── bulk/            # Bulk file extractors (with chunked processing)
├── loaders/             # Database loaders with UPSERT logic
├── transformers/        # Bronze→Silver→Gold transformations
├── flows/               # Prefect flows
│   ├── bronze_ingestion_flow.py
│   ├── bronze_all_states_flow.py
│   ├── monthly_bulk_reconciliation_flow.py
│   ├── silver_transformation_flow.py
│   └── gold_transformation_flow.py
├── deployments/         # Deployment configurations with triggers
├── utils/               # Extraction state management, rate limiters
└── config.py            # Pydantic settings

scripts/
├── deploy.py                          # Main deployment script
├── download_bulk_data.py              # Download FEC bulk files
├── run_bulk_ingestion.py             # Run bulk ingestion
├── update_extraction_states_from_db.py # Sync extraction states
├── run_full_load_all_states.py        # Initial full backfill for new cycles
└── retry_states.py                    # Retry failed states (troubleshooting)

docs/
├── FINAL_PIPELINE_ARCHITECTURE.md    # Complete architecture overview
├── TRIGGERED_PIPELINE.md              # Event-driven trigger details
├── MONTHLY_RECONCILIATION.md          # Bulk reconciliation guide
├── EXTRACTION_STATE_FIX.md           # State management explanation
├── REDEPLOY_GUIDE.md                 # Deployment instructions
├── DEPLOYMENT_QUICK_START.md         # Quick deployment reference
└── BULK_DATA_DEPLOYMENT.md           # Bulk data download/ingestion
```

## Data Pipeline

### Extraction Strategy

**Daily Incremental (API)**
- All 51 states (50 states + DC)
- 1-day lookback from last extraction
- Committees, candidates, individual contributions (Schedule A)
- Page-by-page streaming with automatic checkpointing

**Monthly Reconciliation (Bulk Files)**
- Complete FEC bulk data files
- UPSERT logic (only inserts new/changed records)
- Updates extraction states for all committees
- Catches amendments and late filings

### Transformation Layers

**Bronze → Silver**
- Text normalization and standardization
- ZIP code formatting, state code uppercasing
- Committee/candidate enrichment via JOINs
- Data quality validation and cleaning

**Silver → Gold**
- Donor aggregations (counts, totals, averages)
- Committee fundraising metrics
- Time-series trends (daily/monthly)
- Donor concentration analysis

## Data Quality

- **Validation**: Pydantic models for configuration, SQLAlchemy constraints
- **Cleaning**: Standardized text, normalized codes, validated dates
- **Deduplication**: UPSERT on unique identifiers (`sub_id`)
- **Lineage**: Full raw JSON preserved in Bronze layer
- **Monitoring**: Record counts, skip rates, error tracking

## Scalability

- **State-agnostic**: Supports all 50 states + DC
- **Multi-cycle**: 2020-2030 election cycles
- **Incremental processing**: 95% reduction in API calls vs full refresh
- **Streaming**: Page-by-page processing for large datasets
- **Efficient reconciliation**: Bulk UPSERT skips existing records

## Development

### Local Testing

```bash
# Install dependencies
poetry install

# Run specific flow
poetry run python -m fund_lens_etl.flows.bronze_ingestion_flow

# View notebooks
jupyter lab notebooks/
```

### Deployment

```bash
# Deploy all flows with triggers
poetry run python scripts/deploy.py --all

# Deploy specific layer
poetry run python scripts/deploy.py --bronze
poetry run python scripts/deploy.py --silver
poetry run python scripts/deploy.py --gold
```

## Monitoring

### Prefect UI

Navigate to: `http://<vm-ip>:4200`

- **Flow Runs**: View execution history and trigger chains
- **Deployments**: See schedules and trigger configurations
- **Events**: Monitor deployment completion events

### Key Metrics

- **Bronze**: Committees extracted, contributions loaded, extraction states updated
- **Silver**: Records transformed, validation pass rate
- **Gold**: Aggregations computed, analytics tables populated

## Documentation

| Document | Description |
|----------|-------------|
| [`FINAL_PIPELINE_ARCHITECTURE.md`](docs/FINAL_PIPELINE_ARCHITECTURE.md) | Complete pipeline overview with diagrams |
| [`TRIGGERED_PIPELINE.md`](docs/TRIGGERED_PIPELINE.md) | Event-driven trigger architecture |
| [`MONTHLY_RECONCILIATION.md`](docs/MONTHLY_RECONCILIATION.md) | Bulk reconciliation details |
| [`EXTRACTION_STATE_FIX.md`](docs/EXTRACTION_STATE_FIX.md) | State management and lookback logic |
| [`REDEPLOY_GUIDE.md`](docs/REDEPLOY_GUIDE.md) | Step-by-step deployment guide |
| [`DEPLOYMENT_QUICK_START.md`](docs/DEPLOYMENT_QUICK_START.md) | Quick deployment reference |
| [`BULK_DATA_DEPLOYMENT.md`](docs/BULK_DATA_DEPLOYMENT.md) | Bulk file download and ingestion |

## CI/CD

Fully automated deployment via GitHub Actions:
- **Infrastructure**: Terraform provisions Azure resources
- **Application**: Auto-deploys flows on push to main
- **Scheduling**: Event-driven execution (no manual intervention)

## Key Improvements

This pipeline implements several optimizations over traditional ETL approaches:

1. **Event-Driven vs Time-Based**: Triggers ensure correct execution order and eliminate idle time
2. **Hybrid Updates**: Daily incremental (fast) + monthly bulk (comprehensive)
3. **Smart Lookback**: 1 day instead of 180 (99% faster daily runs)
4. **Bulk Optimization**: UPSERT skips existing records (10x faster reconciliation)
5. **State Management**: Automatic extraction state synchronization
6. **Unified Pipeline**: Same Silver/Gold flows for both daily and monthly data

---

**Portfolio Project**: Demonstrates modern data engineering practices including medallion architecture, event-driven orchestration, hybrid incremental/bulk processing, extraction state management, production monitoring, and automated CI/CD.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
