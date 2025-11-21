# Bulk Data Deployment Guide

Complete guide for deploying bulk data ingestion on your ETL VM.

## Prerequisites

- SSH access to your ETL VM
- PostgreSQL database accessible from VM
- ~5 GB free disk space (for 2026 cycle data)
- Python environment with project dependencies

## Step-by-Step Deployment

### 1. SSH into your ETL VM

```bash
ssh azureuser@fund-lens-production-etl-vm
cd /opt/fund-lens-etl
```

### 2. Pull latest code

```bash
git pull origin main
```

### 3. Download bulk data files

**Option A: Essential files only** (committees, candidates, contributions - ~2 GB)
```bash
poetry run python scripts/download_bulk_data.py \
    --cycle 2026 \
    --output-dir data/2025-2026
```

**Option B: All available files** (includes PAC data, expenditures - ~4 GB)
```bash
poetry run python scripts/download_bulk_data.py \
    --cycle 2026 \
    --output-dir data/2025-2026 \
    --all
```

**Option C: Specific files only**
```bash
# Just committees and candidates (fast, <1 minute download)
poetry run python scripts/download_bulk_data.py \
    --cycle 2026 \
    --output-dir data/2025-2026 \
    --files cm cn

# Add individual contributions later (large file, ~30 min download)
poetry run python scripts/download_bulk_data.py \
    --cycle 2026 \
    --output-dir data/2025-2026 \
    --files indiv
```

**Download Progress:**
- Committee file (cm): ~2 MB, <1 min
- Candidate file (cn): ~600 KB, <1 min
- Individual contributions (indiv): ~700 MB - 2 GB, 10-30 min
- Total download time: ~15-45 minutes depending on files selected

### 4. Verify downloads

```bash
ls -lh data/2025-2026/
```

Expected output:
```
data/2025-2026/
├── cm.txt                    (~2 MB)
├── cm_header_file.csv        (<1 KB)
├── cn.txt                    (~600 KB)
├── cn_header_file.csv        (<1 KB)
├── indiv_header_file.csv     (<1 KB)
└── indiv26/
    ├── itcont.txt            (~1.6 GB)
    └── by_date/
        └── (date-partitioned files)
```

### 5. Run bulk ingestion

**Full ingestion** (all data types - 1-3 hours):
```bash
poetry run python scripts/run_bulk_ingestion.py \
    --data-dir data/2025-2026 \
    --cycle 2026
```

**Staged ingestion** (recommended for large datasets):

```bash
# Step 1: Load committees and candidates first (fast, <1 minute)
poetry run python scripts/run_bulk_ingestion.py \
    --data-dir data/2025-2026 \
    --cycle 2026 \
    --no-contributions

# Step 2: Load contributions separately (1-3 hours)
poetry run python scripts/run_bulk_ingestion.py \
    --data-dir data/2025-2026 \
    --cycle 2026 \
    --only-contributions
```

**Monitor progress:**
```bash
# The script will output a log file path like:
# Logging to: /tmp/bulk_ingestion_20251121_143022.log

# Stream the log in another terminal:
tail -f /tmp/bulk_ingestion_<timestamp>.log
```

### 6. Verify data loaded

```bash
# Connect to database
psql postgresql://root:root@127.0.0.1:5432/fund_lens

# Check record counts
SELECT
    'Committees' as table_name,
    COUNT(*) as records,
    COUNT(DISTINCT source_system) as sources
FROM bronze_fec_committee
UNION ALL
SELECT
    'Candidates',
    COUNT(*),
    COUNT(DISTINCT source_system)
FROM bronze_fec_candidate
UNION ALL
SELECT
    'Contributions',
    COUNT(*),
    COUNT(DISTINCT source_system)
FROM bronze_fec_schedule_a;
```

Expected results:
```
   table_name   |  records  | sources
----------------+-----------+---------
 Committees     |    17,387 |       2
 Candidates     |     6,576 |       2
 Contributions  | 9,218,832 |       2
```

Note: `sources = 2` means you have both `FEC` (API) and `FEC_BULK` data.

## Ingestion Performance

### Expected Timings

| Task | Records | Time | Notes |
|------|---------|------|-------|
| Download essential files | - | 15-45 min | Depends on connection speed |
| Load committees | 17K | <30 sec | Small dataset |
| Load candidates | 6K | <30 sec | Small dataset |
| Load contributions | 9.2M | 1-3 hours | Large dataset, chunked processing |

### Memory Usage

The contribution ingestion processes 100,000 records at a time by default:
- **Memory**: ~500 MB peak per chunk
- **Total runtime**: 1-3 hours for 9.2M records

To reduce memory usage (slower but safer):
```bash
poetry run python scripts/run_bulk_ingestion.py \
    --data-dir data/2025-2026 \
    --cycle 2026 \
    --chunksize 50000  # Smaller chunks
```

## Troubleshooting

### Download Failures

If download fails:
```bash
# Check disk space
df -h /opt/fund-lens-etl/data

# Retry specific file
poetry run python scripts/download_bulk_data.py \
    --cycle 2026 \
    --output-dir data/2025-2026 \
    --files indiv  # Retry just the failed file
```

### Database Connection Issues

Update your `.env` file on the VM:
```bash
# Edit .env
nano /opt/fund-lens-etl/.env

# Ensure DATABASE_URL is correct:
DATABASE_URL=postgresql://root:root@127.0.0.1:5432/fund_lens
```

### Memory Issues During Ingestion

If you see `MemoryError` or the process gets killed:
```bash
# Use smaller chunk size
poetry run python scripts/run_bulk_ingestion.py \
    --data-dir data/2025-2026 \
    --cycle 2026 \
    --only-contributions \
    --chunksize 25000  # Very small chunks for low-memory systems
```

### Monitor System Resources

```bash
# Check memory usage during ingestion
watch -n 5 free -h

# Check disk I/O
iostat -x 5

# Check Python process
htop
# (Press F4 to filter, type "python", Enter)
```

## Cleanup

### Remove downloaded files after ingestion

Once data is successfully loaded to the database:
```bash
# Remove just the data files (keep headers for future use)
rm data/2025-2026/cm.txt
rm data/2025-2026/cn.txt
rm -rf data/2025-2026/indiv26/

# Or remove entire directory
rm -rf data/2025-2026/
```

### Re-download later for monthly reconciliation

```bash
# Download fresh data monthly to catch amendments
poetry run python scripts/download_bulk_data.py \
    --cycle 2026 \
    --output-dir data/2025-2026

# Re-run ingestion (UPSERT will update existing records)
poetry run python scripts/run_bulk_ingestion.py \
    --data-dir data/2025-2026 \
    --cycle 2026
```

## Integration with Existing Workflows

### Hybrid Approach (Recommended)

1. **Initial backfill**: Use bulk files (what we just built)
   ```bash
   # One-time historical data load
   poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026
   poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026
   ```

2. **Daily incremental**: Continue using API (existing flows)
   ```bash
   # Your existing daily flow (via Prefect deployments)
   # Runs automatically via schedule
   ```

3. **Monthly reconciliation**: Re-run bulk files
   ```bash
   # Monthly cron job or manual run
   poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026
   poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026
   ```

### Advantages

- ✅ No more API timeout failures for historical data
- ✅ Faster initial load (hours vs days/weeks)
- ✅ Still get real-time updates via API
- ✅ Monthly reconciliation catches amendments
- ✅ Both sources write to same tables (UPSERT on `sub_id`)

## Monitoring via Prefect UI

The bulk ingestion flow appears in Prefect UI just like API flows:

```
http://<your-vm-ip>:4200/runs
```

You can monitor:
- Task progress (committees → candidates → contributions)
- Chunk processing progress
- Error logs and retries
- Total runtime

## Next Steps After Successful Load

1. **Verify data quality**
   ```bash
   # Check for null sub_ids (should be 0)
   psql -c "SELECT COUNT(*) FROM bronze_fec_schedule_a WHERE sub_id IS NULL;" fund_lens

   # Check date range
   psql -c "SELECT MIN(contribution_receipt_date), MAX(contribution_receipt_date) FROM bronze_fec_schedule_a;" fund_lens
   ```

2. **Run silver transformations**
   ```bash
   # Transform bronze → silver (if you have this flow)
   poetry run python scripts/run_silver_transformation.py --cycle 2026
   ```

3. **Run gold aggregations**
   ```bash
   # Create gold analytics tables (if you have this flow)
   poetry run python scripts/run_gold_aggregation.py --cycle 2026
   ```

## File Locations

| Purpose | Location |
|---------|----------|
| Download script | `/opt/fund-lens-etl/scripts/download_bulk_data.py` |
| Ingestion script | `/opt/fund-lens-etl/scripts/run_bulk_ingestion.py` |
| Test script | `/opt/fund-lens-etl/scripts/test_bulk_ingestion.py` |
| Bulk extractors | `/opt/fund-lens-etl/fund_lens_etl/extractors/bulk/` |
| Bulk flow | `/opt/fund-lens-etl/fund_lens_etl/flows/bulk_ingestion_flow.py` |
| Download logs | `/tmp/download_bulk_data_*.log` |
| Ingestion logs | `/tmp/bulk_ingestion_*.log` |
| Data directory | `/opt/fund-lens-etl/data/2025-2026/` |

## Quick Reference Commands

```bash
# Download essential files
poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026

# Test extraction (validates files are readable)
poetry run python scripts/test_bulk_ingestion.py

# Load all data
poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026

# Monitor progress
tail -f /tmp/bulk_ingestion_*.log

# Check database
psql fund_lens -c "SELECT COUNT(*) FROM bronze_fec_schedule_a;"

# Cleanup after successful load
rm -rf data/2025-2026/
```
