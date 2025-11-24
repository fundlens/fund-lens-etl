# Monthly Bulk Reconciliation

## Overview

The monthly bulk reconciliation flow downloads fresh FEC bulk data and reconciles it with existing database records. This catches:

- **Late amendments** to previous filings
- **Corrections** submitted after initial filing
- **Any data missed** in daily incremental runs
- **Data quality issues** that need correction

## Architecture

### Hybrid Update Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA INGESTION STRATEGY                   │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Daily Incremental (API)          Monthly Reconciliation     │
│  ├─ Fast (7-day lookback)         ├─ Comprehensive          │
│  ├─ New contributions only        ├─ Full bulk file         │
│  ├─ ~minutes per run              ├─ Catches amendments     │
│  └─ Runs: Every day at 1 AM       └─ Runs: 1st of month     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Benefits

1. **Fast Daily Updates**: 7-day lookback keeps daily runs fast
2. **Comprehensive Monthly Sync**: Bulk file catches everything missed
3. **Amendment Detection**: Late filings and corrections are reconciled
4. **Data Quality**: UPSERT logic ensures latest data always wins

## Flow Details

### Monthly Reconciliation Flow

**File**: `fund_lens_etl/flows/monthly_bulk_reconciliation_flow.py`

**Steps**:
1. **Download** - Downloads fresh bulk files from FEC
2. **Ingest** - Loads data using bulk ingestion flow (UPSERT logic)
3. **Update States** - Updates extraction states for all committees
4. **Cleanup** - Removes downloaded files to save disk space

**Schedule**: 2 AM on the 1st of every month (Eastern Time)

**Runtime**: ~2-3 hours (mostly download + ingestion time)

### What Gets Updated

- **Committees**: Latest committee information
- **Candidates**: Latest candidate information
- **Contributions**: All individual contributions (UPSERT on `sub_id`)
- **Extraction States**: Latest contribution dates for all committees

### UPSERT Logic

The bulk ingestion uses efficient UPSERT:

```python
# Check which sub_id values already exist
existing_sub_ids = query(sub_id).where(sub_id.in_(chunk_sub_ids))

# Only insert new records (skip existing)
new_records = chunk[~chunk.sub_id.isin(existing_sub_ids)]
load(new_records)  # Fast INSERT, no conflict checking
```

**Result**: Only inserts truly new/changed records, making reconciliation fast even with millions of existing records.

## Deployment

### Deploy to Prefect

On your ETL VM:

```bash
cd /opt/fund-lens-etl

# Deploy the monthly reconciliation flow
poetry run python scripts/deploy_monthly_reconciliation.py
```

This creates a scheduled deployment that runs automatically on the 1st of each month.

### Verify Deployment

```bash
# List deployments
poetry run prefect deployment ls

# Check the schedule
poetry run prefect deployment inspect monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026
```

### Manual Run (Testing)

```bash
# Run immediately (for testing)
poetry run prefect deployment run monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026
```

## Monitoring

### Check Flow Status

**Prefect UI**: `http://<vm-ip>:4200/deployments`

### Log Files

The flow produces detailed logs:
- Download logs: `/tmp/download_bulk_data_*.log`
- Ingestion progress tracked in Prefect UI

### Expected Results

After a successful monthly reconciliation:

```
MONTHLY RECONCILIATION COMPLETE
================================================================================
Committees updated: 17,389
Candidates updated: 6,576
Contributions processed: 2,847 new, 9,215,985 skipped
Extraction states updated: 7,000
================================================================================
```

**Interpretation**:
- Most contributions are skipped (already exist)
- New records are from amendments/late filings
- Extraction states updated to latest dates

## Troubleshooting

### Download Fails

If bulk file download fails:

```bash
# Check disk space
df -h /opt/fund-lens-etl/data

# Retry download manually
poetry run python scripts/download_bulk_data.py \
  --cycle 2026 \
  --output-dir /opt/fund-lens-etl/data/2025-2026
```

### Ingestion Fails

If ingestion fails midway:
- The flow has retries configured
- Partial progress is saved (UPSERT is idempotent)
- Safe to re-run - won't duplicate data

### Out of Disk Space

If disk space is low:

```bash
# Clean up old data directories
rm -rf /opt/fund-lens-etl/data/2025-2026/

# The cleanup task should do this automatically
# Check if cleanup_after=True in deployment
```

### Long Runtime

Monthly reconciliation takes time due to:
- Download: ~15-30 min (depends on FEC server speed)
- Ingestion: ~1-2 hours (processing 9M+ records)
- State updates: ~3-5 min (updating 7,000 committees)

**This is normal and expected.**

## Configuration

### Adjust Schedule

To change when the flow runs:

```python
# In scripts/deploy_monthly_reconciliation.py
schedule=CronSchedule(
    cron="0 2 1 * *",  # Current: 2 AM on 1st of month
    timezone="America/New_York",
)

# Options:
# "0 2 1 * *"     - 2 AM on 1st of month
# "0 3 15 * *"    - 3 AM on 15th of month
# "0 1 * * 0"     - 1 AM every Sunday (weekly)
```

Then re-deploy:

```bash
poetry run python scripts/deploy_monthly_reconciliation.py
```

### Adjust Election Cycle

To reconcile different cycles:

```python
# In scripts/deploy_monthly_reconciliation.py
parameters={
    "election_cycle": 2028,  # Change cycle
    "cleanup_after": True,
}
```

### Keep Downloaded Files

To preserve bulk files after ingestion (for debugging):

```python
parameters={
    "election_cycle": 2026,
    "cleanup_after": False,  # Don't delete files
}
```

## Integration with Daily Incremental

### How They Work Together

**Daily Incremental (API)**:
- Runs every day at 1 AM
- Uses 7-day lookback from last extraction
- Fast, only gets recent data
- Updates extraction states after each run

**Monthly Reconciliation (Bulk)**:
- Runs 1st of month at 2 AM (after daily)
- Processes complete bulk file
- Catches amendments from any time period
- Updates extraction states for all committees

### Why Both?

1. **Daily**: Fast updates, low latency
2. **Monthly**: Comprehensive, catches everything
3. **Together**: Best of both worlds

### Lookback Configuration

With monthly reconciliation, we can use a short lookback:

**File**: `fund_lens_etl/config.py`
```python
lookback_days: int = 7  # Short lookback with monthly reconciliation
```

**Without monthly reconciliation**:
```python
lookback_days: int = 180  # Long lookback needed to catch amendments
```

## Related Files

- **Flow**: `fund_lens_etl/flows/monthly_bulk_reconciliation_flow.py`
- **Deployment**: `scripts/deploy_monthly_reconciliation.py`
- **Download Script**: `scripts/download_bulk_data.py`
- **Bulk Ingestion**: `fund_lens_etl/flows/bulk_ingestion_flow.py`
- **Config**: `fund_lens_etl/config.py` (lookback_days setting)

## Best Practices

1. **Monitor First Run**: Watch the first monthly run closely to ensure it completes
2. **Check Disk Space**: Ensure at least 5 GB free before reconciliation
3. **Review Logs**: Check `/tmp/download_bulk_data_*.log` for download issues
4. **Verify States**: After reconciliation, verify extraction states are updated
5. **Daily Still Runs**: Ensure daily incremental continues running between monthly runs

## FAQ

**Q: Will monthly reconciliation duplicate data?**
A: No, the UPSERT logic ensures no duplicates. Existing records are skipped.

**Q: How long does it take?**
A: ~2-3 hours total (30 min download + 1-2 hours ingestion)

**Q: Can I run it more frequently?**
A: Yes, but it's heavy. Weekly is the practical minimum. Monthly is recommended.

**Q: What if it fails?**
A: Safe to re-run. UPSERT is idempotent - won't create duplicates.

**Q: Does it impact daily runs?**
A: No, they run independently. Daily runs continue as normal.

**Q: Can I disable cleanup?**
A: Yes, set `cleanup_after=False` in deployment parameters.
