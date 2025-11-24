# Deployment Quick Start - Monthly Reconciliation

## Deploy the Monthly Reconciliation Flow

### On the ETL VM

```bash
# 1. SSH into VM
ssh azureuser@fund-lens-production-etl-vm

# 2. Navigate to project
cd /opt/fund-lens-etl

# 3. Pull latest code
git pull origin main

# 4. Deploy the flow
poetry run python scripts/deploy_monthly_reconciliation.py
```

**Expected Output**:
```
✓ Deployed monthly reconciliation flow
  Deployment ID: abc-123-def-456
  Schedule: 2 AM on the 1st of every month (Eastern Time)
  Next run: Check Prefect UI for scheduled time

View in Prefect UI:
  http://localhost:4200/deployments
```

## Verify Deployment

```bash
# List all deployments
poetry run prefect deployment ls

# Should show:
# - bronze-ingestion-all-states/... (daily)
# - monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026 (monthly) ← NEW
```

## Test the Flow (Optional)

```bash
# Run manually (doesn't wait for schedule)
poetry run prefect deployment run \
  monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026

# Monitor in Prefect UI
# http://<vm-ip>:4200/flow-runs
```

## What Happens Next

### Automatic Execution

- **1st of each month at 2 AM ET**: Flow runs automatically
- **Duration**: ~2-3 hours
- **Actions**: Downloads bulk files → Ingests data → Updates extraction states → Cleans up

### First Run

The first monthly run will:
- Download ~2 GB of bulk data
- Process ~9M existing contribution records (most will be skipped)
- Insert any new records from amendments/late filings
- Update extraction states for all 7,000 committees

### Ongoing Runs

Subsequent monthly runs will:
- Re-download fresh bulk files
- Skip most existing records (fast)
- Insert only new amendments and corrections
- Keep extraction states current

## Monitoring

### Check Status

**Prefect UI**: Navigate to http://<vm-ip>:4200
- Click "Deployments"
- Find "monthly-bulk-reconciliation-2026"
- View schedule and recent runs

### Check Logs

```bash
# View download logs
ls -lht /tmp/download_bulk_data_*.log | head -5
cat /tmp/download_bulk_data_<timestamp>.log

# View flow logs in Prefect UI
# Or use CLI:
poetry run prefect flow-run logs <flow-run-id>
```

## Next Steps

1. ✅ **Deployed**: Monthly reconciliation is scheduled
2. ✅ **Daily runs**: Continue running as normal
3. ✅ **Lookback reduced**: From 180 days to 1 day (fast daily runs)
4. 🎯 **First monthly run**: Wait for 1st of next month or trigger manually

## Summary

You now have a robust data ingestion strategy:

| Type | Frequency | Speed | Coverage |
|------|-----------|-------|----------|
| **Daily Incremental** | Every day 1 AM | Fast (~30-60 min) | Last 1 day |
| **Monthly Reconciliation** | 1st of month 2 AM | Slow (~2-3 hours) | Complete |

**Result**: Fast daily updates + comprehensive monthly synchronization
