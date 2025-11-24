# Redeployment Guide

## Quick Redeploy (After Code Changes)

```bash
# 1. SSH into VM
ssh azureuser@fund-lens-production-etl-vm

# 2. Pull latest code
cd /opt/fund-lens-etl
git pull origin main

# 3. Redeploy all flows
poetry run python scripts/deploy.py --all

# 4. Verify deployments
poetry run prefect deployment ls
```

## What Changed

### Bronze → Silver → Gold Pipeline Now Uses Triggers

**Old Behavior** (Time-Based):
- Bronze: Scheduled M-F 2 AM
- Silver: Scheduled M-F 6 AM (fixed 4-hour gap)
- Gold: Scheduled M-F 8 AM (fixed 2-hour gap)

**New Behavior** (Event-Based):
- Bronze: Scheduled M-F 1 AM
- Silver: **Triggered** when Bronze completes
- Gold: **Triggered** when Silver completes

### Benefits

1. **No wasted time**: Silver starts immediately when Bronze completes
2. **Guaranteed order**: Silver won't start if Bronze fails
3. **Dynamic timing**: Adapts to actual processing time
4. **Earlier completion**: Pipeline finishes earlier with 1 AM start

## Expected Behavior After Redeploy

### Monday @ 1:00 AM ET
```
1:00 AM - Bronze starts (scheduled)
~3:30 AM - Bronze completes
~3:30 AM - Silver starts (triggered)
~4:15 AM - Silver completes
~4:15 AM - Gold starts (triggered)
~5:00 AM - Gold completes
✅ Full pipeline complete by ~5 AM
```

### Prefect UI

**Deployments Tab**: Should show 4 deployments
1. `bronze-ingestion-all-states-daily` - Scheduled M-F 1 AM
2. `silver-transformation-all-states-triggered` - Triggered by Bronze OR Monthly Bulk
3. `gold-transformation-all-states-triggered` - Triggered by Silver
4. `monthly-bulk-reconciliation-2026` - Scheduled 1st Saturday 1 AM

**Flow Runs Tab**: After Monday's run
- Bronze run: Status "Scheduled", started at 1:00 AM
- Silver run: Status "Triggered by: bronze-ingestion...", started when Bronze completed
- Gold run: Status "Triggered by: silver-transformation...", started when Silver completed

## Verification Steps

### 1. Check Deployments Exist

```bash
poetry run prefect deployment ls
```

Expected output (abbreviated):
```
bronze-all-states-flow/bronze-ingestion-all-states-daily
silver-transformation-flow/silver-transformation-all-states-triggered
gold-transformation-flow/gold-transformation-all-states-triggered
monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026
```

### 2. Verify Bronze Schedule

```bash
poetry run prefect deployment inspect \
  bronze-all-states-flow/bronze-ingestion-all-states-daily | grep -A 5 schedule
```

Should show: `cron: 0 1 * * 1-5` (1 AM Monday-Friday)

### 3. Verify Silver Trigger

```bash
poetry run prefect deployment inspect \
  silver-transformation-flow/silver-transformation-all-states-triggered | grep -A 5 triggers
```

Should show trigger on bronze deployment completion

### 4. Verify Gold Trigger

```bash
poetry run prefect deployment inspect \
  gold-transformation-flow/gold-transformation-all-states-triggered | grep -A 5 triggers
```

Should show trigger on silver deployment completion

### 5. Test Trigger Chain (Optional)

```bash
# Manually run bronze to test full chain
poetry run prefect deployment run \
  bronze-all-states-flow/bronze-ingestion-all-states-daily

# Watch in Prefect UI - should see:
# 1. Bronze runs
# 2. Bronze completes → Silver automatically starts
# 3. Silver completes → Gold automatically starts
```

## Troubleshooting

### Issue: Old Deployments Still Listed

**Solution**: Delete old deployments

```bash
# List all to find old ones
poetry run prefect deployment ls

# Delete old scheduled versions (if they exist)
poetry run prefect deployment delete \
  silver-transformation-flow/silver-transformation-all-states-daily

poetry run prefect deployment delete \
  gold-transformation-flow/gold-transformation-all-states-daily
```

Then redeploy:
```bash
poetry run python scripts/deploy.py --all
```

### Issue: Triggers Not Firing

**Check 1**: Verify worker is running
```bash
systemctl status prefect-worker
# Should show "active (running)"

# If not running:
sudo systemctl start prefect-worker
```

**Check 2**: Verify deployment names match
- Silver should trigger on: `bronze-ingestion-all-states-daily`
- Gold should trigger on: `silver-transformation-all-states-triggered`

**Check 3**: Check Prefect events
- Prefect UI → Events tab
- Look for "deployment.completed" events after Bronze/Silver runs

### Issue: Flows Running at Old Times

**Solution**: Old schedules still active

```bash
# Delete all deployments and redeploy fresh
poetry run prefect deployment ls | grep bronze-ingestion
# (Note the exact names)

# Delete old ones
poetry run prefect deployment delete <old-deployment-name>

# Redeploy
poetry run python scripts/deploy.py --all
```

## Rollback (If Needed)

If you need to rollback to time-based scheduling:

```bash
# 1. Checkout previous commit
git log --oneline | head -5  # Find commit before trigger changes
git checkout <commit-hash>

# 2. Redeploy old version
poetry run python scripts/deploy.py --all

# 3. Return to latest
git checkout main
```

## Next Scheduled Run

**Next Bronze Run**: Monday at 1:00 AM ET

After redeploying, the next automatic run will be Monday @ 1:00 AM, which will:
1. Run Bronze ingestion
2. Automatically trigger Silver when Bronze completes
3. Automatically trigger Gold when Silver completes

## Summary

| Change | Before | After |
|--------|--------|-------|
| Bronze Schedule | 2 AM M-F | **1 AM M-F** |
| Monthly Bulk | 2 AM 1st of month | **1 AM 1st Saturday** |
| Silver Trigger | Scheduled 6 AM | **Triggered on Bronze OR Bulk** |
| Gold Trigger | Scheduled 8 AM | **Triggered on Silver** |
| Earliest Completion | ~9 AM | **~5 AM** |
| Pipeline Guarantee | Time-based (might overlap) | **Event-based (guaranteed order)** |

**Result**: Faster pipeline with guaranteed ordering, no wasted idle time, and monthly bulk now triggers full pipeline.
