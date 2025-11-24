# Deployment Checklist

Complete checklist for deploying the updated FundLens ETL pipeline with event-driven triggers.

## Pre-Deployment

### 1. Code Review
- [ ] All changes committed to git
- [ ] Changes pushed to `main` branch
- [ ] No uncommitted local changes

### 2. Environment Check
```bash
# Verify you're on the VM
hostname  # Should show fund-lens-production-etl-vm

# Check Python environment
poetry --version

# Check Prefect worker status
systemctl status prefect-worker
```

### 3. Backup Current State
```bash
# List current deployments
poetry run prefect deployment ls > /tmp/deployments_backup_$(date +%Y%m%d).txt

# Note current schedules
poetry run python scripts/deploy.py --summary > /tmp/schedule_backup_$(date +%Y%m%d).txt
```

## Deployment Steps

### 1. Pull Latest Code
```bash
cd /opt/fund-lens-etl
git pull origin main
```

Expected output: Updated files related to:
- `fund_lens_etl/deployments/schedules.py`
- `fund_lens_etl/flows/monthly_bulk_reconciliation_flow.py`
- `fund_lens_etl/config.py` (lookback_days = 7)
- `scripts/update_extraction_states_from_db.py`
- Various docs/ files

### 2. Update Extraction States
```bash
# This ensures all committees have current states for 7-day lookback
poetry run python scripts/update_extraction_states_from_db.py --cycle 2026
```

Expected: Updates ~7,000 committee extraction states
Runtime: ~3-5 minutes

### 3. Deploy All Flows
```bash
# Deploy with new trigger configuration
poetry run python scripts/deploy.py --all
```

Expected output:
```
================================================================================
Deploying All Flows
================================================================================

✓ Successfully deployed 4 All Flows

✅ All deployments completed successfully!
```

### 4. Verify Deployments
```bash
# List deployments
poetry run prefect deployment ls

# Should show exactly 4 deployments:
# 1. bronze-all-states-flow/bronze-ingestion-all-states-daily
# 2. monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026
# 3. silver-transformation-flow/silver-transformation-all-states-triggered
# 4. gold-transformation-flow/gold-transformation-all-states-triggered
```

### 5. Verify Bronze Schedule
```bash
poetry run prefect deployment inspect \
  bronze-all-states-flow/bronze-ingestion-all-states-daily \
  | grep -A 3 "cron"
```

Expected: `cron: 0 1 * * 1-5` (1 AM Monday-Friday)

### 6. Verify Monthly Bulk Schedule
```bash
poetry run prefect deployment inspect \
  monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026 \
  | grep -A 3 "cron"
```

Expected: `cron: 0 1 1-7 * 6` (1 AM on first Saturday)

### 7. Verify Silver Triggers
```bash
poetry run prefect deployment inspect \
  silver-transformation-flow/silver-transformation-all-states-triggered \
  | grep -A 15 "triggers"
```

Expected: TWO triggers:
- bronze-ingestion-all-states-daily
- monthly-bulk-reconciliation-2026

### 8. Verify Gold Trigger
```bash
poetry run prefect deployment inspect \
  gold-transformation-flow/gold-transformation-all-states-triggered \
  | grep -A 10 "triggers"
```

Expected: ONE trigger:
- silver-transformation-all-states-triggered

## Post-Deployment Verification

### 1. Check Prefect UI

Navigate to: `http://<vm-ip>:4200/deployments`

**Expected**:
- 4 deployments listed
- Bronze and Monthly Bulk show schedule icons
- Silver and Gold show trigger icons

### 2. View Schedule Summary
```bash
poetry run python scripts/deploy.py --summary
```

Expected output:
```
================================================================================
FundLens ETL Pipeline - Deployment Schedules
================================================================================

WEEKDAY PIPELINE (Monday-Friday) - ALL 51 STATES:
  1:00 AM ET - Bronze: Incremental ingestion (7-day lookback)
               Runtime: ~2-4 hours
               ↓
  [TRIGGERED] Silver: Transform bronze → silver
               Runtime: ~30-60 minutes
               ↓
  [TRIGGERED] Gold: Transform silver → gold
               Runtime: ~30-60 minutes

MONTHLY RECONCILIATION (First Saturday) - ALL 51 STATES:
  1:00 AM ET - Bulk: Download + reconcile (full data)
               Runtime: ~2-3 hours
               ↓
  [TRIGGERED] Silver: Transform bronze → silver
               Runtime: ~30-60 minutes
               ↓
  [TRIGGERED] Gold: Transform silver → gold
               Runtime: ~30-60 minutes

CONFIGURATION:
  States: All 51 (50 states + DC)
  Cycle: 2026
  Timezone: America/New_York
  Total Deployments: 4

TRIGGER CHAINS:
  Daily:   Bronze → Silver → Gold (M-F)
  Monthly: Bulk Reconciliation → Silver → Gold (1st Saturday)
  Silver accepts triggers from BOTH Bronze and Bulk flows
================================================================================
```

### 3. Test Trigger Chain (Optional)

```bash
# Manually trigger bronze to test full chain
poetry run prefect deployment run \
  bronze-all-states-flow/bronze-ingestion-all-states-daily

# Watch in Prefect UI:
# 1. Bronze runs and completes
# 2. Silver automatically starts (triggered)
# 3. Silver completes
# 4. Gold automatically starts (triggered)
# 5. Gold completes
```

**Expected Behavior**:
- Bronze completes with state "Completed"
- Silver starts within ~30 seconds of Bronze completion
- Gold starts within ~30 seconds of Silver completion

## Cleanup (Optional)

### Delete Old Deployments

If old scheduled versions of Silver/Gold exist:

```bash
# List all deployments
poetry run prefect deployment ls | grep transformation

# Delete old scheduled versions (if they exist)
poetry run prefect deployment delete \
  silver-transformation-flow/silver-transformation-all-states-daily

poetry run prefect deployment delete \
  gold-transformation-flow/gold-transformation-all-states-daily
```

## Monitoring First Run

### Next Scheduled Run

**Bronze/Daily**: Next Monday at 1:00 AM ET
**Monthly Bulk**: Next first Saturday at 1:00 AM ET

### Watch First Run

```bash
# Monitor flow runs
watch -n 10 'poetry run prefect flow-run ls --limit 5'

# Or use Prefect UI
# http://<vm-ip>:4200/flow-runs
```

### Expected Timeline (Monday @ 1 AM)

```
1:00 AM - Bronze starts
3:30 AM - Bronze completes, Silver automatically starts
4:15 AM - Silver completes, Gold automatically starts
5:00 AM - Gold completes
✅ Complete pipeline finished
```

## Rollback Procedure (If Needed)

If issues occur and you need to rollback:

```bash
cd /opt/fund-lens-etl

# Find the commit before trigger changes
git log --oneline | head -10

# Checkout previous version
git checkout <commit-hash>

# Redeploy old version
poetry run python scripts/deploy.py --all

# Return to latest when ready
git checkout main
```

## Success Criteria

- [ ] All 4 deployments exist and are active
- [ ] Bronze scheduled for M-F 1 AM
- [ ] Monthly bulk scheduled for 1st Saturday 1 AM
- [ ] Silver has TWO triggers configured
- [ ] Gold has ONE trigger configured
- [ ] Extraction states updated for all ~7,000 committees
- [ ] Prefect worker is running
- [ ] Schedule summary shows correct configuration
- [ ] (Optional) Manual test of trigger chain successful

## Troubleshooting

### Triggers Not Firing

**Issue**: Bronze completes but Silver doesn't start

**Solutions**:
1. Check worker is running: `systemctl status prefect-worker`
2. Check events in Prefect UI → Events tab
3. Verify trigger configuration: See verification steps above
4. Restart worker: `sudo systemctl restart prefect-worker`

### Wrong Version Running

**Issue**: Old flow version executes

**Solution**:
1. Delete old deployments: See cleanup section above
2. Redeploy: `poetry run python scripts/deploy.py --all`
3. Verify version 3.0.0 in deployment details

### Extraction States Not Updated

**Issue**: Bronze still using 180-day lookback

**Solution**:
1. Verify config.py has `lookback_days: int = 7`
2. Re-run state update: `poetry run python scripts/update_extraction_states_from_db.py --cycle 2026`
3. Check extraction state in database for sample committee

## Support Documentation

- **Architecture**: `docs/FINAL_PIPELINE_ARCHITECTURE.md`
- **Triggers**: `docs/TRIGGERED_PIPELINE.md`
- **Deployment**: `docs/REDEPLOY_GUIDE.md`
- **Monthly Bulk**: `docs/MONTHLY_RECONCILIATION.md`
- **State Management**: `docs/EXTRACTION_STATE_FIX.md`

## Contact

For issues or questions, check:
1. Prefect UI logs: `http://<vm-ip>:4200`
2. Worker logs: `journalctl -u prefect-worker -f`
3. Application logs in flow run details

---

**Deployment Date**: _______________

**Deployed By**: _______________

**Verification Status**: ⬜ Complete
