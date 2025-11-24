# Triggered Pipeline Architecture

## Overview

The FundLens ETL pipeline uses **event-driven triggers** to orchestrate the Bronze → Silver → Gold transformation flow. This ensures each layer completes successfully before the next begins.

## Pipeline Flow

```
Monday-Friday @ 1:00 AM ET
         ↓
    [SCHEDULED]
  Bronze Ingestion
  (7-day lookback)
    ~2-4 hours
         ↓
    [TRIGGERED]
Silver Transformation
    ~30-60 minutes
         ↓
    [TRIGGERED]
 Gold Transformation
    ~30-60 minutes
```

## Deployment Configuration

### Bronze Ingestion (Scheduled)

- **Schedule**: Monday-Friday at 1:00 AM Eastern Time
- **Trigger Type**: Cron schedule
- **Lookback**: 7 days
- **States**: All 51 (50 states + DC)
- **Runtime**: ~2-4 hours

```python
# Deployment: bronze-ingestion-all-states-daily
schedules=[
    CronSchedule(cron="0 1 * * 1-5", timezone="America/New_York")
]
```

### Silver Transformation (Triggered)

- **Trigger Type**: DeploymentCompletedTrigger
- **Triggers On**: bronze-ingestion-all-states-daily completion
- **Runtime**: ~30-60 minutes

```python
# Deployment: silver-transformation-all-states-triggered
triggers=[
    DeploymentCompletedTrigger(
        expect=["bronze-ingestion-all-states-daily"]
    )
]
```

### Gold Transformation (Triggered)

- **Trigger Type**: DeploymentCompletedTrigger
- **Triggers On**: silver-transformation-all-states-triggered completion
- **Runtime**: ~30-60 minutes

```python
# Deployment: gold-transformation-all-states-triggered
triggers=[
    DeploymentCompletedTrigger(
        expect=["silver-transformation-all-states-triggered"]
    )
]
```

## Benefits of Triggered Architecture

### 1. **Guaranteed Ordering**
- Silver won't start until Bronze completes successfully
- Gold won't start until Silver completes successfully
- No race conditions or partial data processing

### 2. **Dynamic Timing**
- If Bronze takes longer (e.g., more data to process), Silver waits
- No fixed time gaps that might be too short or wastefully long
- Pipeline adapts to actual processing time

### 3. **Failure Isolation**
- If Bronze fails, Silver/Gold don't run on bad data
- Each layer can be retried independently
- Clear visibility into where failures occur

### 4. **Resource Efficiency**
- Silver starts immediately when Bronze completes (no waiting)
- Gold starts immediately when Silver completes (no waiting)
- No unnecessary idle time between stages

## Deployment

### Deploy All Flows

```bash
cd /opt/fund-lens-etl
git pull origin main

# Deploy bronze, silver, and gold with triggers
poetry run python scripts/deploy.py --all
```

### Verify Deployments

```bash
# List all deployments
poetry run prefect deployment ls

# Expected deployments:
# - bronze-ingestion-all-states-daily (scheduled M-F 1 AM)
# - silver-transformation-all-states-triggered (triggered)
# - gold-transformation-all-states-triggered (triggered)
# - monthly-bulk-reconciliation-2026 (scheduled 1st of month 2 AM)
```

### View Deployment Details

```bash
# Check bronze schedule
poetry run prefect deployment inspect \
  bronze-all-states-flow/bronze-ingestion-all-states-daily

# Check silver trigger
poetry run prefect deployment inspect \
  silver-transformation-flow/silver-transformation-all-states-triggered

# Check gold trigger
poetry run prefect deployment inspect \
  gold-transformation-flow/gold-transformation-all-states-triggered
```

## Monitoring

### Prefect UI

Navigate to: `http://<vm-ip>:4200`

**Flow Runs Tab**: Shows all flow runs with their triggers
- Bronze runs show as "Scheduled"
- Silver runs show as "Triggered by: bronze-ingestion-all-states-daily"
- Gold runs show as "Triggered by: silver-transformation-all-states-triggered"

**Deployments Tab**: Shows trigger configuration for each deployment

### Trigger Chain Visualization

In Prefect UI, you can see the trigger relationships:
```
bronze-ingestion-all-states-daily [Scheduled: M-F 1 AM]
    └─▶ silver-transformation-all-states-triggered [Triggered]
            └─▶ gold-transformation-all-states-triggered [Triggered]
```

### Check Trigger Status

```bash
# View recent flow runs with triggers
poetry run prefect flow-run ls --limit 10

# View specific flow run details
poetry run prefect flow-run inspect <flow-run-id>
```

## Testing

### Manual Trigger Test

You can manually trigger the bronze flow to test the entire chain:

```bash
# Manually start bronze (will trigger silver → gold)
poetry run prefect deployment run \
  bronze-all-states-flow/bronze-ingestion-all-states-daily

# Watch the cascade in Prefect UI
# Bronze completes → Silver starts → Silver completes → Gold starts
```

### Test Individual Flows

You can also test each flow independently:

```bash
# Test silver alone (won't trigger gold unless it's a proper deployment run)
poetry run prefect deployment run \
  silver-transformation-flow/silver-transformation-all-states-triggered

# Test gold alone
poetry run prefect deployment run \
  gold-transformation-flow/gold-transformation-all-states-triggered
```

## Troubleshooting

### Silver/Gold Not Triggering

**Problem**: Bronze completes but Silver doesn't start

**Solutions**:
1. **Check trigger configuration**:
   ```bash
   poetry run prefect deployment inspect \
     silver-transformation-flow/silver-transformation-all-states-triggered
   ```

2. **Verify deployment names match**:
   - Silver trigger expects: `bronze-ingestion-all-states-daily`
   - Gold trigger expects: `silver-transformation-all-states-triggered`

3. **Check Prefect events**:
   - Prefect UI → Events tab
   - Look for "deployment.completed" events

4. **Ensure worker is running**:
   ```bash
   # Check worker status
   poetry run prefect worker ls

   # Restart worker if needed
   systemctl restart prefect-worker
   ```

### Trigger Fires But Flow Fails

**Problem**: Silver triggers but fails immediately

**Solutions**:
1. **Check flow logs**:
   ```bash
   poetry run prefect flow-run logs <flow-run-id>
   ```

2. **Verify parameters**:
   - Silver/Gold should inherit cycle parameter (2026)
   - State parameter should be None (all states)

3. **Check database connectivity**:
   - Silver/Gold need database access
   - Verify DATABASE_URL in .env

### Wrong Flow Version Running

**Problem**: Old version of flow runs despite redeployment

**Solutions**:
1. **Re-deploy with version bump**:
   - Versions are now at 3.0.0 for all flows
   - Redeploy: `poetry run python scripts/deploy.py --all`

2. **Clear old deployments** (if needed):
   ```bash
   # List all deployments
   poetry run prefect deployment ls

   # Delete old version (if needed)
   poetry run prefect deployment delete <deployment-name>
   ```

## Weekly Schedule Example

**Monday @ 1:00 AM**:
- Bronze starts (incremental extraction)
- Bronze completes @ ~4:00 AM
- Silver starts @ ~4:00 AM
- Silver completes @ ~4:45 AM
- Gold starts @ ~4:45 AM
- Gold completes @ ~5:30 AM
- ✅ Complete pipeline: Bronze → Silver → Gold

**Tuesday-Friday**: Same pattern

**Saturday-Sunday**: No runs (weekday schedule only)

**1st of Month @ 2:00 AM**: Monthly bulk reconciliation

## Migration from Scheduled to Triggered

### Old Architecture (Time-Based)
```
1:00 AM - Bronze starts
3:00 AM - Bronze might still be running...
6:00 AM - Silver starts (might be too early!)
8:00 AM - Gold starts (might be too early!)
```

**Problems**:
- Fixed time gaps waste time if Bronze finishes early
- Silver/Gold might start before Bronze completes
- Hard to coordinate timing across different data volumes

### New Architecture (Event-Based)
```
1:00 AM - Bronze starts
[Dynamic] - Bronze completes
[Immediate] - Silver starts
[Dynamic] - Silver completes
[Immediate] - Gold starts
```

**Benefits**:
- No wasted time waiting
- Guaranteed completion order
- Adapts to actual processing time

## Related Files

- **Schedules**: `fund_lens_etl/deployments/schedules.py`
- **Bronze Flow**: `fund_lens_etl/flows/bronze_all_states_flow.py`
- **Silver Flow**: `fund_lens_etl/flows/silver_transformation_flow.py`
- **Gold Flow**: `fund_lens_etl/flows/gold_transformation_flow.py`
- **Deployment Script**: `scripts/deploy.py`

## Best Practices

1. **Always redeploy after code changes**:
   ```bash
   poetry run python scripts/deploy.py --all
   ```

2. **Monitor first triggered run**: Watch Prefect UI to ensure triggers fire

3. **Check logs for each stage**: Verify each layer processes correctly

4. **Test trigger chain manually**: Run bronze manually to test full pipeline

5. **Keep worker running**: Ensure Prefect worker is always running
   ```bash
   systemctl status prefect-worker
   ```

## FAQ

**Q: What happens if Bronze fails?**
A: Silver won't trigger. Only successful completions trigger downstream flows.

**Q: Can I still run Silver/Gold manually?**
A: Yes, manual runs work independently of triggers.

**Q: Do triggers work across worker restarts?**
A: Yes, triggers are managed by Prefect server, not the worker.

**Q: Can I have both scheduled and triggered versions?**
A: Technically yes, but not recommended. Use one or the other for each flow.

**Q: How do I disable triggers?**
A: Redeploy with schedules instead of triggers, or pause the deployment.

**Q: What if I need to run Silver without Bronze?**
A: Manually trigger Silver deployment - triggers only apply to automatic runs.
