# Final Pipeline Architecture

## Complete ETL Pipeline Configuration

### Overview

The FundLens ETL pipeline uses a **hybrid event-driven architecture** with two distinct update paths:

1. **Daily Incremental** (M-F): Fast, 1-day lookback API extraction
2. **Monthly Reconciliation** (1st Saturday): Comprehensive bulk file reconciliation

Both paths trigger the same Silver → Gold transformation chain, ensuring all data flows through the complete pipeline.

## Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     WEEKDAY PIPELINE                         │
│                    (Monday - Friday)                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1:00 AM ET - Bronze Ingestion (API)                        │
│               ├─ 1-day lookback                              │
│               ├─ Incremental extraction                      │
│               └─ ~30-60 minutes                              │
│                    ↓ [TRIGGER]                               │
│  ~3-5 AM ET - Silver Transformation                          │
│               ├─ Bronze → Silver                             │
│               └─ ~30-60 minutes                              │
│                    ↓ [TRIGGER]                               │
│  ~4-6 AM ET - Gold Transformation                            │
│               ├─ Silver → Gold                               │
│               └─ ~30-60 minutes                              │
│                                                               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  MONTHLY RECONCILIATION                       │
│                  (First Saturday)                             │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1:00 AM ET - Bulk Data Reconciliation                       │
│               ├─ Download fresh bulk files                   │
│               ├─ UPSERT all data                            │
│               ├─ Update extraction states                    │
│               └─ ~2-3 hours                                  │
│                    ↓ [TRIGGER]                               │
│  ~3-4 AM ET - Silver Transformation                          │
│               ├─ Bronze → Silver                             │
│               └─ ~30-60 minutes                              │
│                    ↓ [TRIGGER]                               │
│  ~4-5 AM ET - Gold Transformation                            │
│               ├─ Silver → Gold                               │
│               └─ ~30-60 minutes                              │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Deployment Configuration

### 1. Bronze Ingestion (Scheduled)

**Deployment**: `bronze-ingestion-all-states-daily`
- **Schedule**: Monday-Friday at 1:00 AM ET
- **Trigger Type**: Cron schedule `0 1 * * 1-5`
- **Lookback**: 1 day
- **States**: All 51
- **Runtime**: ~30-60 minutes
- **Version**: 3.0.0

### 2. Monthly Bulk Reconciliation (Scheduled)

**Deployment**: `monthly-bulk-reconciliation-2026`
- **Schedule**: First Saturday of each month at 1:00 AM ET
- **Trigger Type**: Cron schedule `0 1 1-7 * 6`
- **Scope**: Complete bulk file
- **Runtime**: ~2-3 hours
- **Version**: 3.0.0

### 3. Silver Transformation (Triggered)

**Deployment**: `silver-transformation-all-states-triggered`
- **Trigger Type**: DeploymentCompletedTrigger
- **Triggers On**:
  - Bronze daily ingestion completion, OR
  - Monthly bulk reconciliation completion
- **Runtime**: ~30-60 minutes
- **Version**: 3.0.0

### 4. Gold Transformation (Triggered)

**Deployment**: `gold-transformation-all-states-triggered`
- **Trigger Type**: DeploymentCompletedTrigger
- **Triggers On**: Silver transformation completion
- **Runtime**: ~30-60 minutes
- **Version**: 3.0.0

## Trigger Chains

### Daily Chain (M-F)
```
Bronze Ingestion
       ↓
   [Completes]
       ↓
Silver Transformation
       ↓
   [Completes]
       ↓
Gold Transformation
```

### Monthly Chain (1st Saturday)
```
Bulk Reconciliation
       ↓
   [Completes]
       ↓
Silver Transformation (same deployment as daily)
       ↓
   [Completes]
       ↓
Gold Transformation (same deployment as daily)
```

**Key Point**: Silver and Gold are the **same deployments** for both daily and monthly runs. They accept triggers from either Bronze or Bulk Reconciliation.

## Weekly Schedule Example

### Week 1
- **Mon-Fri 1 AM**: Daily incremental → Silver → Gold
- **Sat 1 AM (1st)**: Monthly bulk reconciliation → Silver → Gold
- **Sun**: No runs

### Week 2-4
- **Mon-Fri 1 AM**: Daily incremental → Silver → Gold
- **Sat-Sun**: No runs

### Week 5 (Next Month)
- **Sat 1 AM (1st)**: Monthly bulk reconciliation → Silver → Gold
- (Cycle continues...)

## Benefits of This Architecture

### 1. Unified Transformation Pipeline
- Same Silver/Gold flows for daily and monthly
- Consistent data processing logic
- No duplication of transformation code

### 2. Event-Driven Execution
- Silver starts immediately when Bronze OR Bulk completes
- Gold starts immediately when Silver completes
- No fixed time gaps (efficient resource usage)

### 3. Guaranteed Data Quality
- Silver won't run if Bronze/Bulk fails
- Gold won't run if Silver fails
- Each layer validates before triggering next

### 4. Optimal Scheduling
- Daily runs on weekdays (when most active)
- Monthly runs on Saturday (less load, more time)
- No weekend daily runs (reduces costs/complexity)

### 5. Comprehensive Coverage
- Daily: Fast updates (1-day lookback)
- Monthly: Complete reconciliation (catches all amendments)
- Together: Best of both worlds

## Data Freshness Guarantees

### Daily Incremental (M-F)
- **Freshness**: Within ~24 hours (runs every weekday)
- **Coverage**: Last 1 day of data
- **Use Case**: Regular updates, recent contributions

### Monthly Reconciliation (1st Saturday)
- **Freshness**: Within ~30 days
- **Coverage**: Complete historical data
- **Use Case**: Catch amendments, corrections, late filings

### Combined Result
- New data: Available next business day
- Amendments: Caught within 30 days
- Completeness: Guaranteed by monthly reconciliation

## Monitoring

### Prefect UI

Navigate to: `http://<vm-ip>:4200`

**Expected Deployments** (4 total):
1. `bronze-ingestion-all-states-daily` - Scheduled M-F 1 AM
2. `monthly-bulk-reconciliation-2026` - Scheduled 1st Sat 1 AM
3. `silver-transformation-all-states-triggered` - Triggered by #1 or #2
4. `gold-transformation-all-states-triggered` - Triggered by #3

**Flow Run Visualization**:
- Daily: Bronze → Silver → Gold (chain visible in UI)
- Monthly: Bulk → Silver → Gold (chain visible in UI)

### Verify Trigger Relationships

```bash
# Check Silver triggers
poetry run prefect deployment inspect \
  silver-transformation-flow/silver-transformation-all-states-triggered \
  | grep -A 10 triggers

# Should show TWO triggers:
# 1. bronze-ingestion-all-states-daily
# 2. monthly-bulk-reconciliation-2026
```

## Deployment

### Initial Deployment

```bash
cd /opt/fund-lens-etl
git pull origin main
poetry run python scripts/deploy.py --all
```

### Verify Deployment

```bash
# List deployments
poetry run prefect deployment ls

# View schedule summary
poetry run python scripts/deploy.py --summary
```

Expected output:
```
================================================================================
FundLens ETL Pipeline - Deployment Schedules
================================================================================

WEEKDAY PIPELINE (Monday-Friday) - ALL 51 STATES:
  1:00 AM ET - Bronze: Incremental ingestion (1-day lookback)
               Runtime: ~30-60 minutes
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

## Testing

### Test Daily Chain

```bash
# Manually trigger bronze (will cascade to Silver → Gold)
poetry run prefect deployment run \
  bronze-all-states-flow/bronze-ingestion-all-states-daily

# Watch cascade in Prefect UI
# Bronze completes → Silver starts → Silver completes → Gold starts
```

### Test Monthly Chain

```bash
# Manually trigger monthly bulk
poetry run prefect deployment run \
  monthly-bulk-reconciliation/monthly-bulk-reconciliation-2026

# Watch cascade in Prefect UI
# Bulk completes → Silver starts → Silver completes → Gold starts
```

## Troubleshooting

### Silver Not Triggering After Bronze

1. Check Bronze completed successfully
2. Verify trigger configuration on Silver deployment
3. Check Prefect events tab for "deployment.completed" events
4. Ensure worker is running

### Silver Not Triggering After Monthly Bulk

1. Check Bulk completed successfully
2. Verify Monthly Bulk deployment name is exactly: `monthly-bulk-reconciliation-2026`
3. Check Silver deployment has TWO triggers configured
4. Redeploy if needed: `poetry run python scripts/deploy.py --all`

### Gold Not Triggering After Silver

1. Check Silver completed successfully
2. Verify trigger configuration on Gold deployment
3. Check Silver deployment name matches trigger expectation

## Performance Expectations

### Daily Run (M-F)
- **Start**: 1:00 AM ET
- **Bronze Complete**: ~1:45 AM
- **Silver Complete**: ~2:15 AM
- **Gold Complete**: ~2:45 AM
- **Total Duration**: ~2 hours

### Monthly Run (1st Saturday)
- **Start**: 1:00 AM ET
- **Bulk Complete**: ~3:30 AM
- **Silver Complete**: ~4:15 AM
- **Gold Complete**: ~5:00 AM
- **Total Duration**: ~4 hours

## Related Documentation

- **Triggered Pipeline**: `docs/TRIGGERED_PIPELINE.md` - Detailed trigger architecture
- **Redeploy Guide**: `docs/REDEPLOY_GUIDE.md` - Step-by-step redeployment
- **Monthly Reconciliation**: `docs/MONTHLY_RECONCILIATION.md` - Bulk reconciliation details
- **Extraction State Fix**: `docs/EXTRACTION_STATE_FIX.md` - Lookback configuration context

## Summary

This architecture provides:
- ✅ **Fast daily updates** (1-day lookback)
- ✅ **Comprehensive monthly reconciliation** (full dataset)
- ✅ **Event-driven triggers** (guaranteed ordering)
- ✅ **Unified transformation pipeline** (no duplication)
- ✅ **Efficient scheduling** (weekdays + first Saturday)
- ✅ **Complete data coverage** (incremental + reconciliation)

**Result**: A robust, efficient, and maintainable ETL pipeline that balances speed with completeness.
