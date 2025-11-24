# Extraction State Synchronization Fix

## Problem

After running the bulk ingestion, the daily incremental API extraction was re-extracting all contributions for some committees (e.g., C00784934 with 39,285 records), causing extremely slow runs.

### Root Cause

The bulk ingestion flow **did not update extraction states** after loading contributions. This meant:

1. Bulk file loaded contributions with `source_system = 'FEC_BULK'`
2. The `bronze_fec_extraction_state` table was never updated with these bulk-loaded contributions
3. Daily incremental flow calculated start date using old/missing extraction state
4. With the old 180-day lookback, it calculated `start_date = 2025-01-01`
5. This caused re-extraction of ~39K records that already existed in the database

**Note**: Lookback is now 1 day with monthly bulk reconciliation, making this much faster.

### Example from Logs

```
2025-11-24 16:53:39.358 | INFO | Flow run 'complex-kangaroo' - Incremental extraction starting from 2025-01-01 (default lookback from last extraction)
2025-11-24 16:53:42.837 | INFO | Flow run 'complex-kangaroo' - Total available: 39,285 records, 393 pages
```

This committee's contributions were already in the bulk file, but the extraction state didn't know about them.

## Solution

### 1. Fix the Bulk Ingestion Flow (Future Runs)

**Changes to `fund_lens_etl/flows/bulk_ingestion_flow.py`:**

1. Modified `extract_contributions_from_bulk_task()` to track unique committee IDs:
   - Added `committees_seen` set to track all committees with contributions
   - Returns `committees_seen` in the result dictionary

2. Added new task `update_extraction_states_for_bulk_task()`:
   - Takes list of committee IDs and election cycle
   - For each committee, queries the database for latest contribution date/sub_id
   - Updates extraction state with:
     - `last_contribution_date`: Latest contribution date in DB
     - `last_sub_id`: Latest sub_id in DB
     - `extraction_start_date = None`: Indicates full load
     - `is_complete = True`: Marks as complete

3. Updated `bulk_ingestion_flow()` to call the new task:
   - Collects all committees across all contribution types (indiv, pas2, oth)
   - After loading contributions, updates extraction states for all committees
   - Logs progress and results

**Result:** Future bulk ingestion runs will automatically update extraction states, preventing this issue.

### 2. Fix Existing Data (Immediate Solution)

**New script: `scripts/update_extraction_states_from_db.py`**

This script updates extraction states based on existing data in the database:

```bash
# Update all committees for 2026 cycle
poetry run python scripts/update_extraction_states_from_db.py --cycle 2026

# Dry run to see what would be updated
poetry run python scripts/update_extraction_states_from_db.py --cycle 2026 --dry-run

# Update specific committees only
poetry run python scripts/update_extraction_states_from_db.py --cycle 2026 --committees C00784934 C00401224
```

**What it does:**
1. Queries `bronze_fec_schedule_a` for all committees with contributions
2. For each committee, gets the latest `contribution_receipt_date` and `sub_id`
3. Updates `bronze_fec_extraction_state` with this information
4. Logs progress every 100 committees

**When to use:**
- After bulk loading data (if you used the old version without state updates)
- When extraction states are out of sync with actual data
- After a data migration or database restore

## How Incremental Extraction Works

### Extraction State Table

The `bronze_fec_extraction_state` table tracks:

```sql
committee_id               | C00784934
election_cycle             | 2026
last_contribution_date     | 2024-12-31
last_sub_id                | 1234567890
extraction_start_date      | NULL (full load) or date (incremental)
is_complete                | true
last_page_processed        | 0
```

### Incremental Start Date Calculation

From `fund_lens_etl/utils/extraction_state.py:calculate_incremental_start_date()`:

```python
lookback_date = state.last_contribution_date - timedelta(days=lookback_days)
```

**Current lookback:** 1 day (configured in `config.py`)
**Old lookback:** Was 180 days before monthly reconciliation

**Why lookback?**
- FEC filings can be late (up to 45 days after quarter end)
- Amendments to previous filings
- FEC data processing delays

### What Happens Without Extraction State

If no extraction state exists for a committee:
1. `calculate_incremental_start_date()` returns `None`
2. API extractor fetches **all** contributions (no date filter)
3. First page shows total available records
4. Extracts page-by-page until complete

With ~39K records × 100 per page = 393 pages × ~3 seconds/page = **~20 minutes per committee**

### What Happens With Extraction State

If extraction state exists:
1. Calculates `start_date = last_contribution_date - 1 day` (was 180 days before monthly reconciliation)
2. API extractor uses date filter: `contribution_receipt_date >= start_date`
3. Only fetches new/amended contributions
4. Typically 0-10 pages instead of 393 pages

**Result:** Incremental runs are **100x faster** (seconds vs minutes per committee)

## Verification

### Check Extraction States

```sql
-- Check if extraction states exist
SELECT
    committee_id,
    last_contribution_date,
    extraction_start_date,
    is_complete,
    last_extraction_timestamp
FROM bronze_fec_extraction_state
WHERE election_cycle = 2026
ORDER BY last_extraction_timestamp DESC
LIMIT 10;
```

### Check Next Incremental Run

After updating extraction states, the next incremental run should:

1. Log: `Incremental extraction starting from <recent date> (default lookback from last extraction)`
2. Show much smaller page counts (e.g., "Total available: 50 records, 1 pages")
3. Complete in seconds instead of minutes

### Example Before/After

**Before (no extraction state):**
```
Incremental extraction starting from 2025-01-01
Total available: 39,285 records, 393 pages
Time: ~20 minutes
```

**After (with extraction state + 1-day lookback):**
```
Incremental extraction starting from 2024-11-23 (1 day lookback from last extraction)
Total available: 12 records, 1 page
Time: ~5 seconds
```

**Note**: With monthly bulk reconciliation, 1-day lookback is optimal. Monthly bulk ensures all historical data is complete.

## Prevention

### Future Bulk Loads

The updated `bulk_ingestion_flow()` will automatically:
1. Track all committees during contribution loading
2. Update extraction states after loading
3. Prevent re-extraction in next incremental run

### Monthly Bulk Reconciliation

From `docs/BULK_DATA_DEPLOYMENT.md`:

```bash
# Monthly reconciliation (catches amendments)
poetry run python scripts/download_bulk_data.py --cycle 2026 --output-dir data/2025-2026
poetry run python scripts/run_bulk_ingestion.py --data-dir data/2025-2026 --cycle 2026
```

The bulk ingestion will:
- Skip existing records (fast UPSERT check on `sub_id`)
- Insert only new/amended records
- Update extraction states to latest dates
- Keep incremental runs fast

## Files Changed

### Modified

- `fund_lens_etl/flows/bulk_ingestion_flow.py`
  - Track committees during contribution loading
  - Added `update_extraction_states_for_bulk_task()`
  - Call state update task after loading contributions

### Created

- `scripts/update_extraction_states_from_db.py`
  - One-time script to fix existing data
  - Can also be used for periodic state rebuilds

- `docs/EXTRACTION_STATE_FIX.md`
  - This document

## Next Steps

### Immediate (Fix Current Issue)

1. **Update extraction states for existing data:**
   ```bash
   # On your ETL VM
   cd /opt/fund-lens-etl
   poetry run python scripts/update_extraction_states_from_db.py --cycle 2026
   ```

2. **Verify the fix:**
   - Wait for next scheduled incremental run
   - Check logs for "Incremental extraction starting from <recent date>"
   - Verify much smaller page counts

### Going Forward

1. **Deploy updated code:**
   ```bash
   git add -A
   git commit -m "Fix: Update extraction states after bulk ingestion"
   git push origin main
   ```

2. **Future bulk loads will automatically update states**
   - No manual intervention needed
   - Extraction states stay in sync

3. **Monthly reconciliation:**
   - Re-run bulk ingestion monthly
   - Catches amendments and late filings
   - Maintains data accuracy

## Related Files

- `fund_lens_etl/utils/extraction_state.py` - State management utilities
- `fund_lens_etl/flows/bronze_ingestion_flow.py` - Incremental API flow
- `fund_lens_etl/config.py` - Lookback days configuration (180)
- `docs/BULK_DATA_DEPLOYMENT.md` - Bulk ingestion documentation
