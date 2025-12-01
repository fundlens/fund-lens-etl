# Cleanup Non-Maryland FEC Data

This document contains SQL commands to remove all non-Maryland FEC data from the database, limiting the scope to:

- **Maryland committees**: All committees where `state = 'MD'`
- **Maryland candidates**: All candidates where `state = 'MD'`
- **Out-of-state committees for MD candidates**: Committees linked to MD candidates even if committee state ≠ MD
- **Contributions**: All contributions TO the above committees (regardless of contributor location)

## Prerequisites

- Run these commands in `psql` connected to your fund_lens database
- Ensure you have a backup before running destructive commands
- Run commands in order due to foreign key relationships

## SQL Commands

### Step 1: Create temp tables to identify MD-related data to KEEP

```sql
-- Identify MD-related committees (MD committees + committees linked to MD candidates)
CREATE TEMP TABLE md_committees_to_keep AS
SELECT DISTINCT gc.id
FROM gold_committee gc
WHERE gc.fec_committee_id IS NOT NULL
  AND (
    -- MD-based committees
    gc.state = 'MD'
    -- OR committees linked to MD candidates (handles out-of-state principal campaign committees)
    OR gc.candidate_id IN (
      SELECT id FROM gold_candidate WHERE state = 'MD' AND fec_candidate_id IS NOT NULL
    )
  );

-- Identify MD candidates to KEEP
CREATE TEMP TABLE md_candidates_to_keep AS
SELECT id FROM gold_candidate
WHERE fec_candidate_id IS NOT NULL AND state = 'MD';
```

### Step 2: Preview what will be deleted (optional)

```sql
SELECT 'Contributions to delete' as entity, COUNT(*) as count
FROM gold_contribution
WHERE source_system = 'FEC'
  AND recipient_committee_id NOT IN (SELECT id FROM md_committees_to_keep)
UNION ALL
SELECT 'Committees to delete', COUNT(*)
FROM gold_committee
WHERE fec_committee_id IS NOT NULL
  AND id NOT IN (SELECT id FROM md_committees_to_keep)
UNION ALL
SELECT 'Candidates to delete', COUNT(*)
FROM gold_candidate
WHERE fec_candidate_id IS NOT NULL
  AND id NOT IN (SELECT id FROM md_candidates_to_keep);
```

### Step 3: Delete from Gold layer

```sql
-- Delete FEC contributions to non-MD committees
DELETE FROM gold_contribution
WHERE source_system = 'FEC'
  AND recipient_committee_id NOT IN (SELECT id FROM md_committees_to_keep);

-- Delete non-MD FEC committees (that aren't linked to MD candidates)
DELETE FROM gold_committee
WHERE fec_committee_id IS NOT NULL
  AND id NOT IN (SELECT id FROM md_committees_to_keep);

-- Delete non-MD FEC candidates
DELETE FROM gold_candidate
WHERE fec_candidate_id IS NOT NULL
  AND id NOT IN (SELECT id FROM md_candidates_to_keep);
```

### Step 4: Clean up orphaned contributors

```sql
-- WARNING: This may take a while on large datasets
DELETE FROM gold_contributor
WHERE id NOT IN (SELECT DISTINCT contributor_id FROM gold_contribution);
```

### Step 5: Delete from Silver layer

```sql
-- Silver contributions
DELETE FROM silver_fec_contribution
WHERE committee_id NOT IN (
  SELECT fec_committee_id FROM gold_committee WHERE fec_committee_id IS NOT NULL
);

-- Silver committees
DELETE FROM silver_fec_committee
WHERE source_committee_id NOT IN (
  SELECT fec_committee_id FROM gold_committee WHERE fec_committee_id IS NOT NULL
);

-- Silver candidates
DELETE FROM silver_fec_candidate
WHERE source_candidate_id NOT IN (
  SELECT fec_candidate_id FROM gold_candidate WHERE fec_candidate_id IS NOT NULL
);
```

### Step 6: Delete from Bronze layer

```sql
-- Bronze Schedule A (contributions)
DELETE FROM bronze_fec_schedule_a
WHERE committee_id NOT IN (
  SELECT fec_committee_id FROM gold_committee WHERE fec_committee_id IS NOT NULL
);

-- Bronze committees
DELETE FROM bronze_fec_committee
WHERE committee_id NOT IN (
  SELECT fec_committee_id FROM gold_committee WHERE fec_committee_id IS NOT NULL
);

-- Bronze candidates
DELETE FROM bronze_fec_candidate
WHERE candidate_id NOT IN (
  SELECT fec_candidate_id FROM gold_candidate WHERE fec_candidate_id IS NOT NULL
);

-- Bronze extraction state
DELETE FROM bronze_fec_extraction_state
WHERE committee_id NOT IN (
  SELECT fec_committee_id FROM gold_committee WHERE fec_committee_id IS NOT NULL
);
```

### Step 7: Verify results

```sql
SELECT 'gold_contribution (FEC)' as table_name, COUNT(*) as count
FROM gold_contribution WHERE source_system = 'FEC'
UNION ALL
SELECT 'gold_contribution (MD)', COUNT(*)
FROM gold_contribution WHERE source_system = 'MARYLAND'
UNION ALL
SELECT 'gold_committee (FEC)', COUNT(*)
FROM gold_committee WHERE fec_committee_id IS NOT NULL
UNION ALL
SELECT 'gold_committee (MD)', COUNT(*)
FROM gold_committee WHERE state_committee_id IS NOT NULL
UNION ALL
SELECT 'gold_candidate (FEC)', COUNT(*)
FROM gold_candidate WHERE fec_candidate_id IS NOT NULL
UNION ALL
SELECT 'gold_candidate (MD)', COUNT(*)
FROM gold_candidate WHERE state_candidate_id IS NOT NULL
UNION ALL
SELECT 'gold_contributor', COUNT(*)
FROM gold_contributor
UNION ALL
SELECT 'silver_fec_contribution', COUNT(*)
FROM silver_fec_contribution
UNION ALL
SELECT 'silver_fec_committee', COUNT(*)
FROM silver_fec_committee
UNION ALL
SELECT 'silver_fec_candidate', COUNT(*)
FROM silver_fec_candidate
UNION ALL
SELECT 'bronze_fec_schedule_a', COUNT(*)
FROM bronze_fec_schedule_a
UNION ALL
SELECT 'bronze_fec_committee', COUNT(*)
FROM bronze_fec_committee
UNION ALL
SELECT 'bronze_fec_candidate', COUNT(*)
FROM bronze_fec_candidate
UNION ALL
SELECT 'bronze_fec_extraction_state', COUNT(*)
FROM bronze_fec_extraction_state;
```

### Step 8: Clean up temp tables

```sql
DROP TABLE md_committees_to_keep;
DROP TABLE md_candidates_to_keep;
```

## Expected Results

After cleanup, you should have approximately:

| Table | Expected Count |
|-------|---------------|
| gold_committee (FEC) | ~446 (MD committees + any out-of-state committees linked to MD candidates) |
| gold_candidate (FEC) | ~131 (MD federal candidates) |
| gold_contribution (FEC) | Varies (all contributions to MD committees) |
| gold_contributor | Reduced (only contributors to MD committees) |

## Notes

- This cleanup is one-time. After running, update the FEC ingestion code to filter for Maryland-only data going forward.
- The Maryland state data (`source_system = 'MARYLAND'`) is not affected by this cleanup.
- Out-of-state committees that are principal campaign committees for MD candidates are preserved.
