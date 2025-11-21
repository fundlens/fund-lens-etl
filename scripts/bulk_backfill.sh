#!/bin/bash
# All-in-one bulk data backfill script
#
# This script downloads FEC bulk data and loads it into the database.
# Usage:
#   ./scripts/bulk_backfill.sh 2026
#   ./scripts/bulk_backfill.sh 2026 --all        # Download all files (not just essential)
#   ./scripts/bulk_backfill.sh 2026 --download-only  # Only download, don't ingest

set -e  # Exit on error

CYCLE=${1:-2026}
DATA_DIR="data/2025-2026"
DOWNLOAD_ARGS=""
SKIP_INGESTION=false

# Parse additional arguments
shift || true  # Remove first argument (cycle)
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            DOWNLOAD_ARGS="--all"
            shift
            ;;
        --download-only)
            SKIP_INGESTION=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 CYCLE [--all] [--download-only]"
            exit 1
            ;;
    esac
done

echo "================================================================================"
echo "FEC BULK DATA BACKFILL"
echo "================================================================================"
echo "Cycle: $CYCLE"
echo "Data directory: $DATA_DIR"
echo "Download all files: ${DOWNLOAD_ARGS:-(essential only)}"
echo "Skip ingestion: $SKIP_INGESTION"
echo "================================================================================"
echo ""

# Step 1: Download bulk data files
echo "Step 1: Downloading bulk data files..."
echo "================================================================================"

poetry run python scripts/download_bulk_data.py \
    --cycle "$CYCLE" \
    --output-dir "$DATA_DIR" \
    $DOWNLOAD_ARGS

echo ""
echo "✓ Download complete"
echo ""

# Step 2: Verify downloads
echo "Step 2: Verifying downloads..."
echo "================================================================================"

if [ ! -f "$DATA_DIR/cm.txt" ]; then
    echo "✗ Error: Committee file not found (cm.txt)"
    exit 1
fi

if [ ! -f "$DATA_DIR/cn.txt" ]; then
    echo "✗ Error: Candidate file not found (cn.txt)"
    exit 1
fi

if [ ! -f "$DATA_DIR/indiv26/itcont.txt" ]; then
    echo "✗ Error: Individual contributions file not found (indiv26/itcont.txt)"
    exit 1
fi

echo "✓ All required files present"
echo ""
du -sh "$DATA_DIR"
echo ""

# Step 3: Run test to validate file parsing
echo "Step 3: Testing bulk file extraction..."
echo "================================================================================"

poetry run python scripts/test_bulk_ingestion.py

echo ""
echo "✓ File parsing validation complete"
echo ""

# Step 4: Run ingestion (unless --download-only)
if [ "$SKIP_INGESTION" = true ]; then
    echo "Skipping ingestion (--download-only specified)"
    echo ""
    echo "To ingest manually, run:"
    echo "  poetry run python scripts/run_bulk_ingestion.py \\"
    echo "    --data-dir $DATA_DIR \\"
    echo "    --cycle $CYCLE"
else
    echo "Step 4: Running bulk ingestion..."
    echo "================================================================================"
    echo "⏱️  This may take 1-3 hours for ~9.2M contribution records"
    echo "   Monitor progress: tail -f /tmp/bulk_ingestion_*.log"
    echo ""

    poetry run python scripts/run_bulk_ingestion.py \
        --data-dir "$DATA_DIR" \
        --cycle "$CYCLE"

    echo ""
    echo "✓ Ingestion complete"
fi

echo ""
echo "================================================================================"
echo "BULK BACKFILL COMPLETE"
echo "================================================================================"
echo ""
echo "Data loaded from: $DATA_DIR"
echo ""
echo "Next steps:"
echo "  1. Verify data: psql fund_lens -c 'SELECT COUNT(*) FROM bronze_fec_schedule_a;'"
echo "  2. Run silver transformation (if available)"
echo "  3. Run gold aggregation (if available)"
echo "  4. Optional: Clean up data directory to free disk space"
echo "     rm -rf $DATA_DIR"
echo ""
