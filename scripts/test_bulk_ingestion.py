#!/usr/bin/env python3
"""
Test script for bulk file ingestion.

Quick validation of bulk file parsing and schema mapping.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fund_lens_etl.extractors.bulk import (
    BulkFECCandidateExtractor,
    BulkFECCommitteeExtractor,
    BulkFECContributionExtractor,
)


def test_committee_extraction():
    """Test committee bulk file extraction."""
    print("\n" + "=" * 80)
    print("Testing Committee Extraction")
    print("=" * 80)

    data_dir = project_root / "data" / "2025-2026"
    data_file = data_dir / "cm.txt"
    header_file = data_dir / "cm_header_file.csv"

    if not data_file.exists():
        print(f"❌ File not found: {data_file}")
        return False

    extractor = BulkFECCommitteeExtractor()
    df = extractor.extract(file_path=data_file, header_file_path=header_file)

    print(f"✓ Extracted {len(df):,} committees")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample record:")
    print(df.head(1).T)

    # Validate key columns
    required_cols = ["committee_id", "name", "state", "committee_type"]
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        print(f"❌ Missing required columns: {missing}")
        return False

    print(f"✓ All required columns present")
    return True


def test_candidate_extraction():
    """Test candidate bulk file extraction."""
    print("\n" + "=" * 80)
    print("Testing Candidate Extraction")
    print("=" * 80)

    data_dir = project_root / "data" / "2025-2026"
    data_file = data_dir / "cn.txt"
    header_file = data_dir / "cn_header_file.csv"

    if not data_file.exists():
        print(f"❌ File not found: {data_file}")
        return False

    extractor = BulkFECCandidateExtractor()
    df = extractor.extract(file_path=data_file, header_file_path=header_file)

    print(f"✓ Extracted {len(df):,} candidates")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSample record:")
    print(df.head(1).T)

    # Validate key columns
    required_cols = ["candidate_id", "name", "state", "office"]
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        print(f"❌ Missing required columns: {missing}")
        return False

    print(f"✓ All required columns present")
    return True


def test_contribution_extraction_chunked():
    """Test contribution bulk file extraction (chunked for large file)."""
    print("\n" + "=" * 80)
    print("Testing Contribution Extraction (First Chunk Only)")
    print("=" * 80)

    data_dir = project_root / "data" / "2025-2026"
    data_file = data_dir / "indiv26" / "itcont.txt"
    header_file = data_dir / "indiv_header_file.csv"

    if not data_file.exists():
        print(f"❌ File not found: {data_file}")
        return False

    extractor = BulkFECContributionExtractor()

    # Test first chunk only (file is huge)
    chunk_iter = extractor.extract_chunked(
        file_path=data_file,
        header_file_path=header_file,
        chunksize=1000,  # Small chunk for testing
    )

    first_chunk = next(chunk_iter)

    print(f"✓ Extracted first chunk: {len(first_chunk):,} records")
    print(f"\nColumns ({len(first_chunk.columns)}): {list(first_chunk.columns)[:10]}...")
    print(f"\nSample record:")
    print(first_chunk.head(1).T)

    # Validate key columns
    required_cols = [
        "sub_id",
        "committee_id",
        "contributor_name",
        "contribution_receipt_date",
        "contribution_receipt_amount",
    ]
    missing = [col for col in required_cols if col not in first_chunk.columns]

    if missing:
        print(f"❌ Missing required columns: {missing}")
        return False

    # Validate data types
    print(f"\nData type validation:")
    print(f"  contribution_receipt_date: {first_chunk['contribution_receipt_date'].dtype}")
    print(f"  contribution_receipt_amount: {first_chunk['contribution_receipt_amount'].dtype}")
    print(f"  sub_id: {first_chunk['sub_id'].dtype}")

    # Check for nulls in key fields
    null_sub_ids = first_chunk["sub_id"].isna().sum()
    null_amounts = first_chunk["contribution_receipt_amount"].isna().sum()

    print(f"\nNull check:")
    print(f"  sub_id nulls: {null_sub_ids}")
    print(f"  amount nulls: {null_amounts}")

    if null_sub_ids > 0:
        print(f"⚠️  Warning: {null_sub_ids} records have null sub_id")

    print(f"✓ All required columns present")
    return True


def main():
    """Run all tests."""
    print("=" * 80)
    print("BULK FILE EXTRACTION TESTS")
    print("=" * 80)

    results = {
        "committees": test_committee_extraction(),
        "candidates": test_candidate_extraction(),
        "contributions": test_contribution_extraction_chunked(),
    }

    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "❌ FAIL"
        print(f"{test_name.capitalize()}: {status}")

    all_passed = all(results.values())
    print("=" * 80)

    if all_passed:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
