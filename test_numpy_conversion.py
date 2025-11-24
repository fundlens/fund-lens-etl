#!/usr/bin/env python3
"""Test numpy type conversion for psycopg2 compatibility."""

import pandas as pd
from typing import Any

def to_python_type(value: Any) -> Any:
    """Convert numpy/pandas types to Python native types for database compatibility."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar
        return value.item()
    return value


# Create a sample dataframe matching production data structure
data = {
    "contributor_id_gold": [1],
    "committee_id_gold": [2],
    "candidate_id_gold": [None],  # NULL in production
    "contribution_amount": [24.00],
    "election_cycle": [2026],
    "contribution_date": ["2025-06-23"],
    "receipt_type": ["15J"],
    "election_type": ["P2026"],
    "transaction_id": ["SA.5037275.141.T225"],
    "source_sub_id": ["4100620251292663745"],
    "memo_text": ["TRANSFER FROM TEAM SCALISE JFC"],
}

df = pd.DataFrame(data)

# Convert to dict like in the actual code
records = df.to_dict("records")

print("Testing conversion...")
for record in records:
    print(f"\nOriginal record types:")
    for key, value in record.items():
        print(f"  {key}: {type(value)} = {value}")

    print(f"\nAfter to_python_type():")
    for key, value in record.items():
        converted = to_python_type(value)
        print(f"  {key}: {type(converted)} = {converted}")

    # Test the specific fields that are causing issues
    print(f"\nSpecific field checks:")
    print(f"  contributor_id_gold: {type(record['contributor_id_gold'])}")
    print(f"  hasattr .item()? {hasattr(record['contributor_id_gold'], 'item')}")
    print(f"  After conversion: {type(to_python_type(record['contributor_id_gold']))}")
