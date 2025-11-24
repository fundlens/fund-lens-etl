#!/usr/bin/env python3
"""Test numpy type conversion when reading from SQL."""

import pandas as pd
import numpy as np
from typing import Any

def to_python_type(value: Any) -> Any:
    """Convert numpy/pandas types to Python native types for database compatibility."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar
        return value.item()
    return value


# Simulate what pd.read_sql returns - creates numpy dtypes
data = {
    "contributor_id_gold": pd.array([1], dtype="Int64"),  # Pandas nullable Int
    "committee_id_gold": np.array([2], dtype=np.int64),   # NumPy int64
    "candidate_id_gold": pd.array([None], dtype="Int64"), # Nullable with None
    "contribution_amount": np.array([24.00], dtype=np.float64),
    "election_cycle": np.array([2026], dtype=np.int64),
}

df = pd.DataFrame(data)

print("DataFrame dtypes:")
print(df.dtypes)
print()

# Convert to dict like in the actual code
records = df.to_dict("records")

print("Testing conversion from SQL-like data...")
for record in records:
    print(f"\nOriginal record types:")
    for key, value in record.items():
        print(f"  {key}: {type(value)} = {value}")

    print(f"\nAfter to_python_type():")
    for key, value in record.items():
        converted = to_python_type(value)
        print(f"  {key}: {type(converted)} = {converted}")

    # Test the specific fields
    print(f"\nChecking contributor_id_gold:")
    val = record['contributor_id_gold']
    print(f"  Type: {type(val)}")
    print(f"  Value: {val}")
    print(f"  hasattr .item()? {hasattr(val, 'item')}")
    if hasattr(val, 'item'):
        print(f"  .item() = {val.item()} (type: {type(val.item())})")

    print(f"\nChecking candidate_id_gold (nullable):")
    val = record['candidate_id_gold']
    print(f"  Type: {type(val)}")
    print(f"  Value: {val}")
    print(f"  pd.isna()? {pd.isna(val)}")
    print(f"  After conversion: {to_python_type(val)}")
