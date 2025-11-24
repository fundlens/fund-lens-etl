#!/usr/bin/env python3
"""Test gold contribution insertion with production-like data."""

import pandas as pd
from typing import Any, cast
from fund_lens_models.gold import GoldContribution
from fund_lens_etl.database import get_session

# Sample data from production silver table
data = {
    "id": [13888312],
    "source_sub_id": ["4100620251292663745"],
    "transaction_id": ["SA.5037275.141.T225"],
    "contribution_date": ["2025-06-23"],
    "contribution_amount": [24.00],
    "contributor_name": ["ROBERSON, MARILYNN"],
    "contributor_city": ["EASLEY"],
    "contributor_state": ["SC"],
    "contributor_employer": ["RETIRED"],
    "committee_id": ["C00394957"],
    "candidate_id": [None],  # NULL in production
    "receipt_type": ["15J"],
    "election_type": ["P2026"],
    "memo_text": ["TRANSFER FROM TEAM SCALISE JFC"],
    "election_cycle": [2026],
}

df = pd.DataFrame(data)

print("DataFrame info:")
print(df.dtypes)
print()

# Simulate the cache lookups (fake IDs)
contributor_cache = {"ROBERSON, MARILYNN|EASLEY|SC|RETIRED": 1}
committee_cache = {"C00394957": 2}
candidate_cache = {}  # Empty - no candidate

# Simulate the transformation code
df["contributor_key"] = df.apply(
    lambda r: f"{r['contributor_name']}|{r['contributor_city']}|{r['contributor_state']}|{r['contributor_employer']}",
    axis=1,
)

df["contributor_id_gold"] = df["contributor_key"].map(contributor_cache)
df["committee_id_gold"] = df["committee_id"].map(committee_cache)
df["candidate_id_gold"] = df["candidate_id"].map(candidate_cache)

# Filter valid rows
valid_chunk = df[
    df["contributor_id_gold"].notna() & df["committee_id_gold"].notna()
].copy()

print("Valid chunk:")
print(valid_chunk[["contributor_id_gold", "committee_id_gold", "candidate_id_gold", "election_cycle"]])
print()

# Convert to dict
records = cast(list[dict[str, Any]], valid_chunk.to_dict("records"))

print("Record data:")
for key, value in records[0].items():
    print(f"  {key}: {type(value).__name__} = {repr(value)}")
print()

# Try to create the model object
print("Creating GoldContribution object...")
record = records[0]

try:
    contribution = GoldContribution(
        contributor_id=record["contributor_id_gold"],
        recipient_committee_id=record["committee_id_gold"],
        recipient_candidate_id=record["candidate_id_gold"],
        contribution_date=record["contribution_date"],
        amount=record["contribution_amount"],
        contribution_type=record.get("receipt_type") or "DIRECT",
        election_type=record.get("election_type"),
        source_system="FEC",
        source_transaction_id=record.get("transaction_id"),
        source_sub_id=record["source_sub_id"],
        election_year=record["election_cycle"],
        election_cycle=record["election_cycle"],
        memo_text=record.get("memo_text"),
    )

    print("✓ GoldContribution object created successfully")
    print(f"  contributor_id: {contribution.contributor_id} (type: {type(contribution.contributor_id)})")
    print(f"  recipient_committee_id: {contribution.recipient_committee_id} (type: {type(contribution.recipient_committee_id)})")
    print(f"  recipient_candidate_id: {contribution.recipient_candidate_id} (type: {type(contribution.recipient_candidate_id)})")
    print(f"  election_cycle: {contribution.election_cycle} (type: {type(contribution.election_cycle)})")
    print(f"  election_year: {contribution.election_year} (type: {type(contribution.election_year)})")
    print()

    # Try to insert it
    print("Attempting database insert...")
    with get_session() as session:
        session.add(contribution)
        session.commit()
        print(f"✓ Successfully inserted contribution with id={contribution.id}")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
