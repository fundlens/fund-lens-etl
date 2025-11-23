#!/usr/bin/env python3
"""Compare fields between bulk files and Bronze model."""

# Fields from pas2_header_file.csv (and indiv/oth which are similar)
BULK_FILE_FIELDS = {
    "CMTE_ID",
    "AMNDT_IND",
    "RPT_TP",
    "TRANSACTION_PGI",
    "IMAGE_NUM",
    "TRANSACTION_TP",
    "ENTITY_TP",
    "NAME",
    "CITY",
    "STATE",
    "ZIP_CODE",
    "EMPLOYER",
    "OCCUPATION",
    "TRANSACTION_DT",
    "TRANSACTION_AMT",
    "OTHER_ID",
    "CAND_ID",  # Only in pas2
    "TRAN_ID",
    "FILE_NUM",
    "MEMO_CD",
    "MEMO_TEXT",
    "SUB_ID",
}

# Fields in BronzeFECScheduleA model (from fund-lens-models)
BRONZE_MODEL_FIELDS = {
    "sub_id",  # PK
    "transaction_id",
    "file_number",
    "amendment_indicator",
    "contribution_receipt_date",
    "contribution_receipt_amount",
    "contributor_aggregate_ytd",
    "contributor_name",
    "contributor_first_name",
    "contributor_last_name",
    "contributor_middle_name",
    "contributor_city",
    "contributor_state",
    "contributor_zip",
    "contributor_employer",
    "contributor_occupation",
    "entity_type",
    "committee_id",
    "recipient_committee_designation",
    "recipient_committee_type",
    "recipient_committee_org_type",
    "receipt_type",
    "election_type",
    "memo_text",
    "memo_code",
    "two_year_transaction_period",
    "report_year",
    "report_type",
    "raw_json",
    # Missing: candidate_id!
}

# Column mapping from extractor
COLUMN_MAPPING = {
    "CMTE_ID": "committee_id",
    "AMNDT_IND": "amendment_indicator",
    "RPT_TP": "report_type",
    "TRANSACTION_PGI": "election_type",
    "IMAGE_NUM": "image_number",  # NOT IN MODEL!
    "TRANSACTION_TP": "transaction_type",  # NOT IN MODEL!
    "ENTITY_TP": "entity_type",
    "NAME": "contributor_name",
    "CITY": "contributor_city",
    "STATE": "contributor_state",
    "ZIP_CODE": "contributor_zip",
    "EMPLOYER": "contributor_employer",
    "OCCUPATION": "contributor_occupation",
    "TRANSACTION_DT": "contribution_receipt_date",
    "TRANSACTION_AMT": "contribution_receipt_amount",
    "OTHER_ID": "other_id",  # NOT IN MODEL!
    "CAND_ID": "candidate_id",  # NOT IN MODEL!
    "TRAN_ID": "transaction_id",
    "FILE_NUM": "file_number",
    "MEMO_CD": "memo_code",
    "MEMO_TEXT": "memo_text",
    "SUB_ID": "sub_id",
}

if __name__ == "__main__":
    print("=" * 80)
    print("FIELD COMPARISON: Bulk Files vs Bronze Model")
    print("=" * 80)

    mapped_fields = set(COLUMN_MAPPING.values())
    missing_in_model = mapped_fields - BRONZE_MODEL_FIELDS

    print("\n Fields being DROPPED (in bulk file but NOT in Bronze model):")
    print("-" * 80)
    for field in sorted(missing_in_model):
        bulk_field = [k for k, v in COLUMN_MAPPING.items() if v == field][0]
        print(f"  - {field:30s} (from {bulk_field})")

    print(f"\n Total fields being dropped: {len(missing_in_model)}")
    print("=" * 80)
