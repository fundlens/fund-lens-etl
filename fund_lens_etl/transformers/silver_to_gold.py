"""Transformers for silver to gold layer."""

from datetime import UTC, datetime

import pandas as pd
from prefect import get_run_logger
from sqlalchemy.orm import Session

from fund_lens_etl.models.gold import GoldCommittee, GoldContributor
from fund_lens_etl.transformers.base import BaseTransformer


# noinspection PyArgumentEqualDefault,PyMethodMayBeStatic
class SilverToGoldFECTransformer(BaseTransformer):
    """Transform FEC silver data to gold layer with entity resolution."""

    def __init__(self, session: Session):
        """
        Initialize transformer with database session.

        Args:
            session: SQLAlchemy session for entity lookups/creation
        """
        self.session = session

    def get_source_layer(self) -> str:
        """Get source layer name."""
        return "silver"

    def get_target_layer(self) -> str:
        """Get target layer name."""
        return "gold"

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform silver FEC contribution data to gold layer.

        This involves:
        - Entity resolution (contributors, committees, candidates)
        - Deduplication across sources
        - Standardized schemas
        - Foreign key relationships

        Args:
            df: Silver layer DataFrame
            **kwargs: Additional parameters

        Returns:
            Gold layer DataFrame with resolved entities
        """
        logger = get_run_logger()
        logger.info(f"Transforming {len(df)} silver records to gold")

        if df.empty:
            logger.warning("Empty DataFrame received, returning empty gold DataFrame")
            return pd.DataFrame()

        gold_records = []

        for idx, row in df.iterrows():
            try:
                # Resolve or create contributor
                contributor_id = self._resolve_contributor(row)

                # Resolve or create committee
                committee_id = self._resolve_committee(row)

                # Resolve candidate (if applicable)
                candidate_id = self._resolve_candidate(row)

                # Create gold contribution record
                gold_record = {
                    "source_system": "FEC",
                    "source_transaction_id": row["bronze_sub_id"],
                    "contribution_date": row["contribution_date"],
                    "amount": row["contribution_amount"],
                    "contributor_id": contributor_id,
                    "recipient_committee_id": committee_id,
                    "recipient_candidate_id": candidate_id,
                    "contribution_type": self._classify_contribution_type(row),
                    "election_type": row.get("election_type"),
                    "election_year": self._get_election_year(row),
                    "election_cycle": row["election_cycle"],
                    "memo_text": row.get("memo_text"),
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }

                gold_records.append(gold_record)

            except Exception as e:
                logger.error(f"Error transforming row {idx}: {e}")
                continue

        # Commit entity resolutions
        self.session.commit()

        gold_df = pd.DataFrame(gold_records)
        logger.info(f"Transformation complete: {len(gold_df)} gold records created")

        return gold_df

    def _resolve_contributor(self, row: pd.Series) -> int:
        """
        Resolve or create a gold contributor entity.

        Args:
            row: Silver contribution row

        Returns:
            Gold contributor ID
        """
        # Try to find existing contributor by name + location
        existing = (
            self.session.query(GoldContributor)
            .filter(
                GoldContributor.name == row["contributor_name"],
                GoldContributor.state == row.get("contributor_state"),
                GoldContributor.zip == row.get("contributor_zip"),
            )
            .first()
        )

        if existing:
            return existing.id  # type: ignore[return-value]

        # Create new contributor
        contributor = GoldContributor(
            name=str(row["contributor_name"]),
            first_name=row.get("contributor_first_name"),
            last_name=row.get("contributor_last_name"),
            city=row.get("contributor_city"),
            state=row.get("contributor_state"),
            zip=row.get("contributor_zip"),
            employer=row.get("contributor_employer"),
            occupation=row.get("contributor_occupation"),
            entity_type=self._map_entity_type(row.get("entity_type")),
            match_confidence=1.0,
        )

        self.session.add(contributor)
        self.session.flush()

        return contributor.id

    def _resolve_committee(self, row: pd.Series) -> int:
        """
        Resolve or create a gold committee entity.

        Args:
            row: Silver contribution row

        Returns:
            Gold committee ID
        """
        committee_fec_id = str(row["committee_id"])  # Fix: explicit cast

        # Try to find existing by FEC ID
        existing = (
            self.session.query(GoldCommittee)
            .filter(GoldCommittee.fec_committee_id == committee_fec_id)
            .first()
        )

        if existing:
            return existing.id  # type: ignore[return-value]

        # Create new committee
        committee = GoldCommittee(
            name=str(row.get("committee_name", "UNKNOWN")),  # Fix: explicit cast
            committee_type=self._map_committee_type(row.get("committee_designation")),
            state=None,  # Would need to join with committee table for this
            city=None,
            candidate_id=None,  # Resolved separately
            fec_committee_id=committee_fec_id,
            is_active=True,
        )

        self.session.add(committee)
        self.session.flush()

        return committee.id

    def _resolve_candidate(self, row: pd.Series) -> int | None:  # Fix: remove unused param warning
        """
        Resolve candidate if this is a candidate committee.

        Args:
            row: Silver contribution row (not used in MVP)

        Returns:
            Gold candidate ID or None
        """
        # For MVP, we don't have candidate linkage in silver layer yet
        # This would require joining with silver_fec_committee/candidate tables
        # Returning None for now
        _ = row  # Explicitly mark as intentionally unused
        return None

    def _classify_contribution_type(self, row: pd.Series) -> str:
        """
        Classify contribution type from receipt_type.

        Args:
            row: Silver contribution row

        Returns:
            Standardized contribution type
        """
        receipt_type = row.get("receipt_type", "")

        type_mapping = {
            "15": "DIRECT",
            "15E": "EARMARKED",
            "15J": "JOINT_FUNDRAISING",
            "15T": "TRANSFER",
            "15C": "LOAN",
            "16": "LOAN_REPAYMENT",
            "17": "REFUND",
            "18": "REBATE",
            "19": "VOID",
        }

        return type_mapping.get(receipt_type, "OTHER")

    def _map_entity_type(self, entity_type: str | None) -> str:
        """
        Map FEC entity type to gold standard.

        Args:
            entity_type: FEC entity type code

        Returns:
            Standardized entity type
        """
        if not entity_type:
            return "UNKNOWN"

        type_mapping = {
            "IND": "INDIVIDUAL",
            "COM": "COMMITTEE",
            "ORG": "ORGANIZATION",
            "PAC": "PAC",
            "CCM": "CANDIDATE_COMMITTEE",
            "PTY": "PARTY",
        }

        return type_mapping.get(entity_type, "OTHER")

    def _map_committee_type(self, designation: str | None) -> str:
        """
        Map FEC committee designation to gold standard.

        Args:
            designation: FEC designation code

        Returns:
            Standardized committee type
        """
        if not designation:
            return "UNKNOWN"

        type_mapping = {
            "P": "CANDIDATE",
            "A": "CANDIDATE",
            "U": "PAC",
            "B": "PAC",
            "D": "PAC",
            "J": "JOINT_FUNDRAISING",
        }

        return type_mapping.get(designation, "OTHER")

    def _get_election_year(self, row: pd.Series) -> int:
        """
        Determine election year from contribution date and cycle.

        Args:
            row: Silver contribution row

        Returns:
            Election year
        """
        # For two-year cycles, the election year is the even year
        cycle = row["election_cycle"]
        return cycle if cycle % 2 == 0 else cycle + 1  # type: ignore[return-value]
