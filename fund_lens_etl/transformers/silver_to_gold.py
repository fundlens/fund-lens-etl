"""Transformers for silver to gold layer."""

import logging
from datetime import UTC, datetime
from typing import cast

import pandas as pd
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from sqlalchemy import select
from sqlalchemy.orm import Session

from fund_lens_etl.models.gold import GoldCandidate, GoldCommittee, GoldContributor
from fund_lens_etl.models.silver.fec import SilverFECCandidate, SilverFECCommittee
from fund_lens_etl.transformers.base import BaseTransformer


def get_logger():
    """Get logger - Prefect if available, otherwise standard logging."""
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic,PyArgumentEqualDefault
class SilverToGoldFECTransformer(BaseTransformer):
    """Transform FEC silver data to gold layer with entity resolution."""

    def __init__(self, session: Session):
        """
        Initialize transformer with database session.

        Args:
            session: SQLAlchemy session for entity lookups/creation and Silver JOINs
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
        - Enriching with Silver committee and candidate data via JOINs
        - Entity resolution (contributors, committees, candidates)
        - Deduplication across sources
        - Standardized schemas
        - Foreign key relationships

        Args:
            df: Silver layer DataFrame (contributions only)
            **kwargs: Additional parameters

        Returns:
            Gold layer DataFrame with resolved entities
        """
        logger = get_logger()
        logger.info(f"Transforming {len(df)} silver records to gold")

        if df.empty:
            logger.warning("Empty DataFrame received, returning empty gold DataFrame")
            return pd.DataFrame()

        # Enrich with Silver committee and candidate data
        df = self._enrich_with_silver_entities(df)

        gold_records = []

        for idx, row in df.iterrows():
            try:
                # Resolve or create contributor
                contributor_id = self._resolve_contributor(row)

                # Resolve or create committee
                committee_id = self._resolve_committee(row)

                # Resolve or create candidate (if applicable)
                candidate_id = self._resolve_candidate(row)

                # Create gold contribution record
                gold_record = {
                    "source_system": "FEC",
                    "source_transaction_id": row["sub_id"],
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

    def _enrich_with_silver_entities(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich contribution DataFrame with Silver committee and candidate data via JOINs.

        Args:
            df: DataFrame with committee_id from SilverFECContribution

        Returns:
            DataFrame enriched with committee and candidate details from Silver tables
        """
        logger = get_logger()
        logger.info("Enriching with Silver committee and candidate data")

        # Get unique committee IDs
        unique_committee_ids = df["committee_id"].dropna().unique().tolist()

        if not unique_committee_ids:
            logger.warning("No committee IDs found")
            return df

        # Query Silver committees
        stmt = select(SilverFECCommittee).where(
            SilverFECCommittee.source_committee_id.in_(unique_committee_ids)
        )
        result = self.session.execute(stmt)

        silver_committees = {}
        for committee in result.scalars():
            silver_committees[committee.source_committee_id] = {
                "committee_name": committee.name,
                "committee_type": committee.committee_type,
                "committee_designation": committee.designation,
                "committee_state": committee.state,
                "committee_city": committee.city,
            }

        logger.info(f"Found {len(silver_committees)} committees in Silver")

        # Map committee data to DataFrame
        df["silver_committee_name"] = df["committee_id"].map(
            lambda x: silver_committees.get(x, {}).get("committee_name")
        )
        df["silver_committee_type"] = df["committee_id"].map(
            lambda x: silver_committees.get(x, {}).get("committee_type")
        )
        df["silver_committee_designation"] = df["committee_id"].map(
            lambda x: silver_committees.get(x, {}).get("committee_designation")
        )
        df["silver_committee_state"] = df["committee_id"].map(
            lambda x: silver_committees.get(x, {}).get("committee_state")
        )
        df["silver_committee_city"] = df["committee_id"].map(
            lambda x: silver_committees.get(x, {}).get("committee_city")
        )

        # Now get candidate data from Silver (if candidate_id exists in contribution)
        if "candidate_id" in df.columns:
            unique_candidate_ids = df["candidate_id"].dropna().unique().tolist()

            if unique_candidate_ids:
                candidate_stmt = select(SilverFECCandidate).where(
                    SilverFECCandidate.source_candidate_id.in_(unique_candidate_ids)
                )
                candidate_result = self.session.execute(candidate_stmt)  # New variable name

                silver_candidates = {}
                for candidate in candidate_result.scalars():  # Use new variable name
                    silver_candidates[candidate.source_candidate_id] = {
                        "candidate_name": candidate.name,
                        "candidate_office": candidate.office,
                        "candidate_party": candidate.party,
                        "candidate_state": candidate.state,
                        "candidate_district": candidate.district,
                    }

                logger.info(f"Found {len(silver_candidates)} candidates in Silver")

                # Map candidate data to DataFrame
                df["silver_candidate_name"] = df["candidate_id"].map(
                    lambda x: silver_candidates.get(x, {}).get("candidate_name")
                )
                df["silver_candidate_office"] = df["candidate_id"].map(
                    lambda x: silver_candidates.get(x, {}).get("candidate_office")
                )
                df["silver_candidate_party"] = df["candidate_id"].map(
                    lambda x: silver_candidates.get(x, {}).get("candidate_party")
                )
                df["silver_candidate_state"] = df["candidate_id"].map(
                    lambda x: silver_candidates.get(x, {}).get("candidate_state")
                )
                df["silver_candidate_district"] = df["candidate_id"].map(
                    lambda x: silver_candidates.get(x, {}).get("candidate_district")
                )

        return df

    def _resolve_contributor(self, row: pd.Series) -> int:
        """
        Resolve or create a gold contributor entity.

        Args:
            row: Silver contribution row

        Returns:
            Gold contributor ID
        """
        # Try to find existing contributor by name + location
        stmt = select(GoldContributor).where(
            GoldContributor.name == row["contributor_name"],
            GoldContributor.state == row.get("contributor_state"),
            GoldContributor.zip == row.get("contributor_zip"),
        )

        result = self.session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            return existing.id

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
        Resolve or create a gold committee entity using Silver committee data.

        Args:
            row: Silver contribution row (enriched with Silver committee data)

        Returns:
            Gold committee ID
        """
        committee_fec_id = str(row["committee_id"])

        # Try to find existing by FEC ID
        stmt = select(GoldCommittee).where(GoldCommittee.fec_committee_id == committee_fec_id)

        result = self.session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            return existing.id

        # Create new committee using Silver data
        committee = GoldCommittee(
            name=str(row.get("silver_committee_name", "UNKNOWN")),
            committee_type=self._map_committee_type(row.get("silver_committee_designation")),
            party=None,  # Party not in Silver committee (could add if needed)
            state=row.get("silver_committee_state"),
            city=row.get("silver_committee_city"),
            candidate_id=None,  # Will be linked after candidate resolution
            fec_committee_id=committee_fec_id,
            is_active=True,
        )

        self.session.add(committee)
        self.session.flush()

        return committee.id

    def _resolve_candidate(self, row: pd.Series) -> int | None:
        """
        Resolve or create candidate using Silver candidate data.

        Args:
            row: Silver contribution row (enriched with Silver candidate data)

        Returns:
            Gold candidate ID or None
        """
        # Check if we have candidate data from Silver enrichment
        candidate_fec_id = row.get("candidate_id")

        if pd.isna(candidate_fec_id) or not candidate_fec_id:
            return None

        candidate_fec_id = str(candidate_fec_id)

        # Try to find existing by FEC ID
        stmt = select(GoldCandidate).where(GoldCandidate.fec_candidate_id == candidate_fec_id)

        result = self.session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            return existing.id

        # Create new candidate using Silver data
        candidate = GoldCandidate(
            name=str(row.get("silver_candidate_name", "UNKNOWN")),
            office=self._map_office_type(row.get("silver_candidate_office")),
            state=row.get("silver_candidate_state"),
            district=row.get("silver_candidate_district"),
            party=row.get("silver_candidate_party"),
            fec_candidate_id=candidate_fec_id,
            is_active=True,
        )

        self.session.add(candidate)
        self.session.flush()

        return candidate.id

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

    def _map_office_type(self, office: str | None) -> str:
        """
        Map FEC office type to gold standard.

        Args:
            office: FEC office code (H, S, P)

        Returns:
            Standardized office type
        """
        if not office:
            return "UNKNOWN"

        type_mapping = {
            "H": "HOUSE",
            "S": "SENATE",
            "P": "PRESIDENT",
        }

        return type_mapping.get(office, "OTHER")

    def _get_election_year(self, row: pd.Series) -> int:
        """
        Determine election year from contribution date and cycle.

        Args:
            row: Silver contribution row

        Returns:
            Election year
        """
        # For two-year cycles, the election year is the even year
        cycle = cast(int, row["election_cycle"])
        return cycle if cycle % 2 == 0 else cycle + 1
