"""
Backfill party and candidate_id fields in silver_fec_committee from bronze data.

This script updates existing silver_fec_committee records with:
- party: from bronze_fec_committee.party
- candidate_id: first value from bronze_fec_committee.candidate_ids array
"""

import logging
from sqlalchemy import select, update
from fund_lens_models.bronze.fec import BronzeFECCommittee
from fund_lens_models.silver.fec import SilverFECCommittee
from fund_lens_etl.database import get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def backfill_committee_fields():
    """Backfill party and candidate_id in silver_fec_committee from bronze."""

    with get_session() as session:
        # Get all silver committees
        silver_committees_stmt = select(SilverFECCommittee)
        silver_committees = session.execute(silver_committees_stmt).scalars().all()

        logger.info(f"Found {len(silver_committees)} silver committees to backfill")

        updated_count = 0
        skipped_count = 0

        for silver_committee in silver_committees:
            # Look up corresponding bronze committee
            bronze_stmt = select(BronzeFECCommittee).where(
                BronzeFECCommittee.committee_id == silver_committee.source_committee_id
            )
            bronze_committee = session.execute(bronze_stmt).scalar_one_or_none()

            if not bronze_committee:
                logger.warning(
                    f"No bronze record found for silver committee {silver_committee.source_committee_id}"
                )
                skipped_count += 1
                continue

            # Extract candidate_id from candidate_ids array (first element)
            candidate_id = None
            if bronze_committee.candidate_ids and len(bronze_committee.candidate_ids) > 0:
                candidate_id = bronze_committee.candidate_ids[0]

            # Update silver committee
            silver_committee.party = bronze_committee.party
            silver_committee.candidate_id = candidate_id

            updated_count += 1

            if updated_count % 100 == 0:
                logger.info(f"Updated {updated_count} committees...")
                session.commit()

        # Final commit
        session.commit()

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Updated: {updated_count}")
        logger.info(f"Skipped: {skipped_count}")
        logger.info("=" * 60)

        # Show some sample results
        sample_stmt = select(SilverFECCommittee).limit(5)
        samples = session.execute(sample_stmt).scalars().all()

        logger.info("\nSample updated records:")
        for committee in samples:
            logger.info(
                f"  {committee.name[:50]:50s} | Party: {committee.party or 'None':5s} | "
                f"Candidate: {committee.candidate_id or 'None'}"
            )


if __name__ == "__main__":
    backfill_committee_fields()
