"""
Backfill candidate_id in gold_committee from silver_fec_committee data.

This script updates existing gold_committee records with:
- candidate_id: resolved from silver_fec_committee.candidate_id -> gold_candidate
- party: from silver_fec_committee.party (if not already set)
"""

import logging
from sqlalchemy import select
from fund_lens_models.gold import GoldCommittee, GoldCandidate
from fund_lens_models.silver.fec import SilverFECCommittee
from fund_lens_etl.database import get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def backfill_gold_committee_fields():
    """Backfill candidate_id and party in gold_committee from silver."""

    with get_session() as session:
        # Get all gold committees
        gold_committees_stmt = select(GoldCommittee)
        gold_committees = session.execute(gold_committees_stmt).scalars().all()

        logger.info(f"Found {len(gold_committees)} gold committees to backfill")

        updated_count = 0
        skipped_count = 0
        no_silver_count = 0
        no_candidate_count = 0

        for gold_committee in gold_committees:
            # Look up corresponding silver committee by FEC ID
            silver_stmt = select(SilverFECCommittee).where(
                SilverFECCommittee.source_committee_id == gold_committee.fec_committee_id
            )
            silver_committee = session.execute(silver_stmt).scalar_one_or_none()

            if not silver_committee:
                logger.warning(
                    f"No silver record found for gold committee {gold_committee.fec_committee_id}"
                )
                no_silver_count += 1
                continue

            # Update party if needed
            if not gold_committee.party and silver_committee.party:
                gold_committee.party = silver_committee.party

            # Resolve candidate_id from silver to gold
            if silver_committee.candidate_id:
                # Look up gold candidate by FEC ID
                candidate_stmt = select(GoldCandidate).where(
                    GoldCandidate.fec_candidate_id == silver_committee.candidate_id
                )
                gold_candidate = session.execute(candidate_stmt).scalar_one_or_none()

                if gold_candidate:
                    gold_committee.candidate_id = gold_candidate.id
                    updated_count += 1
                else:
                    logger.debug(
                        f"No gold candidate found for FEC ID {silver_committee.candidate_id}"
                    )
                    no_candidate_count += 1
            else:
                skipped_count += 1

            if updated_count % 100 == 0 and updated_count > 0:
                logger.info(f"Updated {updated_count} committees...")
                session.commit()

        # Final commit
        session.commit()

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Updated with candidate_id: {updated_count}")
        logger.info(f"No candidate in silver: {skipped_count}")
        logger.info(f"Candidate not found in gold: {no_candidate_count}")
        logger.info(f"No silver committee found: {no_silver_count}")
        logger.info("=" * 60)

        # Show some sample results
        sample_stmt = (
            select(GoldCommittee)
            .where(GoldCommittee.candidate_id.is_not(None))
            .limit(10)
        )
        samples = session.execute(sample_stmt).scalars().all()

        logger.info("\nSample committees with candidate_id:")
        for committee in samples:
            # Get candidate name
            if committee.candidate_id:
                candidate_stmt = select(GoldCandidate).where(
                    GoldCandidate.id == committee.candidate_id
                )
                candidate = session.execute(candidate_stmt).scalar_one_or_none()
                candidate_name = candidate.name if candidate else "Unknown"
            else:
                candidate_name = "None"

            logger.info(
                f"  {committee.name[:40]:40s} | Party: {committee.party or 'None':5s} | "
                f"Candidate: {candidate_name[:30]}"
            )


if __name__ == "__main__":
    backfill_gold_committee_fields()
