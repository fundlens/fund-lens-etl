"""Check candidate_ids in bronze_fec_committee to understand the data."""

import logging
from sqlalchemy import select, func
from fund_lens_models.bronze.fec import BronzeFECCommittee
from fund_lens_etl.database import get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_candidate_ids():
    """Check candidate_ids distribution in bronze."""

    with get_session() as session:
        # Total committees
        total_stmt = select(func.count()).select_from(BronzeFECCommittee)
        total = session.execute(total_stmt).scalar()

        logger.info(f"Total bronze committees: {total}")

        # Committees with candidate_ids
        with_candidates_stmt = select(func.count()).select_from(BronzeFECCommittee).where(
            BronzeFECCommittee.candidate_ids.is_not(None)
        )
        with_candidates = session.execute(with_candidates_stmt).scalar()

        logger.info(f"Committees with candidate_ids: {with_candidates}")

        # Show some examples
        examples_stmt = select(BronzeFECCommittee).where(
            BronzeFECCommittee.candidate_ids.is_not(None)
        ).limit(10)
        examples = session.execute(examples_stmt).scalars().all()

        logger.info("\nSample committees with candidate_ids:")
        for committee in examples:
            logger.info(
                f"  {committee.name[:50]:50s} | Party: {committee.party or 'None':5s} | "
                f"Candidates: {committee.candidate_ids}"
            )

        # Check party distribution
        with_party_stmt = select(func.count()).select_from(BronzeFECCommittee).where(
            BronzeFECCommittee.party.is_not(None)
        )
        with_party = session.execute(with_party_stmt).scalar()

        logger.info(f"\nCommittees with party: {with_party}")


if __name__ == "__main__":
    check_candidate_ids()
