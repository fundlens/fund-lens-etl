"""Verify that committee-to-candidate links are properly established."""

import logging
from sqlalchemy import select, func
from fund_lens_models.gold import GoldCommittee, GoldCandidate, GoldContribution
from fund_lens_etl.database import get_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_links():
    """Verify committee-candidate links across the gold layer."""

    with get_session() as session:
        logger.info("=" * 70)
        logger.info("GOLD LAYER COMMITTEE-CANDIDATE LINK VERIFICATION")
        logger.info("=" * 70)

        # Total committees
        total_committees = session.execute(
            select(func.count()).select_from(GoldCommittee)
        ).scalar()
        logger.info(f"\nTotal committees: {total_committees}")

        # Committees with candidate_id
        with_candidate = session.execute(
            select(func.count()).select_from(GoldCommittee).where(
                GoldCommittee.candidate_id.is_not(None)
            )
        ).scalar()
        logger.info(f"Committees with candidate_id: {with_candidate} ({with_candidate/total_committees*100:.1f}%)")

        # Committees with party
        with_party = session.execute(
            select(func.count()).select_from(GoldCommittee).where(
                GoldCommittee.party.is_not(None)
            )
        ).scalar()
        logger.info(f"Committees with party: {with_party} ({with_party/total_committees*100:.1f}%)")

        # Verify contributions can resolve both committee and candidate
        logger.info("\n" + "=" * 70)
        logger.info("CONTRIBUTION LINK VERIFICATION")
        logger.info("=" * 70)

        # Sample contribution with full resolution
        stmt = (
            select(GoldContribution, GoldCommittee, GoldCandidate)
            .join(GoldCommittee, GoldContribution.recipient_committee_id == GoldCommittee.id)
            .join(GoldCandidate, GoldContribution.recipient_candidate_id == GoldCandidate.id)
            .where(GoldCommittee.candidate_id.is_not(None))
            .limit(5)
        )
        results = session.execute(stmt).all()

        logger.info("\nSample contributions with full committee→candidate link:")
        for contribution, committee, candidate in results:
            logger.info(f"\n  Contribution: ${contribution.amount:.2f}")
            logger.info(f"  → Committee: {committee.name[:50]}")
            logger.info(f"     - Party: {committee.party}")
            logger.info(f"     - candidate_id: {committee.candidate_id}")
            logger.info(f"  → Candidate: {candidate.name}")
            logger.info(f"     - Office: {candidate.office}")
            logger.info(f"     - Party: {candidate.party}")

            # Verify the link
            if committee.candidate_id == candidate.id:
                logger.info(f"  ✓ Committee.candidate_id matches Candidate.id")
            else:
                logger.info(f"  ✗ MISMATCH: Committee.candidate_id={committee.candidate_id} != Candidate.id={candidate.id}")

        # Check for any inconsistencies
        logger.info("\n" + "=" * 70)
        logger.info("CONSISTENCY CHECKS")
        logger.info("=" * 70)

        # Contributions where recipient_candidate_id is set but committee has no candidate_id
        mismatch_stmt = (
            select(func.count())
            .select_from(GoldContribution)
            .join(GoldCommittee, GoldContribution.recipient_committee_id == GoldCommittee.id)
            .where(
                GoldContribution.recipient_candidate_id.is_not(None),
                GoldCommittee.candidate_id.is_(None)
            )
        )
        mismatches = session.execute(mismatch_stmt).scalar()
        logger.info(f"\nContributions with candidate but committee has no candidate_id: {mismatches}")

        if mismatches > 0:
            logger.info("  (This is expected for some cases where committees support multiple candidates)")

        logger.info("\n" + "=" * 70)
        logger.info("VERIFICATION COMPLETE")
        logger.info("=" * 70)


if __name__ == "__main__":
    verify_links()
