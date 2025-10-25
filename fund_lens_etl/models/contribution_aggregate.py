"""Contribution Aggregate model"""

import sqlalchemy as sa
from sqlalchemy import (
    Column,
    BigInteger,
    String,
    Integer,
    Date,
    TIMESTAMP,
    ForeignKey,
    func,
)
from sqlalchemy.orm import relationship
from fund_lens_etl.database import Base


class ContributionAggregate(Base):
    __tablename__ = "contribution_aggregates"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    candidate_id = Column(
        BigInteger, ForeignKey("candidates.id"), nullable=False, index=True
    )
    aggregation_period = Column(String(20), nullable=False)  # cycle, quarter, month
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    total_raised_cents = Column(Integer, nullable=False, default=0)
    total_spent_cents = Column(Integer, nullable=False, default=0)
    total_cash_on_hand_cents = Column(Integer, nullable=False, default=0)
    total_debts_cents = Column(Integer, nullable=False, default=0)
    individual_contributions_count = Column(Integer, nullable=False, default=0)
    individual_contributions_cents = Column(Integer, nullable=False, default=0)
    pac_contributions_count = Column(Integer, nullable=False, default=0)
    pac_contributions_cents = Column(Integer, nullable=False, default=0)
    party_contributions_count = Column(Integer, nullable=False, default=0)
    party_contributions_cents = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        sa.UniqueConstraint(
            "candidate_id",
            "aggregation_period",
            "period_start",
            name="uq_aggregate_period",
        ),
    )

    calculated_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

    candidate = relationship("Candidate", backref="aggregates")

    def __repr__(self):
        return (
            f"<ContributionAggregate(id={self.id}, candidate_id={self.candidate_id}, "
            f"period='{self.aggregation_period}', raised_cents={self.total_raised_cents})>"
        )
