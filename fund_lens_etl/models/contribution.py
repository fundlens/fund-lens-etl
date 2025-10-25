"""Contribution model"""
import sqlalchemy as sa
from sqlalchemy import Column, BigInteger, String, Integer, Date, Text, Float, Boolean, TIMESTAMP, ForeignKey, func, CheckConstraint
from sqlalchemy.orm import relationship

from fund_lens_etl.database import Base


class Contribution(Base):
    __tablename__ = "contributions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_table = Column(String(100), nullable=False)
    source_id = Column(BigInteger, nullable=False)
    contribution_date = Column(Date, nullable=False, index=True)
    amount_cents = Column(Integer, nullable=False, index=True)
    donor_name_raw = Column(String(200), nullable=False, index=True)
    donor_employer_raw = Column(String(200), index=True)
    donor_occupation_raw = Column(String(100))
    donor_city = Column(String(50))
    donor_state = Column(String(2))
    donor_zip = Column(String(9))

    donor_id = Column(BigInteger, ForeignKey("donors.id"), index=True)
    recipient_committee_id = Column(BigInteger, ForeignKey("committees.id"), nullable=False, index=True)
    recipient_candidate_id = Column(BigInteger, ForeignKey("candidates.id"), index=True)

    contribution_type = Column(String(20), nullable=False)  # individual, pac, party, self
    transaction_type = Column(String(3))  # FEC transaction type code
    election_type = Column(String(20))  # primary, general, runoff, special
    is_itemized = Column(Boolean, nullable=False, default=True)
    memo_text = Column(Text)

    data_quality_score = Column(Float, CheckConstraint('data_quality_score >= 0.0 AND data_quality_score <= 1.0'))
    needs_review = Column(Boolean, nullable=False, default=False, index=True)

    __table_args__ = (
        sa.UniqueConstraint('source_table', 'source_id', name='uq_contribution_source'),
    )

    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())

    donor = relationship("Donor", backref="contributions")
    committee = relationship("Committee", backref="contributions_received")
    candidate = relationship("Candidate", backref="contributions_received")

    def __repr__(self):
        return (
            f"<Contribution(id={self.id}, date={self.contribution_date}, amount_cents={self.amount_cents}, "
            f"donor='{self.donor_name_raw}')>"
        )
