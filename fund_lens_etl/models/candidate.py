"""Canditate model"""
import sqlalchemy as sa
from sqlalchemy import Column, BigInteger, String, Integer, TIMESTAMP, ForeignKey, func
from sqlalchemy.orm import relationship

from fund_lens_etl.database import Base


class Candidate(Base):
    __tablename__ = "candidates"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fec_candidate_id = Column(String(9), nullable=True, index=True)
    data_source = Column(String(50), nullable=False, default='fec', index=True)
    source_candidate_id = Column(String(50), nullable=False)
    office_level = Column(String(20), nullable=False, default='federal')
    candidate_name = Column(String(200), nullable=False)
    party = Column(String(3))
    office = Column(String(1), nullable=False)
    state = Column(String(2), nullable=False)
    district = Column(String(2))
    election_year = Column(Integer, nullable=False)

    committee_id = Column(BigInteger, ForeignKey("committees.id"), index=True)

    committee = relationship("Committee", backref="candidates")

    __table_args__ = (
        sa.UniqueConstraint('data_source', 'source_candidate_id', name='uq_candidate_source_id'),
    )

    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())
