"""Donor model"""
from sqlalchemy import Column, BigInteger, String, Float, TIMESTAMP, func, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from fund_lens_etl.database import Base


class Donor(Base):
    __tablename__ = "donors"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    canonical_name = Column(String(200), nullable=False, index=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    employer_canonical = Column(String(200), index=True)
    occupation_canonical = Column(String(100))
    city = Column(String(50))
    state = Column(String(2), index=True)
    zip = Column(String(9))
    entity_resolution_confidence = Column(
        Float, CheckConstraint(
            'entity_resolution_confidence >= 0.0 AND entity_resolution_confidence <= 1.0')
        )
    aliases = Column(JSONB)

    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Donor(id={self.id}, name='{self.canonical_name}', employer='{self.employer_canonical}')>"
