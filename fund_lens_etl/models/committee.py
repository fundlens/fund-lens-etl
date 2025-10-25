"""Committee model"""

import sqlalchemy as sa
from sqlalchemy import Column, BigInteger, String, TIMESTAMP, func

from fund_lens_etl.database import Base


class Committee(Base):
    __tablename__ = "committees"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fec_committee_id = Column(String(9), nullable=True, index=True)
    data_source = Column(String(50), nullable=False, default="fec", index=True)
    source_committee_id = Column(String(50), nullable=False)
    committee_name = Column(String(200), nullable=False)
    committee_type = Column(String(1))
    designation = Column(String(1))
    party_affiliation = Column(String(3))
    state = Column(String(2))

    __table_args__ = (
        sa.UniqueConstraint(
            "data_source", "source_committee_id", name="uq_committee_source_id"
        ),
    )

    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self):
        return f"<Committee(id={self.id}, fec_id='{self.fec_committee_id}', name='{self.committee_name}')>"
