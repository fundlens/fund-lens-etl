"""FEC Contribution Staging model"""

from sqlalchemy import (
    Column,
    BigInteger,
    String,
    Text,
    TIMESTAMP,
    Boolean,
    ForeignKey,
    func,
)
from fund_lens_etl.database import Base


class FECContributionStaging(Base):
    __tablename__ = "fec_contributions_staging"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_filing_id = Column(
        BigInteger, ForeignKey("raw_filings.id"), nullable=False, index=True
    )
    cmte_id = Column(String(9))
    amndt_ind = Column(String(1))
    rpt_tp = Column(String(3))
    transaction_pgi = Column(String(5))
    image_num = Column(String(18))
    transaction_tp = Column(String(3))
    entity_tp = Column(String(3))
    name = Column(Text)
    city = Column(String(30))
    state = Column(String(2))
    zip_code = Column(String(9))
    employer = Column(Text)
    occupation = Column(Text)
    transaction_dt = Column(String(8))
    transaction_amt = Column(String(14))
    other_id = Column(String(9))
    tran_id = Column(String(32))
    file_num = Column(String(22))
    memo_cd = Column(String(1))
    memo_text = Column(Text)
    sub_id = Column(String(19))

    ingested_at = Column(TIMESTAMP, nullable=False, server_default=func.now())
    standardized = Column(Boolean, nullable=False, default=False, index=True)

    def __repr__(self):
        return (
            f"<FECContributionStaging(id={self.id}, cmte_id='{self.cmte_id}', name='{self.name}', "
            f"standardized={self.standardized})>"
        )
