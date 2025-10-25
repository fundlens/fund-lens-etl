"""Raw Filing Model"""
from sqlalchemy import Column, BigInteger, String, Text, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import JSONB
from fund_lens_etl.database import Base


class RawFiling(Base):
    __tablename__ = "raw_filings"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False, index=True)
    file_url = Column(Text, nullable=False)
    file_hash = Column(String(64), nullable=False)
    raw_content = Column(JSONB, nullable=False)
    file_metadata = Column("metadata", JSONB)
    ingested_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

    def __repr__(self):
        return (
            f"<RawFiling(id={self.id}, source='{self.source}', hash='{self.file_hash[:8]}...', "
            f"ingested_at={self.ingested_at})>"
        )
