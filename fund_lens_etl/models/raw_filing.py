"""Raw Filing Model"""

from datetime import datetime
from typing import Any

from sqlalchemy import TIMESTAMP, BigInteger, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from fund_lens_etl.database import Base


class RawFiling(Base):
    __tablename__ = "raw_filings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    file_url: Mapped[str] = mapped_column(Text, nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_content: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    file_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.now()
    )

    def __repr__(self):
        return (
            f"<RawFiling(id={self.id}, source='{self.source}', hash='{self.file_hash[:8]}...', "
            f"ingested_at={self.ingested_at})>"
        )
