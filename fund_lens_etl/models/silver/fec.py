"""Silver layer models - cleaned and standardized FEC data."""

from datetime import date

from sqlalchemy import Date, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from fund_lens_etl.models.base import Base, TimestampMixin


class SilverFECContribution(Base, TimestampMixin):
    """Cleaned and standardized FEC contribution data."""

    __tablename__ = "silver_fec_contribution"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Source reference
    source_sub_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    # Transaction identifiers
    transaction_id: Mapped[str] = mapped_column(String(255), nullable=False)
    file_number: Mapped[int | None] = mapped_column(Integer)
    amendment_indicator: Mapped[str | None] = mapped_column(String(10))

    # Contribution details (required fields)
    contribution_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    contribution_amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    contributor_aggregate_ytd: Mapped[float | None] = mapped_column(Numeric(12, 2))

    # Contributor information (cleaned)
    contributor_name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    contributor_first_name: Mapped[str | None] = mapped_column(String(255))
    contributor_last_name: Mapped[str | None] = mapped_column(String(255))
    contributor_city: Mapped[str | None] = mapped_column(String(255), index=True)
    contributor_state: Mapped[str | None] = mapped_column(String(2), index=True)
    contributor_zip: Mapped[str | None] = mapped_column(String(5))  # Cleaned to 5 digits
    contributor_employer: Mapped[str] = mapped_column(
        String(500), nullable=False, default="NOT PROVIDED"
    )
    contributor_occupation: Mapped[str] = mapped_column(
        String(255), nullable=False, default="NOT PROVIDED"
    )
    entity_type: Mapped[str] = mapped_column(String(10), nullable=False)  # IND, ORG, etc.

    # Committee information (recipient) - enriched from bronze_fec_committee
    committee_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    committee_name: Mapped[str | None] = mapped_column(String(500))
    committee_type: Mapped[str | None] = mapped_column(String(10))
    committee_designation: Mapped[str | None] = mapped_column(String(10))
    committee_party: Mapped[str | None] = mapped_column(String(10))  # NEW: enriched

    # Candidate information - enriched from bronze_fec_candidate via committee
    candidate_id: Mapped[str | None] = mapped_column(String(20), index=True)  # NEW: enriched
    candidate_name: Mapped[str | None] = mapped_column(String(500))  # NEW: enriched
    candidate_office: Mapped[str | None] = mapped_column(String(1))  # NEW: enriched (H, S, P)
    candidate_party: Mapped[str | None] = mapped_column(String(10))  # NEW: enriched

    # Transaction details
    receipt_type: Mapped[str | None] = mapped_column(String(10))
    election_type: Mapped[str | None] = mapped_column(String(10))
    memo_text: Mapped[str | None] = mapped_column(Text)

    # Metadata
    election_cycle: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    report_year: Mapped[int | None] = mapped_column(Integer)

    def __repr__(self) -> str:
        return f"<SilverFECContribution(id={self.id}, amount={self.contribution_amount}, date={self.contribution_date})>"


class SilverFECCandidate(Base, TimestampMixin):
    """Cleaned and standardized FEC candidate data."""

    __tablename__ = "silver_fec_candidate"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Source reference
    source_candidate_id: Mapped[str] = mapped_column(String(20), unique=True, index=True)

    # Candidate information (required)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    office: Mapped[str] = mapped_column(String(1), nullable=False)  # H, S, P
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    district: Mapped[str | None] = mapped_column(String(2))
    party: Mapped[str | None] = mapped_column(String(10))

    # Election info
    election_cycle: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    def __repr__(self) -> str:
        return f"<SilverFECCandidate(id={self.id}, name={self.name}, office={self.office})>"


class SilverFECCommittee(Base, TimestampMixin):
    """Cleaned and standardized FEC committee data."""

    __tablename__ = "silver_fec_committee"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Source reference
    source_committee_id: Mapped[str] = mapped_column(String(20), unique=True, index=True)

    # Committee information (required)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    committee_type: Mapped[str | None] = mapped_column(String(1))
    designation: Mapped[str | None] = mapped_column(String(1))

    # Location
    state: Mapped[str | None] = mapped_column(String(2), index=True)
    city: Mapped[str | None] = mapped_column(String(255))
    zip: Mapped[str | None] = mapped_column(String(5))  # Cleaned to 5 digits

    # Treasurer
    treasurer_name: Mapped[str | None] = mapped_column(String(500))

    # Status
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    election_cycle: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    def __repr__(self) -> str:
        return f"<SilverFECCommittee(id={self.id}, name={self.name})>"
