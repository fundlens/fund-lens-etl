"""Extractors for various data sources."""

from fund_lens_etl.extractors.fec import (
    FECCandidateExtractor,
    FECCommitteeExtractor,
    FECScheduleAExtractor,
)

__all__ = [
    "FECScheduleAExtractor",
    "FECCommitteeExtractor",
    "FECCandidateExtractor",
]
