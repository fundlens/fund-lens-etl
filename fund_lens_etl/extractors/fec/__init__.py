"""FEC extractors for campaign finance data."""

from fund_lens_etl.extractors.fec.candidates import FECCandidateExtractor
from fund_lens_etl.extractors.fec.committees import FECCommitteeExtractor
from fund_lens_etl.extractors.fec.schedule_a import FECScheduleAExtractor

__all__ = [
    "FECScheduleAExtractor",
    "FECCommitteeExtractor",
    "FECCandidateExtractor",
]
