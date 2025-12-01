"""Data loaders for all layers."""

from fund_lens_etl.loaders.bronze import (
    BronzeFECCandidateLoader,
    BronzeFECCommitteeLoader,
    BronzeFECScheduleALoader,
    BronzeMarylandCandidateLoader,
    BronzeMarylandCommitteeLoader,
    BronzeMarylandContributionLoader,
)
from fund_lens_etl.loaders.silver import (
    SilverMarylandCandidateLoader,
    SilverMarylandCommitteeLoader,
    SilverMarylandContributionLoader,
)

__all__ = [
    # Bronze FEC loaders
    "BronzeFECScheduleALoader",
    "BronzeFECCommitteeLoader",
    "BronzeFECCandidateLoader",
    # Bronze Maryland loaders
    "BronzeMarylandContributionLoader",
    "BronzeMarylandCommitteeLoader",
    "BronzeMarylandCandidateLoader",
    # Silver Maryland loaders
    "SilverMarylandContributionLoader",
    "SilverMarylandCommitteeLoader",
    "SilverMarylandCandidateLoader",
]
