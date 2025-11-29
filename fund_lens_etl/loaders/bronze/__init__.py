"""Bronze layer loaders."""

from fund_lens_etl.loaders.bronze.fec import (
    BronzeFECCandidateLoader,
    BronzeFECCommitteeLoader,
    BronzeFECScheduleALoader,
)
from fund_lens_etl.loaders.bronze.maryland import (
    BronzeMarylandCandidateLoader,
    BronzeMarylandCommitteeLoader,
    BronzeMarylandContributionLoader,
)

__all__ = [
    # FEC loaders
    "BronzeFECScheduleALoader",
    "BronzeFECCommitteeLoader",
    "BronzeFECCandidateLoader",
    # Maryland loaders
    "BronzeMarylandContributionLoader",
    "BronzeMarylandCommitteeLoader",
    "BronzeMarylandCandidateLoader",
]
