"""Base extractor class for all data sources."""

from abc import ABC, abstractmethod

import pandas as pd


class BaseExtractor(ABC):
    """Abstract base class for extractors."""

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from source.

        Returns:
            DataFrame containing extracted data.
        """
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        """
        Get the name of the data source.

        Returns:
            Source system identifier (e.g., 'FEC', 'MD_STATE').
        """
        pass
