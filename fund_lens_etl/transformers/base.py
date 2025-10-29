"""Base transformer class for all data transformations."""

from abc import ABC, abstractmethod

import pandas as pd


class BaseTransformer(ABC):
    """Abstract base class for transformers."""

    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform data from one layer to another.

        Args:
            df: Input DataFrame
            **kwargs: Additional transformation parameters

        Returns:
            Transformed DataFrame
        """
        pass

    @abstractmethod
    def get_source_layer(self) -> str:
        """
        Get the source layer name.

        Returns:
            Source layer (e.g., 'bronze', 'silver')
        """
        pass

    @abstractmethod
    def get_target_layer(self) -> str:
        """
        Get the target layer name.

        Returns:
            Target layer (e.g., 'silver', 'gold')
        """
        pass
