"""Base loader class for all loaders."""

from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy.orm import Session


class BaseLoader(ABC):
    """Abstract base class for loaders."""

    @abstractmethod
    def load(self, session: Session, df: pd.DataFrame, **kwargs) -> int:
        """
        Load data into database.

        Args:
            session: SQLAlchemy session for database operations
            df: DataFrame containing data to load
            **kwargs: Additional loader-specific parameters

        Returns:
            Number of records loaded
        """
        pass

    @abstractmethod
    def get_target_table(self) -> str:
        """
        Get the name of the target table.

        Returns:
            Table name (e.g., 'bronze_fec_schedule_a')
        """
        pass
