"""
Base Extractor - Abstract class for all data extractors
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd
from src.core.logger import logger


class BaseExtractor(ABC):
    """
    Abstract base class for data extraction
    All extractors must inherit from this class
    """

    def __init__(self, connection_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the extractor

        Args:
            connection_config: Configuration dictionary for the connection
        """
        self.connection_config = connection_config or {}
        self._connection = None
        self.logger = logger

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the data source"""
        pass

    @abstractmethod
    def extract(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Extract data from the source

        Args:
            query: Query string or configuration for extraction
            params: Optional parameters for the query

        Returns:
            DataFrame with extracted data
        """
        pass

    def extract_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Convenience method to extract data with automatic connection management

        Args:
            query: Query string
            params: Optional parameters

        Returns:
            DataFrame with extracted data
        """
        try:
            self.connect()
            data = self.extract(query, params)
            self.logger.info(f"Successfully extracted {len(data)} rows")
            return data
        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            raise
        finally:
            self.disconnect()

    def validate_connection(self) -> bool:
        """
        Validate that the connection is working

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.connect()
            self.disconnect()
            return True
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
