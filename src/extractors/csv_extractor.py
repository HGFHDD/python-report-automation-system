"""
CSV File Data Extractor
"""
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from src.extractors.base import BaseExtractor


class CSVExtractor(BaseExtractor):
    """
    Extractor for CSV files
    """

    def __init__(self, file_path: Optional[str] = None, **kwargs):
        """
        Initialize CSV extractor

        Args:
            file_path: Path to the CSV file
            **kwargs: Additional pandas read_csv parameters
        """
        super().__init__(kwargs)
        self.file_path = Path(file_path) if file_path else None
        self.read_options = kwargs

    def connect(self) -> None:
        """Validate file exists"""
        if self.file_path and not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        self.logger.info(f"CSV file validated: {self.file_path}")

    def disconnect(self) -> None:
        """No connection to close for CSV files"""
        pass

    def extract(self, query: str = None, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Extract data from CSV file

        Args:
            query: File path (overrides constructor path)
            params: Optional read parameters

        Returns:
            DataFrame with CSV data
        """
        file_to_read = Path(query) if query else self.file_path
        
        if not file_to_read:
            raise ValueError("No file path provided")
        
        if not file_to_read.exists():
            raise FileNotFoundError(f"CSV file not found: {file_to_read}")

        read_params = {**self.read_options}
        if params:
            read_params.update(params)

        try:
            df = pd.read_csv(file_to_read, **read_params)
            self.logger.info(f"Successfully loaded CSV: {len(df)} rows from {file_to_read}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read CSV: {str(e)}")
            raise

    def extract_multiple(self, file_paths: List[str], concat: bool = True) -> pd.DataFrame:
        """
        Extract data from multiple CSV files

        Args:
            file_paths: List of file paths
            concat: If True, concatenate all files into one DataFrame

        Returns:
            DataFrame with combined data or dict of DataFrames
        """
        dataframes = []
        for path in file_paths:
            df = self.extract(path)
            dataframes.append(df)

        if concat:
            combined = pd.concat(dataframes, ignore_index=True)
            self.logger.info(f"Combined {len(file_paths)} CSV files: {len(combined)} total rows")
            return combined
        
        return dataframes

    def extract_with_filter(self, file_path: str, column: str, 
                           values: List[Any]) -> pd.DataFrame:
        """
        Extract and filter CSV data

        Args:
            file_path: Path to CSV file
            column: Column to filter on
            values: Values to filter by

        Returns:
            Filtered DataFrame
        """
        df = self.extract(file_path)
        filtered = df[df[column].isin(values)]
        self.logger.info(f"Filtered data: {len(filtered)} rows (from {len(df)})")
        return filtered

    def extract_date_range(self, file_path: str, date_column: str,
                          start_date: str, end_date: str) -> pd.DataFrame:
        """
        Extract CSV data within a date range

        Args:
            file_path: Path to CSV file
            date_column: Name of date column
            start_date: Start date string
            end_date: End date string

        Returns:
            DataFrame filtered by date range
        """
        df = self.extract(file_path)
        df[date_column] = pd.to_datetime(df[date_column])
        
        mask = (df[date_column] >= start_date) & (df[date_column] <= end_date)
        filtered = df[mask]
        
        self.logger.info(f"Date range filter: {len(filtered)} rows between {start_date} and {end_date}")
        return filtered
