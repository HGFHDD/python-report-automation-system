"""
Data Cleaner - Clean and validate data
"""
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import numpy as np
from src.core.logger import logger


class DataCleaner:
    """
    Class for cleaning and validating data
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize DataCleaner with a DataFrame

        Args:
            data: Input DataFrame to clean
        """
        self.data = data.copy()
        self.original_shape = data.shape
        self.logger = logger
        self.cleaning_log = []

    def _log_change(self, operation: str, details: str) -> None:
        """Log cleaning operation"""
        entry = {"operation": operation, "details": details}
        self.cleaning_log.append(entry)
        self.logger.info(f"Data cleaning: {operation} - {details}")

    def remove_duplicates(self, subset: Optional[List[str]] = None,
                         keep: str = 'first') -> 'DataCleaner':
        """
        Remove duplicate rows

        Args:
            subset: Columns to consider for duplicates
            keep: Which duplicate to keep ('first', 'last', False)

        Returns:
            Self for chaining
        """
        before = len(self.data)
        self.data = self.data.drop_duplicates(subset=subset, keep=keep)
        removed = before - len(self.data)
        self._log_change("remove_duplicates", f"Removed {removed} duplicate rows")
        return self

    def remove_nulls(self, subset: Optional[List[str]] = None,
                    how: str = 'any') -> 'DataCleaner':
        """
        Remove rows with null values

        Args:
            subset: Columns to check for nulls
            how: 'any' or 'all'

        Returns:
            Self for chaining
        """
        before = len(self.data)
        self.data = self.data.dropna(subset=subset, how=how)
        removed = before - len(self.data)
        self._log_change("remove_nulls", f"Removed {removed} rows with null values")
        return self

    def fill_nulls(self, value: Any = None, method: str = None,
                  columns: Optional[List[str]] = None) -> 'DataCleaner':
        """
        Fill null values

        Args:
            value: Value to fill with
            method: Method to fill ('ffill', 'bfill', 'mean', 'median', 'mode')
            columns: Columns to fill

        Returns:
            Self for chaining
        """
        cols = columns or self.data.columns.tolist()

        for col in cols:
            if col not in self.data.columns:
                continue

            null_count = self.data[col].isnull().sum()
            if null_count == 0:
                continue

            if method == 'mean' and pd.api.types.is_numeric_dtype(self.data[col]):
                self.data[col] = self.data[col].fillna(self.data[col].mean())
            elif method == 'median' and pd.api.types.is_numeric_dtype(self.data[col]):
                self.data[col] = self.data[col].fillna(self.data[col].median())
            elif method == 'mode':
                mode_val = self.data[col].mode()
                if len(mode_val) > 0:
                    self.data[col] = self.data[col].fillna(mode_val[0])
            elif method in ['ffill', 'bfill']:
                self.data[col] = self.data[col].fillna(method=method)
            elif value is not None:
                self.data[col] = self.data[col].fillna(value)

            self._log_change("fill_nulls", f"Filled {null_count} nulls in '{col}'")

        return self

    def convert_types(self, type_mapping: Dict[str, str]) -> 'DataCleaner':
        """
        Convert column data types

        Args:
            type_mapping: Dictionary of column: type pairs

        Returns:
            Self for chaining
        """
        for col, dtype in type_mapping.items():
            if col not in self.data.columns:
                continue

            try:
                if dtype == 'datetime':
                    self.data[col] = pd.to_datetime(self.data[col])
                elif dtype == 'numeric':
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                else:
                    self.data[col] = self.data[col].astype(dtype)
                self._log_change("convert_types", f"Converted '{col}' to {dtype}")
            except Exception as e:
                self.logger.error(f"Failed to convert '{col}' to {dtype}: {str(e)}")

        return self

    def trim_strings(self, columns: Optional[List[str]] = None) -> 'DataCleaner':
        """
        Trim whitespace from string columns

        Args:
            columns: Columns to trim

        Returns:
            Self for chaining
        """
        string_cols = columns or self.data.select_dtypes(include=['object']).columns.tolist()

        for col in string_cols:
            if col in self.data.columns:
                self.data[col] = self.data[col].str.strip()

        self._log_change("trim_strings", f"Trimmed {len(string_cols)} string columns")
        return self

    def standardize_text(self, columns: List[str], case: str = 'lower') -> 'DataCleaner':
        """
        Standardize text columns

        Args:
            columns: Columns to standardize
            case: 'lower', 'upper', or 'title'

        Returns:
            Self for chaining
        """
        for col in columns:
            if col not in self.data.columns:
                continue

            if case == 'lower':
                self.data[col] = self.data[col].str.lower()
            elif case == 'upper':
                self.data[col] = self.data[col].str.upper()
            elif case == 'title':
                self.data[col] = self.data[col].str.title()

        self._log_change("standardize_text", f"Standardized {len(columns)} columns to {case}")
        return self

    def remove_outliers(self, columns: List[str], method: str = 'iqr',
                       threshold: float = 1.5) -> 'DataCleaner':
        """
        Remove outliers from numeric columns

        Args:
            columns: Columns to check for outliers
            method: 'iqr' or 'zscore'
            threshold: Threshold for outlier detection

        Returns:
            Self for chaining
        """
        before = len(self.data)

        for col in columns:
            if col not in self.data.columns or not pd.api.types.is_numeric_dtype(self.data[col]):
                continue

            if method == 'iqr':
                Q1 = self.data[col].quantile(0.25)
                Q3 = self.data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - threshold * IQR
                upper = Q3 + threshold * IQR
                self.data = self.data[(self.data[col] >= lower) & (self.data[col] <= upper)]
            elif method == 'zscore':
                mean = self.data[col].mean()
                std = self.data[col].std()
                self.data = self.data[np.abs((self.data[col] - mean) / std) <= threshold]

        removed = before - len(self.data)
        self._log_change("remove_outliers", f"Removed {removed} outlier rows")
        return self

    def rename_columns(self, mapping: Dict[str, str]) -> 'DataCleaner':
        """
        Rename columns

        Args:
            mapping: Dictionary of old_name: new_name

        Returns:
            Self for chaining
        """
        self.data = self.data.rename(columns=mapping)
        self._log_change("rename_columns", f"Renamed {len(mapping)} columns")
        return self

    def drop_columns(self, columns: List[str]) -> 'DataCleaner':
        """
        Drop specified columns

        Args:
            columns: Columns to drop

        Returns:
            Self for chaining
        """
        existing = [c for c in columns if c in self.data.columns]
        self.data = self.data.drop(columns=existing)
        self._log_change("drop_columns", f"Dropped {len(existing)} columns")
        return self

    def get_data(self) -> pd.DataFrame:
        """Get cleaned DataFrame"""
        return self.data

    def get_summary(self) -> Dict[str, Any]:
        """Get cleaning summary"""
        return {
            "original_shape": self.original_shape,
            "final_shape": self.data.shape,
            "rows_removed": self.original_shape[0] - self.data.shape[0],
            "operations": self.cleaning_log
        }
