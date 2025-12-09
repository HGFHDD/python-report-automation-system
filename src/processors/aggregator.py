"""
Aggregator - Data aggregation operations
"""
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import numpy as np
from src.core.logger import logger


class Aggregator:
    """
    Class for performing data aggregations
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize Aggregator with a DataFrame

        Args:
            data: Input DataFrame
        """
        self.data = data.copy()
        self.logger = logger

    def group_by(self, columns: List[str], aggregations: Dict[str, Union[str, List[str]]],
                 as_index: bool = False) -> pd.DataFrame:
        """
        Perform group by aggregation

        Args:
            columns: Columns to group by
            aggregations: Column: aggregation function mapping
            as_index: Whether to use groups as index

        Returns:
            Aggregated DataFrame
        """
        result = self.data.groupby(columns, as_index=as_index).agg(aggregations)
        
        # Flatten column names if multi-level
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(col).strip() for col in result.columns.values]
        
        self.logger.info(f"Aggregated by {columns}: {len(result)} groups")
        return result

    def summarize(self, numeric_only: bool = True) -> pd.DataFrame:
        """
        Get summary statistics

        Args:
            numeric_only: Only include numeric columns

        Returns:
            Summary DataFrame
        """
        return self.data.describe(include='all' if not numeric_only else None)

    def calculate_totals(self, columns: List[str] = None) -> Dict[str, float]:
        """
        Calculate totals for numeric columns

        Args:
            columns: Specific columns (default: all numeric)

        Returns:
            Dictionary of column: total
        """
        if columns:
            numeric_cols = [c for c in columns if pd.api.types.is_numeric_dtype(self.data[c])]
        else:
            numeric_cols = self.data.select_dtypes(include=[np.number]).columns.tolist()

        totals = {col: self.data[col].sum() for col in numeric_cols}
        self.logger.info(f"Calculated totals for {len(totals)} columns")
        return totals

    def calculate_percentages(self, value_column: str, group_column: str = None) -> pd.DataFrame:
        """
        Calculate percentage distribution

        Args:
            value_column: Column to calculate percentages for
            group_column: Optional grouping column

        Returns:
            DataFrame with percentages
        """
        if group_column:
            result = self.data.groupby(group_column)[value_column].sum()
            result = (result / result.sum() * 100).reset_index()
            result.columns = [group_column, 'percentage']
        else:
            total = self.data[value_column].sum()
            result = self.data.copy()
            result['percentage'] = (result[value_column] / total * 100)

        return result

    def rolling_aggregation(self, column: str, window: int, func: str = 'mean',
                           date_column: str = None) -> pd.DataFrame:
        """
        Calculate rolling aggregation

        Args:
            column: Column to aggregate
            window: Window size
            func: Aggregation function ('mean', 'sum', 'min', 'max', 'std')
            date_column: Optional date column to sort by

        Returns:
            DataFrame with rolling values
        """
        result = self.data.copy()
        
        if date_column:
            result = result.sort_values(date_column)

        rolling = result[column].rolling(window=window)
        
        if func == 'mean':
            result[f'{column}_rolling_{func}'] = rolling.mean()
        elif func == 'sum':
            result[f'{column}_rolling_{func}'] = rolling.sum()
        elif func == 'min':
            result[f'{column}_rolling_{func}'] = rolling.min()
        elif func == 'max':
            result[f'{column}_rolling_{func}'] = rolling.max()
        elif func == 'std':
            result[f'{column}_rolling_{func}'] = rolling.std()

        return result

    def cumulative_sum(self, column: str, group_column: str = None) -> pd.DataFrame:
        """
        Calculate cumulative sum

        Args:
            column: Column to calculate cumsum for
            group_column: Optional grouping column

        Returns:
            DataFrame with cumulative sum
        """
        result = self.data.copy()

        if group_column:
            result[f'{column}_cumsum'] = result.groupby(group_column)[column].cumsum()
        else:
            result[f'{column}_cumsum'] = result[column].cumsum()

        return result

    def rank(self, column: str, ascending: bool = False,
            method: str = 'dense') -> pd.DataFrame:
        """
        Add rank column

        Args:
            column: Column to rank
            ascending: Rank order
            method: Ranking method ('average', 'min', 'max', 'first', 'dense')

        Returns:
            DataFrame with rank column
        """
        result = self.data.copy()
        result[f'{column}_rank'] = result[column].rank(ascending=ascending, method=method)
        return result

    def top_n(self, column: str, n: int = 10, ascending: bool = False) -> pd.DataFrame:
        """
        Get top N rows by column

        Args:
            column: Column to sort by
            n: Number of rows
            ascending: Sort order

        Returns:
            Top N DataFrame
        """
        return self.data.nlargest(n, column) if not ascending else self.data.nsmallest(n, column)

    def bottom_n(self, column: str, n: int = 10) -> pd.DataFrame:
        """
        Get bottom N rows by column

        Args:
            column: Column to sort by
            n: Number of rows

        Returns:
            Bottom N DataFrame
        """
        return self.data.nsmallest(n, column)

    def compare_periods(self, date_column: str, value_column: str,
                       current_period: str, previous_period: str) -> Dict[str, Any]:
        """
        Compare two time periods

        Args:
            date_column: Date column name
            value_column: Value column to compare
            current_period: Current period filter
            previous_period: Previous period filter

        Returns:
            Comparison dictionary
        """
        self.data[date_column] = pd.to_datetime(self.data[date_column])
        
        # This is a simplified example - actual implementation would need proper period parsing
        current = self.data[self.data[date_column] >= current_period][value_column].sum()
        previous = self.data[self.data[date_column] < current_period][value_column].sum()

        change = current - previous
        change_pct = (change / previous * 100) if previous != 0 else 0

        return {
            'current': current,
            'previous': previous,
            'change': change,
            'change_percentage': change_pct
        }

    def cross_tabulation(self, row_col: str, col_col: str,
                        value_col: str = None, aggfunc: str = 'count') -> pd.DataFrame:
        """
        Create cross tabulation

        Args:
            row_col: Row column
            col_col: Column column
            value_col: Value column (optional)
            aggfunc: Aggregation function

        Returns:
            Cross tabulation DataFrame
        """
        if value_col:
            return pd.crosstab(
                self.data[row_col],
                self.data[col_col],
                values=self.data[value_col],
                aggfunc=aggfunc
            )
        return pd.crosstab(self.data[row_col], self.data[col_col])
