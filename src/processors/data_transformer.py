"""
Data Transformer - Transform and reshape data
"""
from typing import Any, Callable, Dict, List, Optional, Union
import pandas as pd
import numpy as np
from src.core.logger import logger


class DataTransformer:
    """
    Class for transforming and reshaping data
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize DataTransformer with a DataFrame

        Args:
            data: Input DataFrame to transform
        """
        self.data = data.copy()
        self.logger = logger
        self.transformations = []

    def _log_transform(self, name: str, details: str) -> None:
        """Log transformation operation"""
        self.transformations.append({"name": name, "details": details})
        self.logger.info(f"Transformation: {name} - {details}")

    def apply_transformations(self, transformations: List[Dict] = None) -> pd.DataFrame:
        """
        Apply a list of transformations from configuration

        Args:
            transformations: List of transformation configs

        Returns:
            Transformed DataFrame
        """
        if not transformations:
            return self.data

        for transform in transformations:
            transform_type = transform.get('type')
            
            if transform_type == 'aggregate':
                groupby = transform.get('groupby', [])
                metrics = transform.get('metrics', {})
                self.aggregate(groupby, metrics)
            elif transform_type == 'sort':
                by = transform.get('by')
                ascending = transform.get('ascending', True)
                self.sort(by, ascending)
            elif transform_type == 'filter':
                column = transform.get('column')
                condition = transform.get('condition')
                value = transform.get('value')
                self.filter(column, condition, value)

        return self.data

    def add_column(self, name: str, value: Any = None,
                  formula: Callable = None) -> 'DataTransformer':
        """
        Add a new column

        Args:
            name: Column name
            value: Static value or series
            formula: Function to calculate values

        Returns:
            Self for chaining
        """
        if formula:
            self.data[name] = self.data.apply(formula, axis=1)
        else:
            self.data[name] = value
        
        self._log_transform("add_column", f"Added column '{name}'")
        return self

    def calculate_column(self, name: str, expression: str) -> 'DataTransformer':
        """
        Calculate a new column using an expression

        Args:
            name: New column name
            expression: Expression string (e.g., "col1 + col2")

        Returns:
            Self for chaining
        """
        try:
            self.data[name] = self.data.eval(expression)
            self._log_transform("calculate_column", f"Calculated '{name}' = {expression}")
        except Exception as e:
            self.logger.error(f"Failed to calculate column: {str(e)}")
        return self

    def aggregate(self, groupby: List[str], 
                 aggregations: Dict[str, Union[str, List[str]]]) -> 'DataTransformer':
        """
        Aggregate data by groups

        Args:
            groupby: Columns to group by
            aggregations: Column: aggregation function mapping

        Returns:
            Self for chaining
        """
        self.data = self.data.groupby(groupby, as_index=False).agg(aggregations)
        self._log_transform("aggregate", f"Grouped by {groupby}")
        return self

    def pivot(self, index: str, columns: str, values: str,
             aggfunc: str = 'sum') -> 'DataTransformer':
        """
        Create pivot table

        Args:
            index: Index column
            columns: Column for pivot columns
            values: Values column
            aggfunc: Aggregation function

        Returns:
            Self for chaining
        """
        self.data = pd.pivot_table(
            self.data,
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc
        ).reset_index()
        
        self._log_transform("pivot", f"Pivoted on '{columns}'")
        return self

    def melt(self, id_vars: List[str], value_vars: List[str] = None,
            var_name: str = 'variable', value_name: str = 'value') -> 'DataTransformer':
        """
        Unpivot data from wide to long format

        Args:
            id_vars: Identifier columns
            value_vars: Columns to unpivot
            var_name: Name for variable column
            value_name: Name for value column

        Returns:
            Self for chaining
        """
        self.data = pd.melt(
            self.data,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name
        )
        self._log_transform("melt", f"Unpivoted {len(value_vars or [])} columns")
        return self

    def sort(self, by: Union[str, List[str]], ascending: bool = True) -> 'DataTransformer':
        """
        Sort data

        Args:
            by: Column(s) to sort by
            ascending: Sort order

        Returns:
            Self for chaining
        """
        self.data = self.data.sort_values(by=by, ascending=ascending)
        self._log_transform("sort", f"Sorted by '{by}'")
        return self

    def filter(self, column: str, condition: str, value: Any) -> 'DataTransformer':
        """
        Filter data based on condition

        Args:
            column: Column to filter
            condition: Condition type ('eq', 'ne', 'gt', 'lt', 'gte', 'lte', 'in', 'contains')
            value: Filter value

        Returns:
            Self for chaining
        """
        before = len(self.data)

        if condition == 'eq':
            self.data = self.data[self.data[column] == value]
        elif condition == 'ne':
            self.data = self.data[self.data[column] != value]
        elif condition == 'gt':
            self.data = self.data[self.data[column] > value]
        elif condition == 'lt':
            self.data = self.data[self.data[column] < value]
        elif condition == 'gte':
            self.data = self.data[self.data[column] >= value]
        elif condition == 'lte':
            self.data = self.data[self.data[column] <= value]
        elif condition == 'in':
            self.data = self.data[self.data[column].isin(value)]
        elif condition == 'contains':
            self.data = self.data[self.data[column].str.contains(value, na=False)]

        filtered = before - len(self.data)
        self._log_transform("filter", f"Filtered '{column}' {condition} {value}: {filtered} rows removed")
        return self

    def join(self, other: pd.DataFrame, on: Union[str, List[str]],
            how: str = 'left') -> 'DataTransformer':
        """
        Join with another DataFrame

        Args:
            other: DataFrame to join
            on: Column(s) to join on
            how: Join type ('left', 'right', 'inner', 'outer')

        Returns:
            Self for chaining
        """
        self.data = self.data.merge(other, on=on, how=how)
        self._log_transform("join", f"Joined on '{on}' ({how})")
        return self

    def apply_function(self, column: str, func: Callable,
                      new_column: str = None) -> 'DataTransformer':
        """
        Apply function to column

        Args:
            column: Column to apply function to
            func: Function to apply
            new_column: Optional new column name

        Returns:
            Self for chaining
        """
        target = new_column or column
        self.data[target] = self.data[column].apply(func)
        self._log_transform("apply_function", f"Applied function to '{column}'")
        return self

    def resample_time(self, date_column: str, freq: str,
                     agg_func: str = 'sum') -> 'DataTransformer':
        """
        Resample time series data

        Args:
            date_column: Date column name
            freq: Frequency ('D', 'W', 'M', 'Q', 'Y')
            agg_func: Aggregation function

        Returns:
            Self for chaining
        """
        self.data[date_column] = pd.to_datetime(self.data[date_column])
        self.data = self.data.set_index(date_column).resample(freq).agg(agg_func).reset_index()
        self._log_transform("resample_time", f"Resampled to {freq}")
        return self

    def get_data(self) -> pd.DataFrame:
        """Get transformed DataFrame"""
        return self.data

    def get_transformations(self) -> List[Dict]:
        """Get list of applied transformations"""
        return self.transformations
