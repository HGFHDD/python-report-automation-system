"""
Tests for Data Processors
"""

import pytest
import pandas as pd
import numpy as np


class TestDataCleaner:
    """Tests for Data Cleaner."""
    
    def test_remove_duplicates(self, sample_dataframe_with_duplicates):
        """Test duplicate removal."""
        from src.processors.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.remove_duplicates(sample_dataframe_with_duplicates)
        
        assert len(result) == 3
        assert result['id'].tolist() == [1, 2, 3]
    
    def test_remove_duplicates_subset(self, sample_dataframe_with_duplicates):
        """Test duplicate removal on specific columns."""
        from src.processors.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.remove_duplicates(
            sample_dataframe_with_duplicates, 
            subset=['name']
        )
        
        assert len(result) == 3
    
    def test_fill_nulls_with_value(self, sample_dataframe_with_nulls):
        """Test null filling with specific value."""
        from src.processors.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.fill_nulls(
            sample_dataframe_with_nulls, 
            value='Unknown'
        )
        
        assert result['name'].isnull().sum() == 0
        assert 'Unknown' in result['name'].values
    
    def test_fill_nulls_with_mean(self, sample_dataframe_with_nulls):
        """Test null filling with mean for numeric columns."""
        from src.processors.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.fill_nulls(
            sample_dataframe_with_nulls, 
            strategy='mean',
            columns=['value']
        )
        
        assert result['value'].isnull().sum() == 0
    
    def test_drop_nulls(self, sample_dataframe_with_nulls):
        """Test dropping rows with null values."""
        from src.processors.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        result = cleaner.drop_nulls(sample_dataframe_with_nulls)
        
        assert result.isnull().sum().sum() == 0
        assert len(result) == 2  # Only 2 rows without any nulls


class TestDataTransformer:
    """Tests for Data Transformer."""
    
    def test_aggregate_sum(self, sample_sales_data):
        """Test aggregation with sum."""
        from src.processors.data_transformer import DataTransformer
        
        transformer = DataTransformer()
        result = transformer.aggregate(
            sample_sales_data,
            groupby=['category'],
            agg_funcs={'total_amount': 'sum'}
        )
        
        assert 'category' in result.columns
        assert 'total_amount' in result.columns
        assert len(result) == 2  # Electronics, Clothing
    
    def test_aggregate_multiple_functions(self, sample_sales_data):
        """Test aggregation with multiple functions."""
        from src.processors.data_transformer import DataTransformer
        
        transformer = DataTransformer()
        result = transformer.aggregate(
            sample_sales_data,
            groupby=['product'],
            agg_funcs={
                'quantity': ['sum', 'mean'],
                'total_amount': 'sum'
            }
        )
        
        assert len(result) == 3  # A, B, C
    
    def test_filter_data(self, sample_dataframe):
        """Test data filtering."""
        from src.processors.data_transformer import DataTransformer
        
        transformer = DataTransformer()
        result = transformer.filter_data(
            sample_dataframe,
            conditions={'department': 'Sales'}
        )
        
        assert len(result) == 2
        assert all(result['department'] == 'Sales')
    
    def test_sort_data(self, sample_dataframe):
        """Test data sorting."""
        from src.processors.data_transformer import DataTransformer
        
        transformer = DataTransformer()
        result = transformer.sort_data(
            sample_dataframe,
            by=['salary'],
            ascending=False
        )
        
        assert result.iloc[0]['salary'] == 65000
        assert result.iloc[-1]['salary'] == 45000
    
    def test_pivot_data(self, sample_sales_data):
        """Test data pivoting."""
        from src.processors.data_transformer import DataTransformer
        
        transformer = DataTransformer()
        result = transformer.pivot(
            sample_sales_data,
            index='category',
            columns='product',
            values='total_amount',
            aggfunc='sum'
        )
        
        assert 'A' in result.columns or ('A',) in result.columns


class TestAggregator:
    """Tests for Aggregator."""
    
    def test_group_aggregate(self, sample_kpi_data):
        """Test group aggregation."""
        from src.processors.aggregator import Aggregator
        
        aggregator = Aggregator()
        result = aggregator.group_aggregate(
            sample_kpi_data,
            group_cols=['agent_id'],
            agg_config={
                'calls_handled': 'sum',
                'customer_satisfaction': 'mean'
            }
        )
        
        assert len(result) == 3  # 3 agents
        assert 'calls_handled' in result.columns
    
    def test_calculate_percentiles(self, sample_kpi_data):
        """Test percentile calculation."""
        from src.processors.aggregator import Aggregator
        
        aggregator = Aggregator()
        result = aggregator.calculate_percentiles(
            sample_kpi_data,
            column='sales_amount',
            percentiles=[25, 50, 75]
        )
        
        assert 'p25' in result
        assert 'p50' in result
        assert 'p75' in result


class TestKPICalculator:
    """Tests for KPI Calculator."""
    
    def test_calculate_growth_rate(self, sample_sales_data):
        """Test growth rate calculation."""
        from src.processors.kpi_calculator import KPICalculator
        
        calculator = KPICalculator()
        
        current = 1000
        previous = 800
        result = calculator.calculate_growth_rate(current, previous)
        
        assert result == pytest.approx(25.0, rel=0.01)
    
    def test_calculate_growth_rate_zero_previous(self):
        """Test growth rate with zero previous value."""
        from src.processors.kpi_calculator import KPICalculator
        
        calculator = KPICalculator()
        result = calculator.calculate_growth_rate(1000, 0)
        
        assert result == 0 or result is None or np.isinf(result)
    
    def test_calculate_average(self, sample_kpi_data):
        """Test average calculation."""
        from src.processors.kpi_calculator import KPICalculator
        
        calculator = KPICalculator()
        result = calculator.calculate_average(
            sample_kpi_data, 
            'customer_satisfaction'
        )
        
        expected = sample_kpi_data['customer_satisfaction'].mean()
        assert result == pytest.approx(expected, rel=0.01)
