"""
KPI Calculator - Calculate business metrics and KPIs
"""
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.core.logger import logger


class KPICalculator:
    """
    Class for calculating business KPIs and metrics
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize KPICalculator with a DataFrame

        Args:
            data: Input DataFrame with business data
        """
        self.data = data.copy()
        self.logger = logger
        self.kpis = {}

    def calculate_all(self) -> Dict[str, Any]:
        """
        Calculate all defined KPIs

        Returns:
            Dictionary with all KPI values
        """
        return self.kpis

    def add_kpi(self, name: str, value: Any, category: str = 'general') -> None:
        """
        Add a calculated KPI

        Args:
            name: KPI name
            value: KPI value
            category: KPI category
        """
        if category not in self.kpis:
            self.kpis[category] = {}
        self.kpis[category][name] = value
        self.logger.info(f"KPI calculated: {name} = {value}")

    # Sales KPIs
    def total_revenue(self, amount_column: str) -> float:
        """Calculate total revenue"""
        value = self.data[amount_column].sum()
        self.add_kpi('total_revenue', value, 'sales')
        return value

    def average_order_value(self, amount_column: str, order_column: str = None) -> float:
        """Calculate average order value"""
        if order_column:
            value = self.data.groupby(order_column)[amount_column].sum().mean()
        else:
            value = self.data[amount_column].mean()
        self.add_kpi('average_order_value', value, 'sales')
        return value

    def growth_rate(self, current: float, previous: float) -> float:
        """Calculate growth rate percentage"""
        if previous == 0:
            return 0
        value = ((current - previous) / previous) * 100
        self.add_kpi('growth_rate', value, 'sales')
        return value

    def conversion_rate(self, conversions: int, total: int) -> float:
        """Calculate conversion rate"""
        if total == 0:
            return 0
        value = (conversions / total) * 100
        self.add_kpi('conversion_rate', value, 'sales')
        return value

    # Performance KPIs
    def average_handling_time(self, time_column: str) -> float:
        """Calculate average handling time"""
        value = self.data[time_column].mean()
        self.add_kpi('average_handling_time', value, 'performance')
        return value

    def first_call_resolution(self, resolved_column: str, total_column: str = None) -> float:
        """Calculate first call resolution rate"""
        resolved = self.data[resolved_column].sum()
        total = len(self.data) if total_column is None else self.data[total_column].sum()
        value = (resolved / total * 100) if total > 0 else 0
        self.add_kpi('first_call_resolution', value, 'performance')
        return value

    def productivity_rate(self, output_column: str, hours_column: str) -> float:
        """Calculate productivity rate"""
        total_output = self.data[output_column].sum()
        total_hours = self.data[hours_column].sum()
        value = total_output / total_hours if total_hours > 0 else 0
        self.add_kpi('productivity_rate', value, 'performance')
        return value

    # Customer KPIs
    def net_promoter_score(self, score_column: str) -> float:
        """
        Calculate Net Promoter Score (NPS)
        Promoters (9-10) - Detractors (0-6)
        """
        promoters = len(self.data[self.data[score_column] >= 9])
        detractors = len(self.data[self.data[score_column] <= 6])
        total = len(self.data)
        
        if total == 0:
            return 0
        
        value = ((promoters - detractors) / total) * 100
        self.add_kpi('net_promoter_score', value, 'customer')
        return value

    def customer_satisfaction_score(self, score_column: str, max_score: float = 5) -> float:
        """Calculate customer satisfaction score (CSAT)"""
        value = (self.data[score_column].mean() / max_score) * 100
        self.add_kpi('customer_satisfaction_score', value, 'customer')
        return value

    def churn_rate(self, churned_column: str) -> float:
        """Calculate churn rate"""
        churned = self.data[churned_column].sum()
        total = len(self.data)
        value = (churned / total * 100) if total > 0 else 0
        self.add_kpi('churn_rate', value, 'customer')
        return value

    def retention_rate(self, retained_column: str) -> float:
        """Calculate retention rate"""
        retained = self.data[retained_column].sum()
        total = len(self.data)
        value = (retained / total * 100) if total > 0 else 0
        self.add_kpi('retention_rate', value, 'customer')
        return value

    # Training KPIs
    def training_completion_rate(self, completed_column: str) -> float:
        """Calculate training completion rate"""
        completed = self.data[completed_column].sum()
        total = len(self.data)
        value = (completed / total * 100) if total > 0 else 0
        self.add_kpi('training_completion_rate', value, 'training')
        return value

    def average_assessment_score(self, score_column: str) -> float:
        """Calculate average assessment score"""
        value = self.data[score_column].mean()
        self.add_kpi('average_assessment_score', value, 'training')
        return value

    def training_hours_per_employee(self, hours_column: str, employee_column: str) -> float:
        """Calculate average training hours per employee"""
        total_hours = self.data[hours_column].sum()
        unique_employees = self.data[employee_column].nunique()
        value = total_hours / unique_employees if unique_employees > 0 else 0
        self.add_kpi('training_hours_per_employee', value, 'training')
        return value

    def certification_rate(self, certified_column: str) -> float:
        """Calculate certification rate"""
        certified = self.data[certified_column].sum()
        total = len(self.data)
        value = (certified / total * 100) if total > 0 else 0
        self.add_kpi('certification_rate', value, 'training')
        return value

    # Utility methods
    def compare_kpis(self, current_data: pd.DataFrame, previous_data: pd.DataFrame,
                    kpi_configs: List[Dict]) -> Dict[str, Dict]:
        """
        Compare KPIs between two periods

        Args:
            current_data: Current period data
            previous_data: Previous period data
            kpi_configs: List of KPI configurations

        Returns:
            Dictionary with KPI comparisons
        """
        comparisons = {}
        
        for config in kpi_configs:
            name = config['name']
            func = config['function']
            params = config.get('params', {})
            
            # Calculate current
            self.data = current_data
            current_value = getattr(self, func)(**params)
            
            # Calculate previous
            self.data = previous_data
            previous_value = getattr(self, func)(**params)
            
            change = current_value - previous_value
            change_pct = (change / previous_value * 100) if previous_value != 0 else 0
            
            comparisons[name] = {
                'current': current_value,
                'previous': previous_value,
                'change': change,
                'change_percentage': change_pct,
                'trend': 'up' if change > 0 else 'down' if change < 0 else 'stable'
            }
        
        return comparisons

    def get_summary(self) -> pd.DataFrame:
        """Get KPI summary as DataFrame"""
        rows = []
        for category, kpis in self.kpis.items():
            for name, value in kpis.items():
                rows.append({
                    'category': category,
                    'kpi_name': name,
                    'value': value
                })
        return pd.DataFrame(rows)
