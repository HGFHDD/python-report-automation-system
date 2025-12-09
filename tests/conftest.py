"""
Pytest Configuration and Fixtures
"""

import os
import sys
import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'department': ['Sales', 'IT', 'Sales', 'HR', 'IT'],
        'salary': [50000, 60000, 55000, 45000, 65000],
        'hire_date': pd.to_datetime([
            '2020-01-15', '2019-06-20', '2021-03-10',
            '2018-11-05', '2022-07-01'
        ]),
        'active': [True, True, False, True, True]
    })


@pytest.fixture
def sample_sales_data():
    """Create sample sales data for testing."""
    return pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=30, freq='D'),
        'product': ['A', 'B', 'C'] * 10,
        'category': ['Electronics', 'Clothing', 'Electronics'] * 10,
        'quantity': [10, 5, 8] * 10,
        'unit_price': [100.0, 50.0, 75.0] * 10,
        'total_amount': [1000.0, 250.0, 600.0] * 10,
        'region': ['North', 'South', 'East'] * 10
    })


@pytest.fixture
def sample_kpi_data():
    """Create sample KPI data for testing."""
    return pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=7, freq='D'),
        'agent_id': [1, 2, 3, 1, 2, 3, 1],
        'calls_handled': [50, 45, 60, 55, 48, 62, 53],
        'avg_handling_time': [180, 200, 170, 185, 195, 175, 182],
        'customer_satisfaction': [4.5, 4.2, 4.8, 4.6, 4.3, 4.7, 4.4],
        'sales_amount': [5000, 4500, 6000, 5200, 4800, 6200, 5100]
    })


@pytest.fixture
def sample_dataframe_with_nulls():
    """Create a DataFrame with null values for testing data cleaning."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', None, 'Charlie', 'David', None],
        'value': [100, 200, None, 400, 500],
        'category': ['A', 'B', 'A', None, 'B']
    })


@pytest.fixture
def sample_dataframe_with_duplicates():
    """Create a DataFrame with duplicate rows."""
    return pd.DataFrame({
        'id': [1, 2, 2, 3, 3],
        'name': ['Alice', 'Bob', 'Bob', 'Charlie', 'Charlie'],
        'value': [100, 200, 200, 300, 300]
    })


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def sample_report_config():
    """Create sample report configuration."""
    return {
        'report': {
            'name': 'Test Report',
            'description': 'A test report for unit testing',
            'version': '1.0',
            'data_source': {
                'type': 'postgres',
                'query': 'SELECT * FROM test_table'
            },
            'output': {
                'format': 'excel',
                'filename': 'test_report.xlsx'
            }
        }
    }


@pytest.fixture
def sample_email_config():
    """Create sample email configuration."""
    return {
        'smtp_host': 'smtp.test.com',
        'smtp_port': 587,
        'smtp_user': 'test@test.com',
        'smtp_password': 'testpassword',
        'from_address': 'reports@test.com'
    }


@pytest.fixture
def sample_database_config():
    """Create sample database configuration."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_database_connection():
    """Create a mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_smtp_server():
    """Create a mock SMTP server."""
    mock_server = MagicMock()
    mock_server.sendmail.return_value = {}
    mock_server.quit.return_value = None
    return mock_server


# ============================================================================
# Temporary File Fixtures
# ============================================================================

@pytest.fixture
def temp_csv_file(tmp_path, sample_dataframe):
    """Create a temporary CSV file."""
    file_path = tmp_path / "test_data.csv"
    sample_dataframe.to_csv(file_path, index=False)
    return str(file_path)


@pytest.fixture
def temp_excel_file(tmp_path, sample_dataframe):
    """Create a temporary Excel file."""
    file_path = tmp_path / "test_data.xlsx"
    sample_dataframe.to_excel(file_path, index=False)
    return str(file_path)


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return str(output_dir)


# ============================================================================
# Environment Fixtures
# ============================================================================

@pytest.fixture
def mock_environment(monkeypatch):
    """Set up mock environment variables."""
    env_vars = {
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'test_db',
        'POSTGRES_USER': 'test_user',
        'POSTGRES_PASSWORD': 'test_password',
        'SMTP_HOST': 'smtp.test.com',
        'SMTP_PORT': '587',
        'SMTP_USER': 'test@test.com',
        'SMTP_PASSWORD': 'testpassword',
        'ENVIRONMENT': 'testing',
        'TIMEZONE': 'America/Santiago'
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars
