"""
Tests for Data Extractors
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open
import os


class TestCSVExtractor:
    """Tests for CSV Extractor."""
    
    def test_extract_from_csv(self, temp_csv_file):
        """Test basic CSV extraction."""
        from src.extractors.csv_extractor import CSVExtractor
        
        extractor = CSVExtractor({'file_path': temp_csv_file})
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert 'id' in result.columns
        assert 'name' in result.columns
    
    def test_extract_with_columns(self, temp_csv_file):
        """Test CSV extraction with specific columns."""
        from src.extractors.csv_extractor import CSVExtractor
        
        extractor = CSVExtractor({
            'file_path': temp_csv_file,
            'columns': ['id', 'name']
        })
        result = extractor.extract()
        
        assert list(result.columns) == ['id', 'name']
    
    def test_extract_nonexistent_file(self):
        """Test extraction from non-existent file."""
        from src.extractors.csv_extractor import CSVExtractor
        
        extractor = CSVExtractor({'file_path': '/nonexistent/file.csv'})
        
        with pytest.raises(FileNotFoundError):
            extractor.extract()


class TestExcelExtractor:
    """Tests for Excel Extractor."""
    
    def test_extract_from_excel(self, temp_excel_file):
        """Test basic Excel extraction."""
        from src.extractors.excel_extractor import ExcelExtractor
        
        extractor = ExcelExtractor({'file_path': temp_excel_file})
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
    
    def test_extract_with_sheet_name(self, temp_excel_file):
        """Test Excel extraction from specific sheet."""
        from src.extractors.excel_extractor import ExcelExtractor
        
        extractor = ExcelExtractor({
            'file_path': temp_excel_file,
            'sheet_name': 'Sheet1'
        })
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)


class TestAPIExtractor:
    """Tests for API Extractor."""
    
    @patch('requests.get')
    def test_extract_from_api(self, mock_get):
        """Test basic API extraction."""
        from src.extractors.api_extractor import APIExtractor
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'id': 1, 'name': 'Test 1'},
            {'id': 2, 'name': 'Test 2'}
        ]
        mock_get.return_value = mock_response
        
        extractor = APIExtractor({
            'url': 'https://api.test.com/data',
            'method': 'GET'
        })
        result = extractor.extract()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    @patch('requests.get')
    def test_extract_with_headers(self, mock_get):
        """Test API extraction with custom headers."""
        from src.extractors.api_extractor import APIExtractor
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        extractor = APIExtractor({
            'url': 'https://api.test.com/data',
            'headers': {'Authorization': 'Bearer token123'}
        })
        extractor.extract()
        
        # Verify headers were passed
        call_kwargs = mock_get.call_args[1]
        assert 'headers' in call_kwargs
        assert call_kwargs['headers']['Authorization'] == 'Bearer token123'


class TestPostgresExtractor:
    """Tests for PostgreSQL Extractor."""
    
    @patch('sqlalchemy.create_engine')
    def test_connection_string_format(self, mock_engine):
        """Test that connection string is formatted correctly."""
        from src.extractors.postgres_extractor import PostgresExtractor
        
        config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        
        extractor = PostgresExtractor(config)
        
        # Verify engine was created with correct connection string
        mock_engine.assert_called_once()
        call_args = mock_engine.call_args[0][0]
        assert 'postgresql://' in call_args
        assert 'testuser' in call_args
        assert 'localhost' in call_args


class TestBaseExtractor:
    """Tests for Base Extractor."""
    
    def test_base_extractor_is_abstract(self):
        """Test that base extractor cannot be instantiated directly."""
        from src.extractors.base import BaseExtractor
        
        with pytest.raises(TypeError):
            BaseExtractor({})
