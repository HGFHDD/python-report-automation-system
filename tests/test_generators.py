"""
Tests for Report Generators
"""

import pytest
import pandas as pd
import os
from unittest.mock import MagicMock, patch


class TestExcelGenerator:
    """Tests for Excel Generator."""
    
    def test_generate_basic_excel(self, sample_dataframe, temp_output_dir):
        """Test basic Excel generation."""
        from src.generators.excel_generator import ExcelGenerator
        
        output_path = os.path.join(temp_output_dir, 'test_report.xlsx')
        generator = ExcelGenerator({'output_path': output_path})
        
        result = generator.generate(sample_dataframe)
        
        assert os.path.exists(result)
        assert result.endswith('.xlsx')
    
    def test_generate_with_multiple_sheets(self, sample_dataframe, sample_sales_data, temp_output_dir):
        """Test Excel generation with multiple sheets."""
        from src.generators.excel_generator import ExcelGenerator
        
        output_path = os.path.join(temp_output_dir, 'multi_sheet_report.xlsx')
        generator = ExcelGenerator({'output_path': output_path})
        
        sheets_data = {
            'Employees': sample_dataframe,
            'Sales': sample_sales_data
        }
        
        result = generator.generate_multi_sheet(sheets_data)
        
        assert os.path.exists(result)
        
        # Verify both sheets exist
        excel_file = pd.ExcelFile(result)
        assert 'Employees' in excel_file.sheet_names
        assert 'Sales' in excel_file.sheet_names
    
    def test_generate_with_styling(self, sample_dataframe, temp_output_dir):
        """Test Excel generation with styling."""
        from src.generators.excel_generator import ExcelGenerator
        
        output_path = os.path.join(temp_output_dir, 'styled_report.xlsx')
        generator = ExcelGenerator({
            'output_path': output_path,
            'apply_styling': True
        })
        
        result = generator.generate(sample_dataframe)
        
        assert os.path.exists(result)


class TestPDFGenerator:
    """Tests for PDF Generator."""
    
    def test_generate_basic_pdf(self, sample_dataframe, temp_output_dir):
        """Test basic PDF generation."""
        from src.generators.pdf_generator import PDFGenerator
        
        output_path = os.path.join(temp_output_dir, 'test_report.pdf')
        generator = PDFGenerator({'output_path': output_path})
        
        result = generator.generate(
            sample_dataframe,
            title='Test Report'
        )
        
        assert os.path.exists(result)
        assert result.endswith('.pdf')
    
    def test_generate_pdf_with_metadata(self, sample_dataframe, temp_output_dir):
        """Test PDF generation with metadata."""
        from src.generators.pdf_generator import PDFGenerator
        
        output_path = os.path.join(temp_output_dir, 'metadata_report.pdf')
        generator = PDFGenerator({
            'output_path': output_path,
            'title': 'Test Report',
            'author': 'Test Author',
            'subject': 'Test Subject'
        })
        
        result = generator.generate(sample_dataframe)
        
        assert os.path.exists(result)


class TestHTMLGenerator:
    """Tests for HTML Generator."""
    
    def test_generate_basic_html(self, sample_dataframe, temp_output_dir):
        """Test basic HTML generation."""
        from src.generators.html_generator import HTMLGenerator
        
        output_path = os.path.join(temp_output_dir, 'test_report.html')
        generator = HTMLGenerator({'output_path': output_path})
        
        result = generator.generate(sample_dataframe)
        
        assert os.path.exists(result)
        assert result.endswith('.html')
        
        # Verify HTML content
        with open(result, 'r') as f:
            content = f.read()
            assert '<table' in content
            assert 'Alice' in content
    
    def test_generate_html_with_title(self, sample_dataframe, temp_output_dir):
        """Test HTML generation with custom title."""
        from src.generators.html_generator import HTMLGenerator
        
        output_path = os.path.join(temp_output_dir, 'titled_report.html')
        generator = HTMLGenerator({
            'output_path': output_path,
            'title': 'Custom Title Report'
        })
        
        result = generator.generate(sample_dataframe)
        
        with open(result, 'r') as f:
            content = f.read()
            assert 'Custom Title Report' in content


class TestChartGenerator:
    """Tests for Chart Generator."""
    
    def test_generate_bar_chart(self, sample_sales_data, temp_output_dir):
        """Test bar chart generation."""
        from src.generators.chart_generator import ChartGenerator
        
        generator = ChartGenerator({'output_dir': temp_output_dir})
        
        # Prepare aggregated data for chart
        chart_data = sample_sales_data.groupby('category').agg({
            'total_amount': 'sum'
        }).reset_index()
        
        result = generator.generate_bar_chart(
            chart_data,
            x='category',
            y='total_amount',
            title='Sales by Category',
            filename='bar_chart.png'
        )
        
        assert os.path.exists(result)
        assert result.endswith('.png')
    
    def test_generate_line_chart(self, sample_kpi_data, temp_output_dir):
        """Test line chart generation."""
        from src.generators.chart_generator import ChartGenerator
        
        generator = ChartGenerator({'output_dir': temp_output_dir})
        
        result = generator.generate_line_chart(
            sample_kpi_data,
            x='date',
            y='customer_satisfaction',
            title='Satisfaction Trend',
            filename='line_chart.png'
        )
        
        assert os.path.exists(result)
    
    def test_generate_pie_chart(self, sample_sales_data, temp_output_dir):
        """Test pie chart generation."""
        from src.generators.chart_generator import ChartGenerator
        
        generator = ChartGenerator({'output_dir': temp_output_dir})
        
        # Prepare aggregated data
        chart_data = sample_sales_data.groupby('category').agg({
            'total_amount': 'sum'
        }).reset_index()
        
        result = generator.generate_pie_chart(
            chart_data,
            labels='category',
            values='total_amount',
            title='Category Distribution',
            filename='pie_chart.png'
        )
        
        assert os.path.exists(result)


class TestBaseGenerator:
    """Tests for Base Generator."""
    
    def test_base_generator_is_abstract(self):
        """Test that base generator cannot be instantiated directly."""
        from src.generators.base_generator import BaseGenerator
        
        with pytest.raises(TypeError):
            BaseGenerator({})
    
    def test_ensure_output_directory(self, temp_output_dir):
        """Test output directory creation."""
        from src.generators.excel_generator import ExcelGenerator
        
        nested_path = os.path.join(temp_output_dir, 'nested', 'path', 'report.xlsx')
        generator = ExcelGenerator({'output_path': nested_path})
        
        # Directory should be created when generating
        generator._ensure_output_dir()
        
        parent_dir = os.path.dirname(nested_path)
        assert os.path.exists(parent_dir)
