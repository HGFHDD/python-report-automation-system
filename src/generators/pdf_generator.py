"""
PDF Generator - Generate PDF reports
"""
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.platypus import PageBreak
from src.generators.base_generator import BaseGenerator
from src.core.config import settings


class PDFGenerator(BaseGenerator):
    """
    Generator for PDF reports
    """

    def __init__(self, output_dir: Optional[Path] = None, page_size: tuple = A4):
        """
        Initialize PDF generator

        Args:
            output_dir: Output directory
            page_size: Page size (default A4)
        """
        super().__init__(output_dir or settings.PDF_DIR)
        self.page_size = page_size
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()

    def _setup_custom_styles(self) -> None:
        """Setup custom paragraph styles"""
        self.styles.add(ParagraphStyle(
            name='ReportTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            textColor=colors.HexColor('#2C3E50'),
            alignment=1  # Center
        ))
        
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceBefore=20,
            spaceAfter=10,
            textColor=colors.HexColor('#34495E')
        ))
        
        self.styles.add(ParagraphStyle(
            name='ReportDate',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.gray,
            alignment=1  # Center
        ))

    def generate(self, data: pd.DataFrame, filename: str, 
                title: str = "Report", **kwargs) -> Path:
        """
        Generate PDF report

        Args:
            data: DataFrame with report data
            filename: Output filename
            title: Report title
            **kwargs: Additional options

        Returns:
            Path to generated file
        """
        if not self.validate_data(data):
            raise ValueError("Invalid data provided")

        filename = self._prepare_filename(filename, '.pdf')
        
        if kwargs.get('add_timestamp', False):
            filename = self._add_timestamp_to_filename(filename)

        output_path = self._get_output_path(filename, 'pdf')

        doc = SimpleDocTemplate(
            str(output_path),
            pagesize=self.page_size,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )

        elements = []

        # Title
        elements.append(Paragraph(title, self.styles['ReportTitle']))
        
        # Date
        date_str = datetime.now().strftime(settings.DATETIME_FORMAT)
        elements.append(Paragraph(f"Generated: {date_str}", self.styles['ReportDate']))
        elements.append(Spacer(1, 30))

        # Description if provided
        if 'description' in kwargs:
            elements.append(Paragraph(kwargs['description'], self.styles['Normal']))
            elements.append(Spacer(1, 20))

        # Data table
        if 'section_title' in kwargs:
            elements.append(Paragraph(kwargs['section_title'], self.styles['SectionHeader']))

        table = self._create_table(data)
        elements.append(table)

        # Build PDF
        doc.build(elements)
        self.generated_files.append(output_path)
        self.logger.info(f"PDF report generated: {output_path}")

        return output_path

    def generate_multi_section(self, sections: List[Dict], 
                              filename: str, title: str = "Report") -> Path:
        """
        Generate PDF with multiple sections

        Args:
            sections: List of section configs with 'title' and 'data'
            filename: Output filename
            title: Report title

        Returns:
            Path to generated file
        """
        filename = self._prepare_filename(filename, '.pdf')
        output_path = self._get_output_path(filename, 'pdf')

        doc = SimpleDocTemplate(
            str(output_path),
            pagesize=self.page_size,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )

        elements = []

        # Title
        elements.append(Paragraph(title, self.styles['ReportTitle']))
        date_str = datetime.now().strftime(settings.DATETIME_FORMAT)
        elements.append(Paragraph(f"Generated: {date_str}", self.styles['ReportDate']))
        elements.append(Spacer(1, 30))

        # Sections
        for section in sections:
            section_title = section.get('title', 'Section')
            section_data = section.get('data')
            
            elements.append(Paragraph(section_title, self.styles['SectionHeader']))
            
            if section.get('description'):
                elements.append(Paragraph(section['description'], self.styles['Normal']))
                elements.append(Spacer(1, 10))

            if section_data is not None and not section_data.empty:
                table = self._create_table(section_data)
                elements.append(table)
            
            elements.append(Spacer(1, 20))

            if section.get('page_break', False):
                elements.append(PageBreak())

        doc.build(elements)
        self.generated_files.append(output_path)
        self.logger.info(f"Multi-section PDF report generated: {output_path}")

        return output_path

    def _create_table(self, data: pd.DataFrame) -> Table:
        """
        Create formatted table from DataFrame

        Args:
            data: Source DataFrame

        Returns:
            Formatted Table object
        """
        # Convert DataFrame to list format
        table_data = [data.columns.tolist()]
        for _, row in data.iterrows():
            table_data.append([str(v) for v in row.values])

        # Calculate column widths
        available_width = self.page_size[0] - 144  # Margins
        col_width = available_width / len(data.columns)
        col_widths = [min(col_width, 150)] * len(data.columns)

        table = Table(table_data, colWidths=col_widths)
        
        # Style the table
        style = TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472C4')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            
            # Data rows
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            
            # Grid
            ('GRID', (0, 0), (-1, -1), 0.5, colors.gray),
            
            # Alternate row colors
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F2F2F2')]),
        ])
        
        table.setStyle(style)
        return table

    def add_logo(self, logo_path: str, width: float = 2*inch) -> Image:
        """
        Create logo element for PDF

        Args:
            logo_path: Path to logo image
            width: Logo width

        Returns:
            Image element
        """
        return Image(logo_path, width=width)

    def set_page_size(self, size: tuple) -> None:
        """Set page size"""
        self.page_size = size
