"""
HTML Generator - Generate interactive HTML reports
"""
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from datetime import datetime
from jinja2 import Template, Environment, FileSystemLoader
from src.generators.base_generator import BaseGenerator
from src.core.config import settings


class HTMLGenerator(BaseGenerator):
    """
    Generator for HTML reports with interactive features
    """

    def __init__(self, output_dir: Optional[Path] = None, 
                template_dir: Optional[Path] = None):
        """
        Initialize HTML generator

        Args:
            output_dir: Output directory
            template_dir: Directory with HTML templates
        """
        super().__init__(output_dir or settings.OUTPUT_DIR)
        self.template_dir = template_dir or settings.CONFIG_DIR / 'templates'
        
        if self.template_dir.exists():
            self.env = Environment(loader=FileSystemLoader(str(self.template_dir)))
        else:
            self.env = None

    def generate(self, data: pd.DataFrame, filename: str, 
                title: str = "Report", **kwargs) -> Path:
        """
        Generate HTML report

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

        filename = self._prepare_filename(filename, '.html')
        output_path = self._get_output_path(filename)

        # Generate HTML
        template_name = kwargs.get('template')
        
        if template_name and self.env:
            template = self.env.get_template(template_name)
            html_content = template.render(
                title=title,
                data=data,
                generated_at=datetime.now().strftime(settings.DATETIME_FORMAT),
                **kwargs
            )
        else:
            html_content = self._generate_default_html(data, title, **kwargs)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        self.generated_files.append(output_path)
        self.logger.info(f"HTML report generated: {output_path}")

        return output_path

    def _generate_default_html(self, data: pd.DataFrame, title: str, **kwargs) -> str:
        """
        Generate default HTML template

        Args:
            data: DataFrame
            title: Report title
            **kwargs: Additional options

        Returns:
            HTML string
        """
        description = kwargs.get('description', '')
        
        # Convert DataFrame to HTML table
        table_html = data.to_html(
            classes='data-table',
            index=False,
            escape=False
        )

        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f5f6fa;
            color: #2c3e50;
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 20px;
            text-align: center;
            margin-bottom: 30px;
            border-radius: 0 0 20px 20px;
        }}
        
        header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .meta-info {{
            color: rgba(255,255,255,0.8);
            font-size: 0.9em;
        }}
        
        .description {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .table-container {{
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .data-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        .data-table th {{
            background: #4472C4;
            color: white;
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
        }}
        
        .data-table td {{
            padding: 12px;
            border-bottom: 1px solid #eee;
        }}
        
        .data-table tr:hover {{
            background-color: #f8f9fa;
        }}
        
        .data-table tr:nth-child(even) {{
            background-color: #f5f6fa;
        }}
        
        footer {{
            text-align: center;
            padding: 30px;
            color: #7f8c8d;
            font-size: 0.9em;
        }}
        
        @media (max-width: 768px) {{
            .table-container {{
                overflow-x: auto;
            }}
            
            header h1 {{
                font-size: 1.8em;
            }}
        }}
    </style>
</head>
<body>
    <header>
        <h1>{title}</h1>
        <p class="meta-info">Generated: {datetime.now().strftime(settings.DATETIME_FORMAT)}</p>
    </header>
    
    <div class="container">
        {f'<div class="description"><p>{description}</p></div>' if description else ''}
        
        <div class="table-container">
            {table_html}
        </div>
    </div>
    
    <footer>
        <p>Report generated by Python Report Automation System</p>
    </footer>
</body>
</html>
"""
        return html_template

    def generate_dashboard(self, data_sections: List[Dict], 
                          filename: str, title: str = "Dashboard") -> Path:
        """
        Generate interactive dashboard with multiple sections

        Args:
            data_sections: List of section configs
            filename: Output filename
            title: Dashboard title

        Returns:
            Path to generated file
        """
        filename = self._prepare_filename(filename, '.html')
        output_path = self._get_output_path(filename)

        sections_html = ""
        for section in data_sections:
            section_title = section.get('title', 'Section')
            section_data = section.get('data')
            
            if section_data is not None and not section_data.empty:
                table_html = section_data.to_html(classes='data-table', index=False)
                sections_html += f"""
                <div class="section">
                    <h2>{section_title}</h2>
                    <div class="table-container">{table_html}</div>
                </div>
                """

        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', sans-serif; background: #f5f6fa; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                 color: white; padding: 40px; text-align: center; margin-bottom: 30px; }}
        .section {{ background: white; border-radius: 10px; padding: 20px; 
                   margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #34495e; margin-bottom: 15px; }}
        .data-table {{ width: 100%; border-collapse: collapse; }}
        .data-table th {{ background: #4472C4; color: white; padding: 12px; }}
        .data-table td {{ padding: 10px; border-bottom: 1px solid #eee; }}
        .data-table tr:hover {{ background: #f8f9fa; }}
    </style>
</head>
<body>
    <header>
        <h1>{title}</h1>
        <p>Generated: {datetime.now().strftime(settings.DATETIME_FORMAT)}</p>
    </header>
    <div class="container">
        {sections_html}
    </div>
</body>
</html>
"""

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        self.generated_files.append(output_path)
        self.logger.info(f"HTML dashboard generated: {output_path}")

        return output_path
