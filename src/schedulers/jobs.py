"""
Jobs - Predefined report generation jobs
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
import yaml
from src.core.config import settings
from src.core.logger import logger
from src.extractors.postgres_extractor import PostgresExtractor
from src.processors.data_transformer import DataTransformer
from src.processors.data_cleaner import DataCleaner
from src.processors.kpi_calculator import KPICalculator
from src.generators.excel_generator import ExcelGenerator
from src.generators.pdf_generator import PDFGenerator
from src.distributors.email_sender import EmailSender


def load_report_config(report_name: str) -> Dict[str, Any]:
    """
    Load report configuration from YAML file

    Args:
        report_name: Name of the report

    Returns:
        Report configuration dictionary
    """
    config_path = settings.CONFIG_DIR / 'reports' / f'{report_name}.yaml'
    
    if not config_path.exists():
        raise FileNotFoundError(f"Report config not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def daily_sales_report() -> Path:
    """
    Generate daily sales report
    
    Returns:
        Path to generated report
    """
    logger.info("Starting daily sales report generation")
    
    try:
        # Load config
        config = load_report_config('daily_report')
        
        # Extract data
        extractor = PostgresExtractor()
        query = config.get('report', {}).get('data_source', {}).get('query', 
            "SELECT * FROM sales WHERE date = CURRENT_DATE")
        data = extractor.extract_query(query)
        
        # Process data
        cleaner = DataCleaner(data)
        cleaned_data = cleaner.remove_duplicates().remove_nulls().get_data()
        
        transformer = DataTransformer(cleaned_data)
        transformations = config.get('report', {}).get('transformations', [])
        processed_data = transformer.apply_transformations(transformations)
        
        # Generate report
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"daily_sales_report_{date_str}.xlsx"
        
        generator = ExcelGenerator()
        report_path = generator.generate(
            data=processed_data,
            filename=filename,
            sheet_name="Daily Sales"
        )
        
        # Send via email if configured
        distribution = config.get('report', {}).get('distribution', {})
        if distribution.get('email'):
            sender = EmailSender()
            recipients = distribution['email'].get('to', [])
            subject = distribution['email'].get('subject', 'Daily Sales Report').format(
                date=datetime.now().strftime(settings.DATE_FORMAT)
            )
            sender.send_report(
                to=recipients,
                report_path=report_path,
                subject=subject
            )
        
        logger.info(f"Daily sales report generated: {report_path}")
        return report_path
        
    except Exception as e:
        logger.error(f"Failed to generate daily sales report: {str(e)}")
        raise


def weekly_kpi_report() -> Path:
    """
    Generate weekly KPI report
    
    Returns:
        Path to generated report
    """
    logger.info("Starting weekly KPI report generation")
    
    try:
        # Load config
        config = load_report_config('weekly_report')
        
        # Extract data
        extractor = PostgresExtractor()
        query = config.get('report', {}).get('data_source', {}).get('query',
            """SELECT * FROM performance_metrics 
               WHERE date >= CURRENT_DATE - INTERVAL '7 days'""")
        data = extractor.extract_query(query)
        
        # Process data
        cleaner = DataCleaner(data)
        cleaned_data = cleaner.remove_duplicates().remove_nulls().get_data()
        
        # Calculate KPIs
        kpi_calc = KPICalculator(cleaned_data)
        kpis = kpi_calc.get_summary()
        
        # Generate report
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"weekly_kpi_report_{date_str}.xlsx"
        
        generator = ExcelGenerator()
        report_path = generator.generate_multi_sheet(
            data_dict={
                'Data': cleaned_data,
                'KPIs': kpis
            },
            filename=filename
        )
        
        # Send via email if configured
        distribution = config.get('report', {}).get('distribution', {})
        if distribution.get('email'):
            sender = EmailSender()
            recipients = distribution['email'].get('to', [])
            sender.send_report(
                to=recipients,
                report_path=report_path,
                subject=f"Weekly KPI Report - {datetime.now().strftime(settings.DATE_FORMAT)}"
            )
        
        logger.info(f"Weekly KPI report generated: {report_path}")
        return report_path
        
    except Exception as e:
        logger.error(f"Failed to generate weekly KPI report: {str(e)}")
        raise


def monthly_summary_report() -> Path:
    """
    Generate monthly summary report
    
    Returns:
        Path to generated report
    """
    logger.info("Starting monthly summary report generation")
    
    try:
        # Load config
        config = load_report_config('monthly_report')
        
        # Extract data
        extractor = PostgresExtractor()
        query = config.get('report', {}).get('data_source', {}).get('query',
            """SELECT * FROM summary_data 
               WHERE date >= DATE_TRUNC('month', CURRENT_DATE)""")
        data = extractor.extract_query(query)
        
        # Process data
        cleaner = DataCleaner(data)
        cleaned_data = cleaner.remove_duplicates().remove_nulls().get_data()
        
        transformer = DataTransformer(cleaned_data)
        transformations = config.get('report', {}).get('transformations', [])
        processed_data = transformer.apply_transformations(transformations)
        
        # Generate PDF report
        date_str = datetime.now().strftime('%Y%m')
        filename = f"monthly_summary_report_{date_str}.pdf"
        
        pdf_generator = PDFGenerator()
        report_path = pdf_generator.generate(
            data=processed_data,
            filename=filename,
            title="Monthly Summary Report",
            description=f"Report for {datetime.now().strftime('%B %Y')}"
        )
        
        # Also generate Excel
        excel_filename = f"monthly_summary_report_{date_str}.xlsx"
        excel_generator = ExcelGenerator()
        excel_path = excel_generator.generate(
            data=processed_data,
            filename=excel_filename
        )
        
        # Send via email if configured
        distribution = config.get('report', {}).get('distribution', {})
        if distribution.get('email'):
            sender = EmailSender()
            recipients = distribution['email'].get('to', [])
            sender.send(
                to=recipients,
                subject=f"Monthly Summary Report - {datetime.now().strftime('%B %Y')}",
                body="Please find attached the monthly summary reports.",
                attachments=[report_path, excel_path]
            )
        
        logger.info(f"Monthly summary report generated: {report_path}")
        return report_path
        
    except Exception as e:
        logger.error(f"Failed to generate monthly summary report: {str(e)}")
        raise


def training_report() -> Path:
    """
    Generate training and certification report
    
    Returns:
        Path to generated report
    """
    logger.info("Starting training report generation")
    
    try:
        # Extract training data
        extractor = PostgresExtractor()
        data = extractor.extract_query(
            """SELECT * FROM training_records 
               WHERE completion_date >= CURRENT_DATE - INTERVAL '30 days'"""
        )
        
        # Process data
        cleaner = DataCleaner(data)
        cleaned_data = cleaner.remove_duplicates().get_data()
        
        # Calculate training KPIs
        kpi_calc = KPICalculator(cleaned_data)
        if 'completed' in cleaned_data.columns:
            kpi_calc.training_completion_rate('completed')
        if 'score' in cleaned_data.columns:
            kpi_calc.average_assessment_score('score')
        
        # Generate report
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"training_report_{date_str}.xlsx"
        
        generator = ExcelGenerator()
        report_path = generator.generate_multi_sheet(
            data_dict={
                'Training Data': cleaned_data,
                'KPIs': kpi_calc.get_summary()
            },
            filename=filename
        )
        
        logger.info(f"Training report generated: {report_path}")
        return report_path
        
    except Exception as e:
        logger.error(f"Failed to generate training report: {str(e)}")
        raise


# Job registry
AVAILABLE_JOBS = {
    'daily_sales': {
        'function': daily_sales_report,
        'description': 'Generate daily sales report',
        'schedule': {'hour': 8, 'minute': 0}
    },
    'weekly_kpi': {
        'function': weekly_kpi_report,
        'description': 'Generate weekly KPI report',
        'schedule': {'day_of_week': 'mon', 'hour': 9, 'minute': 0}
    },
    'monthly_summary': {
        'function': monthly_summary_report,
        'description': 'Generate monthly summary report',
        'schedule': {'day': 1, 'hour': 10, 'minute': 0}
    },
    'training': {
        'function': training_report,
        'description': 'Generate training report',
        'schedule': {'hour': 14, 'minute': 0}
    }
}


def get_job(job_name: str) -> Dict[str, Any]:
    """Get job by name"""
    return AVAILABLE_JOBS.get(job_name)


def list_jobs() -> List[str]:
    """List all available jobs"""
    return list(AVAILABLE_JOBS.keys())
