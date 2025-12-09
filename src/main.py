"""
Python Report Automation System
Main entry point with CLI interface
"""
import click
from datetime import datetime
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import settings
from src.core.logger import logger
from src.schedulers.task_scheduler import TaskScheduler
from src.schedulers.jobs import AVAILABLE_JOBS, get_job, list_jobs


@click.group()
@click.version_option(version=settings.APP_VERSION, prog_name=settings.APP_NAME)
def cli():
    """
    Python Report Automation System
    
    Automated report generation and distribution system.
    """
    pass


@cli.command()
@click.option('--report', '-r', required=True, help='Report name to generate')
@click.option('--date', '-d', default=None, help='Date for the report (YYYY-MM-DD)')
@click.option('--output', '-o', default=None, help='Output directory')
@click.option('--format', '-f', type=click.Choice(['excel', 'pdf', 'html', 'csv']), 
              default='excel', help='Output format')
def generate(report: str, date: str, output: str, format: str):
    """Generate a specific report"""
    logger.info(f"Generating report: {report}")
    
    try:
        job = get_job(report)
        if not job:
            click.echo(f"Error: Unknown report '{report}'", err=True)
            click.echo(f"Available reports: {', '.join(list_jobs())}")
            return
        
        # Execute the job
        result = job['function']()
        
        click.echo(f"✅ Report generated successfully: {result}")
        
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        click.echo(f"❌ Error: {str(e)}", err=True)


@cli.command()
@click.option('--report', '-r', required=True, help='Report file to send')
@click.option('--to', '-t', required=True, multiple=True, help='Email recipient(s)')
@click.option('--subject', '-s', default=None, help='Email subject')
def send(report: str, to: tuple, subject: str):
    """Send a report via email"""
    from src.distributors.email_sender import EmailSender
    
    report_path = Path(report)
    if not report_path.exists():
        # Try in output directories
        for output_dir in [settings.EXCEL_DIR, settings.PDF_DIR, settings.OUTPUT_DIR]:
            potential_path = output_dir / report
            if potential_path.exists():
                report_path = potential_path
                break
    
    if not report_path.exists():
        click.echo(f"❌ Report file not found: {report}", err=True)
        return
    
    sender = EmailSender()
    success = sender.send_report(
        to=list(to),
        report_path=report_path,
        subject=subject
    )
    
    if success:
        click.echo(f"✅ Report sent successfully to {', '.join(to)}")
    else:
        click.echo("❌ Failed to send report", err=True)


@cli.command('run-job')
@click.option('--job', '-j', required=True, help='Job name to run')
def run_job(job: str):
    """Run a scheduled job manually"""
    logger.info(f"Running job: {job}")
    
    job_config = get_job(job)
    if not job_config:
        click.echo(f"❌ Unknown job: {job}", err=True)
        click.echo(f"Available jobs: {', '.join(list_jobs())}")
        return
    
    try:
        result = job_config['function']()
        click.echo(f"✅ Job completed: {result}")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        click.echo(f"❌ Job failed: {str(e)}", err=True)


@cli.command('list-jobs')
def list_all_jobs():
    """List all available jobs"""
    click.echo("\n📋 Available Jobs:\n")
    click.echo("-" * 60)
    
    for job_name, job_config in AVAILABLE_JOBS.items():
        description = job_config.get('description', 'No description')
        schedule = job_config.get('schedule', {})
        
        click.echo(f"\n  {click.style(job_name, fg='cyan', bold=True)}")
        click.echo(f"    Description: {description}")
        if schedule:
            click.echo(f"    Schedule: {schedule}")
    
    click.echo("\n" + "-" * 60)


@cli.command()
@click.option('--lines', '-n', default=50, help='Number of lines to show')
@click.option('--level', '-l', type=click.Choice(['debug', 'info', 'warning', 'error']),
              default='info', help='Log level filter')
def logs(lines: int, level: str):
    """View recent logs"""
    log_files = list(settings.LOG_DIR.glob('*.log'))
    
    if not log_files:
        click.echo("No log files found")
        return
    
    # Get most recent log file
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
    
    click.echo(f"📄 Showing last {lines} lines from {latest_log.name}\n")
    click.echo("-" * 60)
    
    with open(latest_log, 'r') as f:
        all_lines = f.readlines()
        
        # Filter by level if needed
        if level != 'debug':
            level_order = ['debug', 'info', 'warning', 'error']
            min_level = level_order.index(level)
            filtered_lines = []
            for line in all_lines:
                for l in level_order[min_level:]:
                    if l.upper() in line:
                        filtered_lines.append(line)
                        break
            all_lines = filtered_lines
        
        for line in all_lines[-lines:]:
            # Colorize by level
            if 'ERROR' in line:
                click.echo(click.style(line.rstrip(), fg='red'))
            elif 'WARNING' in line:
                click.echo(click.style(line.rstrip(), fg='yellow'))
            elif 'INFO' in line:
                click.echo(line.rstrip())
            else:
                click.echo(click.style(line.rstrip(), dim=True))


@cli.command()
def start():
    """Start the scheduler daemon"""
    click.echo("🚀 Starting Report Automation Scheduler...")
    click.echo("-" * 60)
    
    scheduler = TaskScheduler(blocking=True)
    
    # Add all configured jobs
    for job_name, job_config in AVAILABLE_JOBS.items():
        schedule = job_config.get('schedule', {})
        if schedule:
            if 'day_of_week' in schedule:
                scheduler.add_weekly_job(
                    func=job_config['function'],
                    day_of_week=schedule['day_of_week'],
                    hour=schedule.get('hour', 8),
                    minute=schedule.get('minute', 0),
                    job_id=job_name
                )
            elif 'day' in schedule:
                scheduler.add_monthly_job(
                    func=job_config['function'],
                    day=schedule['day'],
                    hour=schedule.get('hour', 8),
                    minute=schedule.get('minute', 0),
                    job_id=job_name
                )
            else:
                scheduler.add_daily_job(
                    func=job_config['function'],
                    hour=schedule.get('hour', 8),
                    minute=schedule.get('minute', 0),
                    job_id=job_name
                )
            click.echo(f"  ✓ Scheduled: {job_name}")
    
    click.echo("\n" + "-" * 60)
    click.echo("Scheduler running. Press Ctrl+C to stop.")
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        click.echo("\n🛑 Scheduler stopped")
        scheduler.shutdown()


@cli.command()
def status():
    """Show system status"""
    click.echo("\n" + "=" * 60)
    click.echo(click.style(f"  {settings.APP_NAME} v{settings.APP_VERSION}", 
                          fg='cyan', bold=True))
    click.echo("=" * 60)
    
    click.echo(f"\n📁 Directories:")
    click.echo(f"    Output: {settings.OUTPUT_DIR}")
    click.echo(f"    Excel:  {settings.EXCEL_DIR}")
    click.echo(f"    PDF:    {settings.PDF_DIR}")
    click.echo(f"    Logs:   {settings.LOG_DIR}")
    
    click.echo(f"\n🗄️  Database:")
    click.echo(f"    Host: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}")
    click.echo(f"    Database: {settings.POSTGRES_DB}")
    
    click.echo(f"\n📧 Email:")
    click.echo(f"    SMTP: {settings.SMTP_HOST}:{settings.SMTP_PORT}")
    click.echo(f"    From: {settings.EMAIL_FROM}")
    
    click.echo(f"\n⏰ Timezone: {settings.TIMEZONE}")
    click.echo(f"🌍 Environment: {settings.ENVIRONMENT}")
    
    # Count files in output directories
    excel_files = len(list(settings.EXCEL_DIR.glob('*.xlsx')))
    pdf_files = len(list(settings.PDF_DIR.glob('*.pdf')))
    log_files = len(list(settings.LOG_DIR.glob('*.log')))
    
    click.echo(f"\n📊 Generated Reports:")
    click.echo(f"    Excel files: {excel_files}")
    click.echo(f"    PDF files:   {pdf_files}")
    click.echo(f"    Log files:   {log_files}")
    
    click.echo("\n" + "=" * 60 + "\n")


@cli.command()
@click.option('--days', '-d', default=30, help='Delete files older than N days')
@click.option('--dry-run', is_flag=True, help='Show what would be deleted')
def cleanup(days: int, dry_run: bool):
    """Clean up old reports and logs"""
    from datetime import timedelta
    import os
    
    cutoff = datetime.now() - timedelta(days=days)
    deleted_count = 0
    total_size = 0
    
    click.echo(f"🧹 Cleaning up files older than {days} days...")
    
    for directory in [settings.EXCEL_DIR, settings.PDF_DIR, settings.LOG_DIR]:
        for file_path in directory.glob('*'):
            if file_path.is_file():
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if mtime < cutoff:
                    file_size = file_path.stat().st_size
                    if dry_run:
                        click.echo(f"  Would delete: {file_path.name}")
                    else:
                        os.remove(file_path)
                        click.echo(f"  Deleted: {file_path.name}")
                    deleted_count += 1
                    total_size += file_size
    
    if dry_run:
        click.echo(f"\n📋 Would delete {deleted_count} files ({total_size / 1024:.1f} KB)")
    else:
        click.echo(f"\n✅ Deleted {deleted_count} files ({total_size / 1024:.1f} KB)")


def main():
    """Main entry point"""
    cli()


if __name__ == '__main__':
    main()
