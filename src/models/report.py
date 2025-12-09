"""
Report Model - Data model for reports
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class ReportFormat(Enum):
    """Report output formats"""
    EXCEL = "excel"
    PDF = "pdf"
    HTML = "html"
    CSV = "csv"


class ReportStatus(Enum):
    """Report generation status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SENT = "sent"


@dataclass
class ReportConfig:
    """Report configuration"""
    name: str
    description: str = ""
    format: ReportFormat = ReportFormat.EXCEL
    template: Optional[str] = None
    
    # Data source
    data_source_type: str = "postgres"
    query: str = ""
    
    # Processing
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Output
    output_filename: str = ""
    output_dir: Optional[Path] = None
    
    # Distribution
    email_recipients: List[str] = field(default_factory=list)
    email_subject: str = ""
    
    # Scheduling
    schedule_type: str = "manual"  # manual, daily, weekly, monthly
    schedule_config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.format, str):
            self.format = ReportFormat(self.format)


@dataclass
class Report:
    """Report instance"""
    id: str
    config: ReportConfig
    status: ReportStatus = ReportStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    output_path: Optional[Path] = None
    error_message: Optional[str] = None
    row_count: int = 0
    file_size: int = 0
    
    def __post_init__(self):
        if isinstance(self.status, str):
            self.status = ReportStatus(self.status)

    def mark_processing(self) -> None:
        """Mark report as processing"""
        self.status = ReportStatus.PROCESSING

    def mark_completed(self, output_path: Path, row_count: int = 0) -> None:
        """Mark report as completed"""
        self.status = ReportStatus.COMPLETED
        self.completed_at = datetime.now()
        self.output_path = output_path
        self.row_count = row_count
        if output_path.exists():
            self.file_size = output_path.stat().st_size

    def mark_failed(self, error: str) -> None:
        """Mark report as failed"""
        self.status = ReportStatus.FAILED
        self.completed_at = datetime.now()
        self.error_message = error

    def mark_sent(self) -> None:
        """Mark report as sent"""
        self.status = ReportStatus.SENT

    @property
    def duration(self) -> Optional[float]:
        """Get processing duration in seconds"""
        if self.completed_at:
            return (self.completed_at - self.created_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'name': self.config.name,
            'status': self.status.value,
            'format': self.config.format.value,
            'created_at': self.created_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'output_path': str(self.output_path) if self.output_path else None,
            'row_count': self.row_count,
            'file_size': self.file_size,
            'duration_seconds': self.duration,
            'error': self.error_message
        }


@dataclass
class ReportSchedule:
    """Report schedule configuration"""
    report_name: str
    schedule_type: str  # daily, weekly, monthly, cron
    
    # Time settings
    hour: int = 8
    minute: int = 0
    
    # Weekly
    day_of_week: Optional[str] = None
    
    # Monthly
    day_of_month: Optional[int] = None
    
    # Cron
    cron_expression: Optional[str] = None
    
    # Status
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None

    def to_cron_args(self) -> Dict[str, Any]:
        """Convert to APScheduler cron arguments"""
        args = {'hour': self.hour, 'minute': self.minute}
        
        if self.schedule_type == 'weekly' and self.day_of_week:
            args['day_of_week'] = self.day_of_week
        elif self.schedule_type == 'monthly' and self.day_of_month:
            args['day'] = self.day_of_month
            
        return args


@dataclass
class ChartConfig:
    """Chart configuration for reports"""
    chart_type: str  # bar, line, pie, scatter
    title: str
    x_column: str
    y_column: str
    
    # Optional settings
    color: Optional[str] = None
    legend: bool = True
    width: int = 600
    height: int = 400


@dataclass
class EmailConfig:
    """Email configuration for report distribution"""
    recipients: List[str]
    subject: str
    body_template: Optional[str] = None
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    attach_report: bool = True
