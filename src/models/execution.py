"""
Execution Model - Track report execution history
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid


class ExecutionStatus(Enum):
    """Execution status"""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionTrigger(Enum):
    """What triggered the execution"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    API = "api"
    RETRY = "retry"


@dataclass
class ExecutionStep:
    """Individual step in execution"""
    name: str
    status: str = "pending"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        """Start step"""
        self.status = "running"
        self.started_at = datetime.now()

    def complete(self, details: Dict = None) -> None:
        """Complete step successfully"""
        self.status = "success"
        self.completed_at = datetime.now()
        if details:
            self.details.update(details)

    def fail(self, error: str) -> None:
        """Mark step as failed"""
        self.status = "failed"
        self.completed_at = datetime.now()
        self.error = error

    @property
    def duration(self) -> Optional[float]:
        """Get step duration in seconds"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class Execution:
    """Report execution record"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    report_name: str = ""
    status: ExecutionStatus = ExecutionStatus.QUEUED
    trigger: ExecutionTrigger = ExecutionTrigger.MANUAL
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    output_path: Optional[str] = None
    row_count: int = 0
    file_size: int = 0
    
    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    
    # Steps tracking
    steps: List[ExecutionStep] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.status, str):
            self.status = ExecutionStatus(self.status)
        if isinstance(self.trigger, str):
            self.trigger = ExecutionTrigger(self.trigger)

    def start(self) -> None:
        """Start execution"""
        self.status = ExecutionStatus.RUNNING
        self.started_at = datetime.now()

    def complete(self, output_path: str = None, row_count: int = 0,
                file_size: int = 0) -> None:
        """Complete execution successfully"""
        self.status = ExecutionStatus.SUCCESS
        self.completed_at = datetime.now()
        self.output_path = output_path
        self.row_count = row_count
        self.file_size = file_size

    def fail(self, error: str) -> None:
        """Mark execution as failed"""
        self.status = ExecutionStatus.FAILED
        self.completed_at = datetime.now()
        self.error_message = error

    def cancel(self) -> None:
        """Cancel execution"""
        self.status = ExecutionStatus.CANCELLED
        self.completed_at = datetime.now()

    def can_retry(self) -> bool:
        """Check if execution can be retried"""
        return self.status == ExecutionStatus.FAILED and self.retry_count < self.max_retries

    def add_step(self, name: str) -> ExecutionStep:
        """Add execution step"""
        step = ExecutionStep(name=name)
        self.steps.append(step)
        return step

    def get_current_step(self) -> Optional[ExecutionStep]:
        """Get current running step"""
        for step in self.steps:
            if step.status == "running":
                return step
        return None

    @property
    def duration(self) -> Optional[float]:
        """Get total execution duration in seconds"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def is_finished(self) -> bool:
        """Check if execution is finished"""
        return self.status in [
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'report_name': self.report_name,
            'status': self.status.value,
            'trigger': self.trigger.value,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_seconds': self.duration,
            'output_path': self.output_path,
            'row_count': self.row_count,
            'file_size': self.file_size,
            'error': self.error_message,
            'retry_count': self.retry_count,
            'steps': [
                {
                    'name': s.name,
                    'status': s.status,
                    'duration': s.duration,
                    'error': s.error
                }
                for s in self.steps
            ],
            'metadata': self.metadata
        }


@dataclass
class ExecutionLog:
    """Log entry for execution"""
    execution_id: str
    timestamp: datetime = field(default_factory=datetime.now)
    level: str = "info"  # debug, info, warning, error
    message: str = ""
    step: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'execution_id': self.execution_id,
            'timestamp': self.timestamp.isoformat(),
            'level': self.level,
            'message': self.message,
            'step': self.step,
            'details': self.details
        }


class ExecutionHistory:
    """Manage execution history"""

    def __init__(self):
        self.executions: Dict[str, Execution] = {}
        self.logs: List[ExecutionLog] = []

    def add_execution(self, execution: Execution) -> None:
        """Add execution to history"""
        self.executions[execution.id] = execution

    def get_execution(self, execution_id: str) -> Optional[Execution]:
        """Get execution by ID"""
        return self.executions.get(execution_id)

    def get_recent(self, limit: int = 10) -> List[Execution]:
        """Get recent executions"""
        sorted_executions = sorted(
            self.executions.values(),
            key=lambda x: x.created_at,
            reverse=True
        )
        return sorted_executions[:limit]

    def get_by_report(self, report_name: str) -> List[Execution]:
        """Get executions for a specific report"""
        return [
            e for e in self.executions.values()
            if e.report_name == report_name
        ]

    def add_log(self, log: ExecutionLog) -> None:
        """Add log entry"""
        self.logs.append(log)

    def get_logs(self, execution_id: str) -> List[ExecutionLog]:
        """Get logs for execution"""
        return [l for l in self.logs if l.execution_id == execution_id]

    def cleanup_old(self, days: int = 30) -> int:
        """Remove executions older than specified days"""
        cutoff = datetime.now() - timedelta(days=days)
        old_ids = [
            e.id for e in self.executions.values()
            if e.created_at < cutoff
        ]
        
        for exec_id in old_ids:
            del self.executions[exec_id]
        
        self.logs = [l for l in self.logs if l.execution_id not in old_ids]
        
        return len(old_ids)


# Import timedelta for cleanup
from datetime import timedelta
