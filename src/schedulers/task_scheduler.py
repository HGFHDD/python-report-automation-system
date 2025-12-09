"""
Task Scheduler - Schedule and manage report jobs
"""
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from src.core.logger import logger
from src.core.config import settings


class TaskScheduler:
    """
    Task scheduler for automating report generation
    """

    def __init__(self, blocking: bool = False, timezone: str = None):
        """
        Initialize task scheduler

        Args:
            blocking: Use blocking scheduler (for standalone scripts)
            timezone: Timezone for scheduling
        """
        self.timezone = timezone or settings.TIMEZONE
        
        if blocking:
            self.scheduler = BlockingScheduler(timezone=self.timezone)
        else:
            self.scheduler = BackgroundScheduler(timezone=self.timezone)
        
        self.logger = logger
        self.jobs = {}
        
        # Add event listeners
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)

    def add_job(self, func: Callable, trigger: str, job_id: str = None,
               name: str = None, **trigger_args) -> str:
        """
        Add a job to the scheduler

        Args:
            func: Function to execute
            trigger: Trigger type ('cron', 'interval', 'date')
            job_id: Unique job ID
            name: Job name
            **trigger_args: Trigger-specific arguments

        Returns:
            Job ID
        """
        job_id = job_id or f"job_{len(self.jobs) + 1}"
        
        if trigger == 'cron':
            trigger_obj = CronTrigger(**trigger_args, timezone=self.timezone)
        elif trigger == 'interval':
            trigger_obj = IntervalTrigger(**trigger_args, timezone=self.timezone)
        elif trigger == 'date':
            trigger_obj = DateTrigger(**trigger_args, timezone=self.timezone)
        else:
            raise ValueError(f"Unknown trigger type: {trigger}")

        job = self.scheduler.add_job(
            func,
            trigger=trigger_obj,
            id=job_id,
            name=name or job_id,
            replace_existing=True
        )

        self.jobs[job_id] = {
            'function': func.__name__,
            'trigger': trigger,
            'trigger_args': trigger_args,
            'name': name or job_id,
            'created_at': datetime.now()
        }

        self.logger.info(f"Job added: {job_id} ({trigger})")
        return job_id

    def add_cron_job(self, func: Callable, hour: int, minute: int = 0,
                    day_of_week: str = '*', job_id: str = None) -> str:
        """
        Add a cron-style job

        Args:
            func: Function to execute
            hour: Hour to run (0-23)
            minute: Minute to run (0-59)
            day_of_week: Days to run ('mon-fri', '*', etc.)
            job_id: Job ID

        Returns:
            Job ID
        """
        return self.add_job(
            func,
            trigger='cron',
            job_id=job_id,
            hour=hour,
            minute=minute,
            day_of_week=day_of_week
        )

    def add_interval_job(self, func: Callable, hours: int = 0,
                        minutes: int = 0, seconds: int = 0,
                        job_id: str = None) -> str:
        """
        Add an interval job

        Args:
            func: Function to execute
            hours: Hours between runs
            minutes: Minutes between runs
            seconds: Seconds between runs
            job_id: Job ID

        Returns:
            Job ID
        """
        return self.add_job(
            func,
            trigger='interval',
            job_id=job_id,
            hours=hours,
            minutes=minutes,
            seconds=seconds
        )

    def add_daily_job(self, func: Callable, hour: int, minute: int = 0,
                     job_id: str = None) -> str:
        """
        Add a daily job

        Args:
            func: Function to execute
            hour: Hour to run
            minute: Minute to run
            job_id: Job ID

        Returns:
            Job ID
        """
        return self.add_cron_job(func, hour, minute, '*', job_id)

    def add_weekly_job(self, func: Callable, day_of_week: str,
                      hour: int, minute: int = 0, job_id: str = None) -> str:
        """
        Add a weekly job

        Args:
            func: Function to execute
            day_of_week: Day to run (e.g., 'mon', 'tue')
            hour: Hour to run
            minute: Minute to run
            job_id: Job ID

        Returns:
            Job ID
        """
        return self.add_cron_job(func, hour, minute, day_of_week, job_id)

    def add_monthly_job(self, func: Callable, day: int, hour: int,
                       minute: int = 0, job_id: str = None) -> str:
        """
        Add a monthly job

        Args:
            func: Function to execute
            day: Day of month (1-31)
            hour: Hour to run
            minute: Minute to run
            job_id: Job ID

        Returns:
            Job ID
        """
        return self.add_job(
            func,
            trigger='cron',
            job_id=job_id,
            day=day,
            hour=hour,
            minute=minute
        )

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a job

        Args:
            job_id: Job ID to remove

        Returns:
            True if removed
        """
        try:
            self.scheduler.remove_job(job_id)
            self.jobs.pop(job_id, None)
            self.logger.info(f"Job removed: {job_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove job {job_id}: {str(e)}")
            return False

    def pause_job(self, job_id: str) -> bool:
        """Pause a job"""
        try:
            self.scheduler.pause_job(job_id)
            self.logger.info(f"Job paused: {job_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to pause job: {str(e)}")
            return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job"""
        try:
            self.scheduler.resume_job(job_id)
            self.logger.info(f"Job resumed: {job_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to resume job: {str(e)}")
            return False

    def run_job_now(self, job_id: str) -> bool:
        """
        Run a job immediately

        Args:
            job_id: Job ID to run

        Returns:
            True if triggered
        """
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                job.func()
                self.logger.info(f"Job executed manually: {job_id}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to run job: {str(e)}")
            return False

    def get_jobs(self) -> List[Dict]:
        """
        Get all scheduled jobs

        Returns:
            List of job information
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': str(job.next_run_time) if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        return jobs

    def start(self) -> None:
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            self.logger.info("Scheduler started")

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the scheduler

        Args:
            wait: Wait for jobs to complete
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            self.logger.info("Scheduler shutdown")

    def _job_executed(self, event) -> None:
        """Handle job executed event"""
        self.logger.info(f"Job executed: {event.job_id}")

    def _job_error(self, event) -> None:
        """Handle job error event"""
        self.logger.error(f"Job error: {event.job_id} - {event.exception}")

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running"""
        return self.scheduler.running
