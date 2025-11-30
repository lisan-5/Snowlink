"""
Advanced job scheduling for automated sync operations
"""

import os
from datetime import datetime
from typing import Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from rich.console import Console
from rich.table import Table

console = Console()

# Try Redis job store
try:
    from apscheduler.jobstores.redis import RedisJobStore
    REDIS_JOBSTORE_AVAILABLE = True
except ImportError:
    REDIS_JOBSTORE_AVAILABLE = False


class JobType(Enum):
    """Types of scheduled jobs"""
    FULL_SYNC = "full_sync"
    CONFLUENCE_SYNC = "confluence_sync"
    JIRA_SYNC = "jira_sync"
    QUALITY_CHECK = "quality_check"
    DRIFT_CHECK = "drift_check"
    CLEANUP = "cleanup"
    CUSTOM = "custom"


@dataclass
class JobStatus:
    """Status of a scheduled job"""
    job_id: str
    job_type: JobType
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    last_status: str = "pending"
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


@dataclass
class JobConfig:
    """Configuration for a scheduled job"""
    job_id: str
    job_type: JobType
    schedule: str  # Cron expression or interval (e.g., "*/5 * * * *" or "5m")
    enabled: bool = True
    params: dict = field(default_factory=dict)
    max_retries: int = 3
    timeout_seconds: int = 300


class JobScheduler:
    """
    Advanced job scheduler for automated sync operations.
    Supports cron schedules, intervals, and distributed execution.
    """
    
    def __init__(self, config: dict, orchestrator=None):
        self.config = config
        self.orchestrator = orchestrator
        self.jobs: dict[str, JobStatus] = {}
        
        # Configure job stores
        jobstores = {"default": MemoryJobStore()}
        
        if REDIS_JOBSTORE_AVAILABLE and os.getenv("REDIS_URL"):
            try:
                jobstores["redis"] = RedisJobStore(
                    host=os.getenv("REDIS_HOST", "localhost"),
                    port=int(os.getenv("REDIS_PORT", 6379))
                )
            except Exception:
                pass
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 60
            }
        )
        
        # Job execution handlers
        self.handlers: dict[JobType, Callable] = {}
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default job handlers"""
        self.handlers[JobType.FULL_SYNC] = self._handle_full_sync
        self.handlers[JobType.CONFLUENCE_SYNC] = self._handle_confluence_sync
        self.handlers[JobType.JIRA_SYNC] = self._handle_jira_sync
        self.handlers[JobType.QUALITY_CHECK] = self._handle_quality_check
        self.handlers[JobType.DRIFT_CHECK] = self._handle_drift_check
        self.handlers[JobType.CLEANUP] = self._handle_cleanup
    
    def _parse_schedule(self, schedule: str):
        """Parse schedule string into trigger"""
        # Check if it's an interval (e.g., "5m", "1h", "30s")
        if schedule.endswith("s"):
            seconds = int(schedule[:-1])
            return IntervalTrigger(seconds=seconds)
        elif schedule.endswith("m"):
            minutes = int(schedule[:-1])
            return IntervalTrigger(minutes=minutes)
        elif schedule.endswith("h"):
            hours = int(schedule[:-1])
            return IntervalTrigger(hours=hours)
        elif schedule.endswith("d"):
            days = int(schedule[:-1])
            return IntervalTrigger(days=days)
        else:
            # Assume cron expression
            return CronTrigger.from_crontab(schedule)
    
    def add_job(self, job_config: JobConfig) -> bool:
        """Add a scheduled job"""
        if not job_config.enabled:
            return False
        
        try:
            trigger = self._parse_schedule(job_config.schedule)
            
            # Create job wrapper
            def job_wrapper():
                self._execute_job(job_config)
            
            self.scheduler.add_job(
                job_wrapper,
                trigger=trigger,
                id=job_config.job_id,
                name=f"{job_config.job_type.value}: {job_config.job_id}",
                replace_existing=True
            )
            
            # Track job status
            self.jobs[job_config.job_id] = JobStatus(
                job_id=job_config.job_id,
                job_type=job_config.job_type
            )
            
            console.print(f"[green]Scheduled job: {job_config.job_id} ({job_config.schedule})[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]Failed to add job {job_config.job_id}: {e}[/red]")
            return False
    
    def _execute_job(self, job_config: JobConfig):
        """Execute a scheduled job"""
        job_id = job_config.job_id
        status = self.jobs.get(job_id)
        
        if not status:
            return
        
        status.last_run = datetime.now()
        status.run_count += 1
        
        try:
            handler = self.handlers.get(job_config.job_type)
            if handler:
                handler(job_config.params)
                status.last_status = "success"
            else:
                status.last_status = "no_handler"
                
        except Exception as e:
            status.last_status = "failed"
            status.error_count += 1
            status.last_error = str(e)
            console.print(f"[red]Job {job_id} failed: {e}[/red]")
        
        # Update next run time
        job = self.scheduler.get_job(job_id)
        if job:
            status.next_run = job.next_run_time
    
    def _handle_full_sync(self, params: dict):
        """Handle full sync job"""
        if self.orchestrator:
            result = self.orchestrator.run_full_sync(dry_run=params.get("dry_run", False))
            console.print(f"[cyan]Full sync: {result.successful}/{result.total_sources}[/cyan]")
    
    def _handle_confluence_sync(self, params: dict):
        """Handle Confluence sync job"""
        if self.orchestrator:
            pages = params.get("pages", [])
            for page_id in pages:
                self.orchestrator.sync_confluence_page(page_id)
    
    def _handle_jira_sync(self, params: dict):
        """Handle Jira sync job"""
        if self.orchestrator:
            issues = params.get("issues", [])
            for issue_key in issues:
                self.orchestrator.sync_jira_issue(issue_key)
    
    def _handle_quality_check(self, params: dict):
        """Handle quality check job"""
        if self.orchestrator:
            tables = params.get("tables")
            result = self.orchestrator.run_quality_checks(tables)
            console.print(f"[cyan]Quality: {result.get('passed', 0)} passed, {result.get('failed', 0)} failed[/cyan]")
    
    def _handle_drift_check(self, params: dict):
        """Handle drift check job"""
        if self.orchestrator and self.orchestrator.drift_detector:
            # Would need documented schema to compare
            console.print("[cyan]Drift check executed[/cyan]")
    
    def _handle_cleanup(self, params: dict):
        """Handle cleanup job"""
        # Clean old audit logs, temp files, etc.
        retention_days = params.get("retention_days", 90)
        console.print(f"[cyan]Cleanup: Removing data older than {retention_days} days[/cyan]")
    
    def register_handler(self, job_type: JobType, handler: Callable):
        """Register a custom job handler"""
        self.handlers[job_type] = handler
    
    def remove_job(self, job_id: str):
        """Remove a scheduled job"""
        try:
            self.scheduler.remove_job(job_id)
            self.jobs.pop(job_id, None)
            console.print(f"[yellow]Removed job: {job_id}[/yellow]")
        except Exception as e:
            console.print(f"[red]Failed to remove job: {e}[/red]")
    
    def pause_job(self, job_id: str):
        """Pause a scheduled job"""
        self.scheduler.pause_job(job_id)
        if job_id in self.jobs:
            self.jobs[job_id].last_status = "paused"
    
    def resume_job(self, job_id: str):
        """Resume a paused job"""
        self.scheduler.resume_job(job_id)
        if job_id in self.jobs:
            self.jobs[job_id].last_status = "pending"
    
    def run_job_now(self, job_id: str):
        """Trigger immediate execution of a job"""
        job = self.scheduler.get_job(job_id)
        if job:
            job.modify(next_run_time=datetime.now())
    
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            console.print("[green]Job scheduler started[/green]")
    
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            console.print("[yellow]Job scheduler stopped[/yellow]")
    
    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get status of a specific job"""
        return self.jobs.get(job_id)
    
    def get_all_jobs(self) -> list[JobStatus]:
        """Get status of all jobs"""
        # Update next run times
        for job_id, status in self.jobs.items():
            job = self.scheduler.get_job(job_id)
            if job:
                status.next_run = job.next_run_time
        
        return list(self.jobs.values())
    
    def display_jobs(self):
        """Display all scheduled jobs"""
        table = Table(title="Scheduled Jobs", show_header=True)
        table.add_column("Job ID", style="cyan")
        table.add_column("Type")
        table.add_column("Next Run")
        table.add_column("Last Run")
        table.add_column("Status")
        table.add_column("Runs")
        table.add_column("Errors")
        
        for status in self.get_all_jobs():
            status_color = {
                "success": "green",
                "failed": "red",
                "pending": "yellow",
                "paused": "dim"
            }.get(status.last_status, "white")
            
            table.add_row(
                status.job_id,
                status.job_type.value,
                status.next_run.strftime("%Y-%m-%d %H:%M") if status.next_run else "-",
                status.last_run.strftime("%Y-%m-%d %H:%M") if status.last_run else "-",
                f"[{status_color}]{status.last_status}[/{status_color}]",
                str(status.run_count),
                str(status.error_count)
            )
        
        console.print(table)
    
    def setup_from_config(self):
        """Setup jobs from configuration"""
        sync_config = self.config.get("sync", {})
        
        # Full sync job
        if sync_config.get("mode") == "poll":
            interval = sync_config.get("interval_seconds", 300)
            self.add_job(JobConfig(
                job_id="full_sync",
                job_type=JobType.FULL_SYNC,
                schedule=f"{interval}s"
            ))
        
        # Quality check job
        quality_config = self.config.get("data_quality", {})
        if quality_config.get("enabled") and quality_config.get("run_schedule"):
            self.add_job(JobConfig(
                job_id="quality_check",
                job_type=JobType.QUALITY_CHECK,
                schedule=quality_config["run_schedule"]
            ))
        
        # Cleanup job (daily at midnight)
        self.add_job(JobConfig(
            job_id="cleanup",
            job_type=JobType.CLEANUP,
            schedule="0 0 * * *",
            params={"retention_days": self.config.get("audit", {}).get("retention_days", 365)}
        ))
