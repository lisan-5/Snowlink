"""
Prometheus metrics and monitoring for snowlink-ai
"""

import os
import time
from functools import wraps
from typing import Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime
from rich.console import Console

console = Console()

# Try to import Prometheus client
try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Info,
        CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST,
        start_http_server
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    console.print("[yellow]prometheus_client not installed. Metrics disabled.[/yellow]")


@dataclass
class MetricStats:
    """Internal metrics storage when Prometheus unavailable"""
    syncs_total: int = 0
    syncs_success: int = 0
    syncs_failed: int = 0
    tables_updated: int = 0
    columns_updated: int = 0
    api_requests: int = 0
    api_errors: int = 0
    llm_calls: int = 0
    llm_tokens: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    quality_checks_passed: int = 0
    quality_checks_failed: int = 0
    drift_issues: int = 0
    last_sync_duration: float = 0.0
    last_sync_time: Optional[datetime] = None


class MetricsCollector:
    """
    Metrics collection for monitoring and alerting.
    Uses Prometheus when available, falls back to internal storage.
    """
    
    def __init__(self, port: int = 9090, enabled: bool = True):
        self.enabled = enabled
        self.port = port
        self.internal_stats = MetricStats()
        
        if not enabled:
            return
        
        if PROMETHEUS_AVAILABLE:
            self._setup_prometheus()
        else:
            console.print("[yellow]Running with internal metrics only[/yellow]")
    
    def _setup_prometheus(self):
        """Setup Prometheus metrics"""
        self.registry = CollectorRegistry()
        
        # Sync metrics
        self.sync_total = Counter(
            "snowlink_syncs_total",
            "Total number of sync operations",
            ["source_type", "status"],
            registry=self.registry
        )
        
        self.sync_duration = Histogram(
            "snowlink_sync_duration_seconds",
            "Duration of sync operations",
            ["source_type"],
            buckets=[1, 5, 10, 30, 60, 120, 300],
            registry=self.registry
        )
        
        self.tables_updated = Counter(
            "snowlink_tables_updated_total",
            "Total tables updated in Snowflake",
            registry=self.registry
        )
        
        self.columns_updated = Counter(
            "snowlink_columns_updated_total",
            "Total columns updated in Snowflake",
            registry=self.registry
        )
        
        # LLM metrics
        self.llm_calls = Counter(
            "snowlink_llm_calls_total",
            "Total LLM API calls",
            ["provider", "model"],
            registry=self.registry
        )
        
        self.llm_tokens = Counter(
            "snowlink_llm_tokens_total",
            "Total LLM tokens used",
            ["provider", "type"],  # type: input/output
            registry=self.registry
        )
        
        self.llm_latency = Histogram(
            "snowlink_llm_latency_seconds",
            "LLM API call latency",
            ["provider"],
            buckets=[0.5, 1, 2, 5, 10, 30],
            registry=self.registry
        )
        
        # API metrics
        self.api_requests = Counter(
            "snowlink_api_requests_total",
            "Total API requests",
            ["method", "endpoint", "status"],
            registry=self.registry
        )
        
        self.api_latency = Histogram(
            "snowlink_api_latency_seconds",
            "API request latency",
            ["endpoint"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1, 5],
            registry=self.registry
        )
        
        # Cache metrics
        self.cache_operations = Counter(
            "snowlink_cache_operations_total",
            "Cache operations",
            ["operation", "status"],  # operation: get/set, status: hit/miss
            registry=self.registry
        )
        
        # Quality metrics
        self.quality_checks = Counter(
            "snowlink_quality_checks_total",
            "Data quality checks",
            ["status"],  # passed/failed/warning
            registry=self.registry
        )
        
        # Drift metrics
        self.drift_issues = Gauge(
            "snowlink_drift_issues",
            "Current schema drift issues",
            ["severity"],
            registry=self.registry
        )
        
        # System metrics
        self.info = Info(
            "snowlink",
            "snowlink-ai information",
            registry=self.registry
        )
        self.info.info({
            "version": "2.0.0",
            "python_version": os.popen("python --version").read().strip()
        })
        
        self.active_jobs = Gauge(
            "snowlink_active_jobs",
            "Number of active scheduled jobs",
            registry=self.registry
        )
    
    def start_server(self):
        """Start Prometheus metrics server"""
        if PROMETHEUS_AVAILABLE and self.enabled:
            try:
                start_http_server(self.port, registry=self.registry)
                console.print(f"[green]Metrics server started on port {self.port}[/green]")
            except Exception as e:
                console.print(f"[red]Failed to start metrics server: {e}[/red]")
    
    def record_sync(
        self,
        source_type: str,
        success: bool,
        duration: float,
        tables: int = 0,
        columns: int = 0
    ):
        """Record a sync operation"""
        self.internal_stats.syncs_total += 1
        self.internal_stats.last_sync_duration = duration
        self.internal_stats.last_sync_time = datetime.now()
        
        if success:
            self.internal_stats.syncs_success += 1
            self.internal_stats.tables_updated += tables
            self.internal_stats.columns_updated += columns
        else:
            self.internal_stats.syncs_failed += 1
        
        if PROMETHEUS_AVAILABLE and self.enabled:
            status = "success" if success else "failed"
            self.sync_total.labels(source_type=source_type, status=status).inc()
            self.sync_duration.labels(source_type=source_type).observe(duration)
            if tables:
                self.tables_updated.inc(tables)
            if columns:
                self.columns_updated.inc(columns)
    
    def record_llm_call(
        self,
        provider: str,
        model: str,
        latency: float,
        input_tokens: int = 0,
        output_tokens: int = 0
    ):
        """Record an LLM API call"""
        self.internal_stats.llm_calls += 1
        self.internal_stats.llm_tokens += input_tokens + output_tokens
        
        if PROMETHEUS_AVAILABLE and self.enabled:
            self.llm_calls.labels(provider=provider, model=model).inc()
            self.llm_latency.labels(provider=provider).observe(latency)
            if input_tokens:
                self.llm_tokens.labels(provider=provider, type="input").inc(input_tokens)
            if output_tokens:
                self.llm_tokens.labels(provider=provider, type="output").inc(output_tokens)
    
    def record_api_request(
        self,
        method: str,
        endpoint: str,
        status_code: int,
        latency: float
    ):
        """Record an API request"""
        self.internal_stats.api_requests += 1
        if status_code >= 400:
            self.internal_stats.api_errors += 1
        
        if PROMETHEUS_AVAILABLE and self.enabled:
            self.api_requests.labels(
                method=method,
                endpoint=endpoint,
                status=str(status_code)
            ).inc()
            self.api_latency.labels(endpoint=endpoint).observe(latency)
    
    def record_cache_operation(self, operation: str, hit: bool):
        """Record a cache operation"""
        if hit:
            self.internal_stats.cache_hits += 1
        else:
            self.internal_stats.cache_misses += 1
        
        if PROMETHEUS_AVAILABLE and self.enabled:
            status = "hit" if hit else "miss"
            self.cache_operations.labels(operation=operation, status=status).inc()
    
    def record_quality_check(self, passed: bool):
        """Record a quality check result"""
        if passed:
            self.internal_stats.quality_checks_passed += 1
        else:
            self.internal_stats.quality_checks_failed += 1
        
        if PROMETHEUS_AVAILABLE and self.enabled:
            status = "passed" if passed else "failed"
            self.quality_checks.labels(status=status).inc()
    
    def set_drift_issues(self, high: int, medium: int, low: int):
        """Set current drift issue counts"""
        self.internal_stats.drift_issues = high + medium + low
        
        if PROMETHEUS_AVAILABLE and self.enabled:
            self.drift_issues.labels(severity="high").set(high)
            self.drift_issues.labels(severity="medium").set(medium)
            self.drift_issues.labels(severity="low").set(low)
    
    def set_active_jobs(self, count: int):
        """Set number of active scheduled jobs"""
        if PROMETHEUS_AVAILABLE and self.enabled:
            self.active_jobs.set(count)
    
    def get_metrics(self) -> bytes:
        """Get Prometheus metrics in text format"""
        if PROMETHEUS_AVAILABLE and self.enabled:
            return generate_latest(self.registry)
        return b""
    
    def get_stats(self) -> dict:
        """Get internal stats as dictionary"""
        return {
            "syncs": {
                "total": self.internal_stats.syncs_total,
                "success": self.internal_stats.syncs_success,
                "failed": self.internal_stats.syncs_failed
            },
            "updates": {
                "tables": self.internal_stats.tables_updated,
                "columns": self.internal_stats.columns_updated
            },
            "llm": {
                "calls": self.internal_stats.llm_calls,
                "tokens": self.internal_stats.llm_tokens
            },
            "api": {
                "requests": self.internal_stats.api_requests,
                "errors": self.internal_stats.api_errors
            },
            "cache": {
                "hits": self.internal_stats.cache_hits,
                "misses": self.internal_stats.cache_misses,
                "hit_rate": f"{self.internal_stats.cache_hits / max(self.internal_stats.cache_hits + self.internal_stats.cache_misses, 1) * 100:.1f}%"
            },
            "quality": {
                "passed": self.internal_stats.quality_checks_passed,
                "failed": self.internal_stats.quality_checks_failed
            },
            "drift_issues": self.internal_stats.drift_issues,
            "last_sync": {
                "duration": self.internal_stats.last_sync_duration,
                "time": self.internal_stats.last_sync_time.isoformat() if self.internal_stats.last_sync_time else None
            }
        }
    
    def timed(self, metric_name: str = "operation"):
        """Decorator to time function execution"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                try:
                    return func(*args, **kwargs)
                finally:
                    duration = time.time() - start
                    # Record to internal stats
                    console.print(f"[dim]{metric_name} took {duration:.2f}s[/dim]")
            return wrapper
        return decorator


# Global metrics instance
_metrics: Optional[MetricsCollector] = None


def get_metrics() -> MetricsCollector:
    """Get or create the global metrics instance"""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector(
            port=int(os.getenv("PROMETHEUS_PORT", 9090)),
            enabled=os.getenv("METRICS_ENABLED", "true").lower() == "true"
        )
    return _metrics
