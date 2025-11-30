"""
Central orchestrator that coordinates all sync operations
with advanced features like caching, retry logic, and parallel processing
"""

import os
import asyncio
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .llm_extractor import LLMExtractor
from .multi_llm import MultiLLMExtractor
from .snowflake_client import SnowflakeClient
from .confluence_watcher import ConfluenceWatcher
from .jira_watcher import JiraWatcher
from .dbt_generator import DBTGenerator
from .er_diagram import ERDiagramGenerator
from .vector_store import VectorStore
from .lineage_tracker import LineageTracker
from .schema_drift import SchemaDriftDetector
from .data_quality import DataQualityChecker
from .notifications import NotificationManager
from .audit_log import AuditLogger, AuditAction

console = Console()


@dataclass
class SyncResult:
    """Result of a sync operation"""
    success: bool
    source_type: str
    source_id: str
    tables_found: int = 0
    tables_updated: int = 0
    columns_updated: int = 0
    drift_issues: int = 0
    quality_failures: int = 0
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    duration_seconds: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class BatchSyncResult:
    """Result of a batch sync operation"""
    total_sources: int = 0
    successful: int = 0
    failed: int = 0
    results: list = field(default_factory=list)
    duration_seconds: float = 0.0


class SyncOrchestrator:
    """
    Central orchestrator for all sync operations.
    Coordinates between components with caching, parallel processing,
    and comprehensive error handling.
    """
    
    def __init__(self, config: dict):
        self.config = config
        
        # Initialize core components
        self.snowflake = SnowflakeClient(config.get("snowflake", {}))
        self.confluence = ConfluenceWatcher(config.get("confluence", {}))
        self.jira = JiraWatcher(config.get("jira", {}))
        self.dbt_gen = DBTGenerator(config.get("dbt", {}))
        self.er_gen = ERDiagramGenerator(config.get("er_diagrams", {}))
        
        # Initialize advanced components
        llm_config = config.get("llm", {})
        if llm_config.get("fallback_providers"):
            self.extractor = MultiLLMExtractor(
                primary=llm_config.get("primary_provider", "openai"),
                fallbacks=llm_config.get("fallback_providers", [])
            )
        else:
            self.extractor = LLMExtractor()
        
        # Optional advanced features
        self.vector_store = None
        self.lineage = None
        self.drift_detector = None
        self.quality_checker = None
        self.notifications = None
        self.audit = None
        
        if config.get("vector_store", {}).get("enabled", False):
            self.vector_store = VectorStore(
                config.get("vector_store", {}).get("persist_directory", "data/vector_store")
            )
        
        if config.get("lineage", {}).get("enabled", False):
            self.lineage = LineageTracker(
                config.get("lineage", {}).get("storage_path", "data/lineage")
            )
        
        if config.get("schema_drift", {}).get("enabled", False):
            self.drift_detector = SchemaDriftDetector(self.snowflake)
        
        if config.get("data_quality", {}).get("enabled", False):
            self.quality_checker = DataQualityChecker(self.snowflake)
        
        if any(config.get("notifications", {}).get(k, {}).get("enabled") 
               for k in ["slack", "teams", "email", "webhook"]):
            self.notifications = NotificationManager(config)
        
        if config.get("audit", {}).get("enabled", True):
            self.audit = AuditLogger(
                config.get("audit", {}).get("db_path", "data/audit.db")
            )
        
        # Thread pool for parallel operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Cache for extracted schemas
        self._schema_cache: dict[str, dict] = {}
        
        # Load prompts
        self.prompts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")
    
    def _load_prompt(self, prompt_name: str) -> str:
        """Load a prompt template"""
        prompt_path = os.path.join(self.prompts_dir, prompt_name)
        with open(prompt_path, "r") as f:
            return f.read()
    
    def sync_confluence_page(
        self,
        page_id: str,
        dry_run: bool = False,
        skip_drift_check: bool = False,
        skip_quality_check: bool = False,
        post_diagram: bool = False
    ) -> SyncResult:
        """
        Sync a single Confluence page with all advanced features
        """
        import time
        start_time = time.time()
        
        result = SyncResult(
            success=False,
            source_type="confluence",
            source_id=page_id
        )
        
        # Log start
        if self.audit:
            self.audit.log_sync_start("confluence", page_id)
        
        try:
            # Fetch page content
            console.print(f"[cyan]Fetching Confluence page {page_id}...[/cyan]")
            page_data = self.confluence.get_page(page_id)
            
            if not page_data:
                raise ValueError(f"Failed to fetch page {page_id}")
            
            # Extract schema using LLM
            console.print("[cyan]Extracting schema with AI...[/cyan]")
            system_prompt = self._load_prompt("extract_schema.txt")
            
            if isinstance(self.extractor, MultiLLMExtractor):
                schema = self.extractor.extract_schema(
                    page_data["content"],
                    system_prompt,
                    source_type="confluence",
                    source_id=page_id
                )
            else:
                schema = self.extractor.extract_schema(
                    page_data["content"],
                    source_type="confluence",
                    source_id=page_id
                )
            
            if not schema or not schema.get("tables"):
                result.warnings.append("No tables found in content")
                result.success = True
                return result
            
            result.tables_found = len(schema["tables"])
            
            # Store in vector database for semantic search
            if self.vector_store:
                table_names = [t["table_name"] for t in schema["tables"]]
                self.vector_store.add_document(
                    content=page_data["content"],
                    source_type="confluence",
                    source_id=page_id,
                    title=page_data.get("title"),
                    tables_mentioned=table_names
                )
            
            # Track lineage
            if self.lineage:
                doc_id = self.lineage.add_document(
                    source_type="confluence",
                    source_id=page_id,
                    title=page_data.get("title", ""),
                    url=page_data.get("url")
                )
                
                for table in schema["tables"]:
                    table_id = self.lineage.add_table(
                        table_name=table["table_name"],
                        owner=table.get("owner"),
                        description=table.get("description")
                    )
                    self.lineage.link_table_to_document(
                        table["table_name"],
                        "confluence",
                        page_id
                    )
            
            # Schema drift detection
            if self.drift_detector and not skip_drift_check:
                console.print("[cyan]Checking for schema drift...[/cyan]")
                drift_report = self.drift_detector.compare(schema)
                result.drift_issues = drift_report.total_issues
                
                if drift_report.high_severity > 0:
                    result.warnings.append(
                        f"Schema drift: {drift_report.high_severity} high severity issues"
                    )
                    if self.notifications:
                        self.notifications.notify_drift_detected({
                            "total_issues": drift_report.total_issues,
                            "high_severity": drift_report.high_severity,
                            "medium_severity": drift_report.medium_severity
                        })
            
            if dry_run:
                console.print("[yellow]Dry run - no changes written[/yellow]")
                result.success = True
                return result
            
            # Write to Snowflake
            console.print("[cyan]Writing comments to Snowflake...[/cyan]")
            sf_result = self.snowflake.write_comments(schema)
            
            result.tables_updated = sf_result.get("tables_updated", 0)
            result.columns_updated = sf_result.get("columns_updated", 0)
            
            if sf_result.get("errors"):
                result.errors.extend(sf_result["errors"])
            
            # Log each comment written
            if self.audit:
                for table in schema["tables"]:
                    self.audit.log_comment_written(
                        table_name=table["table_name"],
                        comment=table.get("description", "")[:100]
                    )
            
            # Generate dbt models
            if self.config.get("dbt", {}).get("enabled", True):
                console.print("[cyan]Generating dbt models...[/cyan]")
                self.dbt_gen.generate(schema)
                
                if self.audit:
                    self.audit.log(
                        AuditAction.DBT_GENERATED,
                        source_type="confluence",
                        source_id=page_id,
                        details={"tables": [t["table_name"] for t in schema["tables"]]}
                    )
            
            # Generate ER diagram
            if self.config.get("er_diagrams", {}).get("enabled", True):
                console.print("[cyan]Generating ER diagram...[/cyan]")
                diagram_path = self.er_gen.generate(schema)
                
                if diagram_path and post_diagram:
                    diagram_content = self.er_gen.get_last_diagram_content()
                    if diagram_content:
                        self.confluence.post_diagram_to_page(page_id, diagram_content)
            
            # Data quality checks
            if self.quality_checker and not skip_quality_check:
                if self.config.get("data_quality", {}).get("run_on_sync", False):
                    console.print("[cyan]Running data quality checks...[/cyan]")
                    checks = self.quality_checker.generate_checks_from_schema(schema)
                    quality_report = self.quality_checker.run_checks(
                        [t["table_name"] for t in schema["tables"]]
                    )
                    result.quality_failures = quality_report.failed
                    
                    if quality_report.failed > 0 and self.notifications:
                        self.notifications.notify_quality_failed({
                            "total_checks": quality_report.total_checks,
                            "passed": quality_report.passed,
                            "failed": quality_report.failed
                        })
            
            result.success = True
            
            # Log completion
            if self.audit:
                self.audit.log_sync_complete(
                    "confluence",
                    page_id,
                    result.tables_updated,
                    result.columns_updated
                )
            
            # Send success notification
            if self.notifications and result.tables_updated > 0:
                self.notifications.notify_sync_complete(
                    "confluence",
                    page_id,
                    result.tables_updated,
                    result.columns_updated
                )
            
        except Exception as e:
            result.errors.append(str(e))
            
            if self.audit:
                self.audit.log_sync_failed("confluence", page_id, str(e))
            
            if self.notifications:
                self.notifications.notify_sync_failed("confluence", page_id, str(e))
        
        finally:
            result.duration_seconds = time.time() - start_time
        
        return result
    
    def sync_jira_issue(
        self,
        issue_key: str,
        dry_run: bool = False,
        skip_drift_check: bool = False
    ) -> SyncResult:
        """Sync a single Jira issue"""
        import time
        start_time = time.time()
        
        result = SyncResult(
            success=False,
            source_type="jira",
            source_id=issue_key
        )
        
        if self.audit:
            self.audit.log_sync_start("jira", issue_key)
        
        try:
            console.print(f"[cyan]Fetching Jira issue {issue_key}...[/cyan]")
            issue_data = self.jira.get_issue(issue_key)
            
            if not issue_data:
                raise ValueError(f"Failed to fetch issue {issue_key}")
            
            console.print("[cyan]Extracting schema with AI...[/cyan]")
            system_prompt = self._load_prompt("extract_schema.txt")
            
            if isinstance(self.extractor, MultiLLMExtractor):
                schema = self.extractor.extract_schema(
                    issue_data["content"],
                    system_prompt,
                    source_type="jira",
                    source_id=issue_key
                )
            else:
                schema = self.extractor.extract_schema(
                    issue_data["content"],
                    source_type="jira",
                    source_id=issue_key
                )
            
            if not schema or not schema.get("tables"):
                result.warnings.append("No tables found in content")
                result.success = True
                return result
            
            result.tables_found = len(schema["tables"])
            
            # Vector store
            if self.vector_store:
                table_names = [t["table_name"] for t in schema["tables"]]
                self.vector_store.add_document(
                    content=issue_data["content"],
                    source_type="jira",
                    source_id=issue_key,
                    title=issue_data.get("summary"),
                    tables_mentioned=table_names
                )
            
            # Lineage
            if self.lineage:
                self.lineage.add_document(
                    source_type="jira",
                    source_id=issue_key,
                    title=issue_data.get("summary", ""),
                    url=issue_data.get("url")
                )
                
                for table in schema["tables"]:
                    self.lineage.add_table(
                        table_name=table["table_name"],
                        owner=table.get("owner"),
                        description=table.get("description")
                    )
                    self.lineage.link_table_to_document(
                        table["table_name"],
                        "jira",
                        issue_key
                    )
            
            # Drift check
            if self.drift_detector and not skip_drift_check:
                drift_report = self.drift_detector.compare(schema)
                result.drift_issues = drift_report.total_issues
            
            if dry_run:
                result.success = True
                return result
            
            # Write to Snowflake
            console.print("[cyan]Writing comments to Snowflake...[/cyan]")
            sf_result = self.snowflake.write_comments(schema)
            
            result.tables_updated = sf_result.get("tables_updated", 0)
            result.columns_updated = sf_result.get("columns_updated", 0)
            
            # Generate artifacts
            self.dbt_gen.generate(schema)
            self.er_gen.generate(schema)
            
            result.success = True
            
            if self.audit:
                self.audit.log_sync_complete(
                    "jira", issue_key, result.tables_updated, result.columns_updated
                )
            
            if self.notifications and result.tables_updated > 0:
                self.notifications.notify_sync_complete(
                    "jira", issue_key, result.tables_updated, result.columns_updated
                )
            
        except Exception as e:
            result.errors.append(str(e))
            if self.audit:
                self.audit.log_sync_failed("jira", issue_key, str(e))
            if self.notifications:
                self.notifications.notify_sync_failed("jira", issue_key, str(e))
        
        finally:
            result.duration_seconds = time.time() - start_time
        
        return result
    
    def batch_sync(
        self,
        confluence_pages: Optional[list[str]] = None,
        jira_issues: Optional[list[str]] = None,
        dry_run: bool = False,
        parallel: bool = True
    ) -> BatchSyncResult:
        """
        Sync multiple sources in batch, optionally in parallel
        """
        import time
        start_time = time.time()
        
        batch_result = BatchSyncResult()
        sources = []
        
        if confluence_pages:
            sources.extend([("confluence", p) for p in confluence_pages])
        if jira_issues:
            sources.extend([("jira", i) for i in jira_issues])
        
        batch_result.total_sources = len(sources)
        
        if not sources:
            return batch_result
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Syncing...", total=len(sources))
            
            if parallel:
                # Parallel execution
                def sync_source(source_type, source_id):
                    if source_type == "confluence":
                        return self.sync_confluence_page(source_id, dry_run=dry_run)
                    else:
                        return self.sync_jira_issue(source_id, dry_run=dry_run)
                
                futures = []
                for source_type, source_id in sources:
                    future = self.executor.submit(sync_source, source_type, source_id)
                    futures.append(future)
                
                for future in futures:
                    result = future.result()
                    batch_result.results.append(result)
                    if result.success:
                        batch_result.successful += 1
                    else:
                        batch_result.failed += 1
                    progress.advance(task)
            else:
                # Sequential execution
                for source_type, source_id in sources:
                    progress.update(task, description=f"Syncing {source_type}:{source_id}")
                    
                    if source_type == "confluence":
                        result = self.sync_confluence_page(source_id, dry_run=dry_run)
                    else:
                        result = self.sync_jira_issue(source_id, dry_run=dry_run)
                    
                    batch_result.results.append(result)
                    if result.success:
                        batch_result.successful += 1
                    else:
                        batch_result.failed += 1
                    
                    progress.advance(task)
        
        batch_result.duration_seconds = time.time() - start_time
        
        return batch_result
    
    def run_full_sync(self, dry_run: bool = False) -> BatchSyncResult:
        """
        Run a full sync of all configured sources
        """
        console.print("[bold cyan]Starting full sync...[/bold cyan]")
        
        confluence_pages = []
        jira_issues = []
        
        # Check Confluence for updates
        if self.config.get("confluence", {}).get("enabled", True):
            updates = self.confluence.check_updates()
            confluence_pages = [u["id"] for u in updates]
            console.print(f"Found {len(confluence_pages)} Confluence pages to sync")
        
        # Check Jira for updates
        if self.config.get("jira", {}).get("enabled", True):
            updates = self.jira.check_updates()
            jira_issues = [u["key"] for u in updates]
            console.print(f"Found {len(jira_issues)} Jira issues to sync")
        
        return self.batch_sync(
            confluence_pages=confluence_pages,
            jira_issues=jira_issues,
            dry_run=dry_run
        )
    
    def search_documentation(self, query: str, n_results: int = 5) -> list[dict]:
        """Search indexed documentation using semantic search"""
        if not self.vector_store:
            console.print("[yellow]Vector store not enabled[/yellow]")
            return []
        
        return self.vector_store.search(query, n_results=n_results)
    
    def get_table_lineage(self, table_name: str) -> dict:
        """Get lineage information for a table"""
        if not self.lineage:
            return {"error": "Lineage tracking not enabled"}
        
        table_id = f"table:ANALYTICS.PUBLIC.{table_name}".upper()
        
        return {
            "table": table_name,
            "upstream": self.lineage.get_upstream(table_id),
            "downstream": self.lineage.get_downstream(table_id),
            "impact_analysis": self.lineage.impact_analysis(table_name)
        }
    
    def run_quality_checks(self, table_names: Optional[list[str]] = None) -> dict:
        """Run data quality checks"""
        if not self.quality_checker:
            return {"error": "Quality checker not enabled"}
        
        report = self.quality_checker.run_checks(table_names)
        return {
            "total_checks": report.total_checks,
            "passed": report.passed,
            "failed": report.failed,
            "warnings": report.warnings,
            "results": [
                {
                    "check": r.check_name,
                    "status": r.status.value,
                    "message": r.message
                }
                for r in report.results
            ]
        }
    
    def get_audit_stats(self, days: int = 30) -> dict:
        """Get audit statistics"""
        if not self.audit:
            return {"error": "Audit logging not enabled"}
        
        return self.audit.get_stats(days)
    
    def close(self):
        """Clean up resources"""
        self.snowflake.close()
        self.executor.shutdown(wait=False)
