"""
Automated data quality checks and validation rules
"""

import os
import json
from datetime import datetime
from typing import Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from rich.console import Console
from rich.table import Table as RichTable

console = Console()


class QualityCheckType(Enum):
    """Types of data quality checks"""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    RELATIONSHIPS = "relationships"
    ROW_COUNT = "row_count"
    FRESHNESS = "freshness"
    CUSTOM_SQL = "custom_sql"
    REGEX_MATCH = "regex_match"
    VALUE_RANGE = "value_range"


class CheckStatus(Enum):
    """Status of a quality check"""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class QualityCheck:
    """Definition of a data quality check"""
    name: str
    check_type: QualityCheckType
    table_name: str
    column_name: Optional[str] = None
    parameters: dict = field(default_factory=dict)
    severity: str = "error"  # error, warning
    enabled: bool = True


@dataclass
class CheckResult:
    """Result of a quality check"""
    check_name: str
    status: CheckStatus
    table_name: str
    column_name: Optional[str] = None
    message: str = ""
    rows_checked: int = 0
    rows_failed: int = 0
    execution_time_ms: int = 0
    executed_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class QualityReport:
    """Complete data quality report"""
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    skipped: int = 0
    results: list = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())


class DataQualityChecker:
    """
    Automated data quality validation for Snowflake tables.
    Generates and runs checks based on documented schema.
    """
    
    def __init__(self, snowflake_client):
        self.snowflake_client = snowflake_client
        self.checks: list[QualityCheck] = []
    
    def generate_checks_from_schema(self, schema: dict) -> list[QualityCheck]:
        """
        Automatically generate quality checks from documented schema
        
        Args:
            schema: Extracted schema with table and column metadata
            
        Returns:
            List of generated QualityCheck objects
        """
        checks = []
        
        for table in schema.get("tables", []):
            table_name = table["table_name"].upper()
            
            # Row count check (table should have data)
            checks.append(QualityCheck(
                name=f"{table_name}_row_count",
                check_type=QualityCheckType.ROW_COUNT,
                table_name=table_name,
                parameters={"min_rows": 1},
                severity="warning"
            ))
            
            for column in table.get("columns", []):
                column_name = column["column_name"].upper()
                
                # Primary key checks
                if column.get("primary_key"):
                    checks.append(QualityCheck(
                        name=f"{table_name}_{column_name}_unique",
                        check_type=QualityCheckType.UNIQUE,
                        table_name=table_name,
                        column_name=column_name,
                        severity="error"
                    ))
                    checks.append(QualityCheck(
                        name=f"{table_name}_{column_name}_not_null",
                        check_type=QualityCheckType.NOT_NULL,
                        table_name=table_name,
                        column_name=column_name,
                        severity="error"
                    ))
                
                # Foreign key checks
                if column.get("foreign_key"):
                    fk_parts = column["foreign_key"].split(".")
                    if len(fk_parts) >= 2:
                        ref_table = fk_parts[0].upper()
                        ref_column = fk_parts[1].upper() if len(fk_parts) > 1 else column_name
                        
                        checks.append(QualityCheck(
                            name=f"{table_name}_{column_name}_fk",
                            check_type=QualityCheckType.RELATIONSHIPS,
                            table_name=table_name,
                            column_name=column_name,
                            parameters={
                                "ref_table": ref_table,
                                "ref_column": ref_column
                            },
                            severity="error"
                        ))
                
                # Not nullable columns
                if not column.get("nullable", True):
                    checks.append(QualityCheck(
                        name=f"{table_name}_{column_name}_not_null",
                        check_type=QualityCheckType.NOT_NULL,
                        table_name=table_name,
                        column_name=column_name,
                        severity="error"
                    ))
                
                # PII columns should have checks
                if column.get("pii"):
                    # Email format check
                    if "email" in column_name.lower():
                        checks.append(QualityCheck(
                            name=f"{table_name}_{column_name}_email_format",
                            check_type=QualityCheckType.REGEX_MATCH,
                            table_name=table_name,
                            column_name=column_name,
                            parameters={"pattern": r"^[^@]+@[^@]+\.[^@]+$"},
                            severity="warning"
                        ))
        
        self.checks.extend(checks)
        return checks
    
    def add_custom_check(
        self,
        name: str,
        table_name: str,
        sql: str,
        column_name: Optional[str] = None,
        severity: str = "error"
    ):
        """Add a custom SQL-based quality check"""
        self.checks.append(QualityCheck(
            name=name,
            check_type=QualityCheckType.CUSTOM_SQL,
            table_name=table_name,
            column_name=column_name,
            parameters={"sql": sql},
            severity=severity
        ))
    
    def run_checks(
        self,
        table_filter: Optional[list[str]] = None
    ) -> QualityReport:
        """
        Run all configured quality checks
        
        Args:
            table_filter: Optional list of table names to check
            
        Returns:
            QualityReport with all results
        """
        report = QualityReport()
        
        checks_to_run = self.checks
        if table_filter:
            table_filter_upper = [t.upper() for t in table_filter]
            checks_to_run = [c for c in self.checks if c.table_name in table_filter_upper]
        
        report.total_checks = len(checks_to_run)
        
        for check in checks_to_run:
            if not check.enabled:
                report.results.append(CheckResult(
                    check_name=check.name,
                    status=CheckStatus.SKIPPED,
                    table_name=check.table_name,
                    column_name=check.column_name,
                    message="Check disabled"
                ))
                report.skipped += 1
                continue
            
            try:
                result = self._run_single_check(check)
                report.results.append(result)
                
                if result.status == CheckStatus.PASSED:
                    report.passed += 1
                elif result.status == CheckStatus.FAILED:
                    report.failed += 1
                elif result.status == CheckStatus.WARNING:
                    report.warnings += 1
                else:
                    report.skipped += 1
                    
            except Exception as e:
                report.results.append(CheckResult(
                    check_name=check.name,
                    status=CheckStatus.FAILED,
                    table_name=check.table_name,
                    column_name=check.column_name,
                    message=f"Check execution error: {str(e)}"
                ))
                report.failed += 1
        
        return report
    
    def _run_single_check(self, check: QualityCheck) -> CheckResult:
        """Run a single quality check"""
        import time
        start_time = time.time()
        
        conn = self.snowflake_client._get_connection()
        cursor = conn.cursor()
        
        try:
            result = CheckResult(
                check_name=check.name,
                status=CheckStatus.PASSED,
                table_name=check.table_name,
                column_name=check.column_name
            )
            
            database = self.snowflake_client.database
            schema = self.snowflake_client.schema
            full_table = f"{database}.{schema}.{check.table_name}"
            
            if check.check_type == QualityCheckType.NOT_NULL:
                sql = f"SELECT COUNT(*) FROM {full_table} WHERE {check.column_name} IS NULL"
                cursor.execute(sql)
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    result.status = CheckStatus.FAILED
                    result.message = f"Found {null_count} NULL values"
                    result.rows_failed = null_count
                else:
                    result.message = "No NULL values found"
            
            elif check.check_type == QualityCheckType.UNIQUE:
                sql = f"""
                    SELECT {check.column_name}, COUNT(*) 
                    FROM {full_table} 
                    GROUP BY {check.column_name} 
                    HAVING COUNT(*) > 1
                """
                cursor.execute(sql)
                duplicates = cursor.fetchall()
                
                if duplicates:
                    result.status = CheckStatus.FAILED
                    result.message = f"Found {len(duplicates)} duplicate values"
                    result.rows_failed = sum(d[1] for d in duplicates)
                else:
                    result.message = "All values are unique"
            
            elif check.check_type == QualityCheckType.ROW_COUNT:
                min_rows = check.parameters.get("min_rows", 1)
                sql = f"SELECT COUNT(*) FROM {full_table}"
                cursor.execute(sql)
                row_count = cursor.fetchone()[0]
                result.rows_checked = row_count
                
                if row_count < min_rows:
                    result.status = CheckStatus.WARNING if check.severity == "warning" else CheckStatus.FAILED
                    result.message = f"Table has {row_count} rows, expected at least {min_rows}"
                else:
                    result.message = f"Table has {row_count} rows"
            
            elif check.check_type == QualityCheckType.RELATIONSHIPS:
                ref_table = check.parameters.get("ref_table")
                ref_column = check.parameters.get("ref_column")
                
                sql = f"""
                    SELECT COUNT(*) FROM {full_table} a
                    LEFT JOIN {database}.{schema}.{ref_table} b 
                    ON a.{check.column_name} = b.{ref_column}
                    WHERE a.{check.column_name} IS NOT NULL
                    AND b.{ref_column} IS NULL
                """
                cursor.execute(sql)
                orphan_count = cursor.fetchone()[0]
                
                if orphan_count > 0:
                    result.status = CheckStatus.FAILED
                    result.message = f"Found {orphan_count} orphan records"
                    result.rows_failed = orphan_count
                else:
                    result.message = "All foreign keys valid"
            
            elif check.check_type == QualityCheckType.CUSTOM_SQL:
                sql = check.parameters.get("sql", "")
                cursor.execute(sql)
                fail_count = cursor.fetchone()[0]
                
                if fail_count > 0:
                    result.status = CheckStatus.FAILED
                    result.message = f"Custom check found {fail_count} issues"
                    result.rows_failed = fail_count
                else:
                    result.message = "Custom check passed"
            
            result.execution_time_ms = int((time.time() - start_time) * 1000)
            return result
            
        finally:
            cursor.close()
    
    def display_report(self, report: QualityReport):
        """Display quality report with rich formatting"""
        # Summary
        summary = RichTable(title="Data Quality Summary", show_header=True)
        summary.add_column("Metric", style="cyan")
        summary.add_column("Value", style="white")
        
        summary.add_row("Total Checks", str(report.total_checks))
        summary.add_row("Passed", f"[green]{report.passed}[/green]")
        summary.add_row("Failed", f"[red]{report.failed}[/red]")
        summary.add_row("Warnings", f"[yellow]{report.warnings}[/yellow]")
        summary.add_row("Skipped", f"[dim]{report.skipped}[/dim]")
        
        console.print(summary)
        
        # Details
        if report.results:
            console.print()
            details = RichTable(title="Check Results", show_header=True)
            details.add_column("Status", style="bold")
            details.add_column("Check")
            details.add_column("Table")
            details.add_column("Column")
            details.add_column("Message")
            details.add_column("Time", justify="right")
            
            for result in report.results:
                status_color = {
                    CheckStatus.PASSED: "green",
                    CheckStatus.FAILED: "red",
                    CheckStatus.WARNING: "yellow",
                    CheckStatus.SKIPPED: "dim"
                }[result.status]
                
                details.add_row(
                    f"[{status_color}]{result.status.value.upper()}[/{status_color}]",
                    result.check_name,
                    result.table_name,
                    result.column_name or "-",
                    result.message[:40] + "..." if len(result.message) > 40 else result.message,
                    f"{result.execution_time_ms}ms"
                )
            
            console.print(details)
    
    def export_checks_to_dbt(self) -> str:
        """Export quality checks as dbt tests in schema.yml format"""
        models = {}
        
        for check in self.checks:
            table = check.table_name.lower()
            if table not in models:
                models[table] = {"columns": {}}
            
            if check.column_name:
                col = check.column_name.lower()
                if col not in models[table]["columns"]:
                    models[table]["columns"][col] = []
                
                # Map to dbt test names
                test_map = {
                    QualityCheckType.NOT_NULL: "not_null",
                    QualityCheckType.UNIQUE: "unique",
                }
                
                if check.check_type in test_map:
                    test_name = test_map[check.check_type]
                    if test_name not in models[table]["columns"][col]:
                        models[table]["columns"][col].append(test_name)
        
        # Build YAML structure
        yaml_content = {"version": 2, "models": []}
        
        for table_name, table_data in models.items():
            model = {
                "name": table_name,
                "columns": []
            }
            
            for col_name, tests in table_data["columns"].items():
                if tests:
                    model["columns"].append({
                        "name": col_name,
                        "tests": tests
                    })
            
            yaml_content["models"].append(model)
        
        import yaml
        return yaml.dump(yaml_content, default_flow_style=False, sort_keys=False)
