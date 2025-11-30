"""
Schema drift detection - compare Snowflake schema with documented schema
to identify discrepancies and changes
"""

import os
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum
from rich.console import Console
from rich.table import Table as RichTable

console = Console()


class DriftType(Enum):
    """Types of schema drift"""
    TABLE_MISSING_IN_SNOWFLAKE = "table_missing_in_snowflake"
    TABLE_MISSING_IN_DOCS = "table_missing_in_docs"
    COLUMN_MISSING_IN_SNOWFLAKE = "column_missing_in_snowflake"
    COLUMN_MISSING_IN_DOCS = "column_missing_in_docs"
    TYPE_MISMATCH = "type_mismatch"
    DESCRIPTION_MISMATCH = "description_mismatch"
    NULLABLE_MISMATCH = "nullable_mismatch"


class DriftSeverity(Enum):
    """Severity levels for drift"""
    HIGH = "high"  # Breaking changes
    MEDIUM = "medium"  # Potential issues
    LOW = "low"  # Cosmetic/documentation only


@dataclass
class DriftIssue:
    """A single drift issue"""
    drift_type: DriftType
    severity: DriftSeverity
    table_name: str
    column_name: Optional[str] = None
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    message: str = ""
    detected_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class DriftReport:
    """Complete drift analysis report"""
    documented_tables: int = 0
    snowflake_tables: int = 0
    total_issues: int = 0
    high_severity: int = 0
    medium_severity: int = 0
    low_severity: int = 0
    issues: list = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())


class SchemaDriftDetector:
    """
    Detect drift between documented schema and actual Snowflake schema.
    Essential for maintaining data governance and documentation accuracy.
    """
    
    def __init__(self, snowflake_client):
        self.snowflake_client = snowflake_client
    
    def compare(
        self,
        documented_schema: dict,
        snowflake_tables: Optional[list[str]] = None
    ) -> DriftReport:
        """
        Compare documented schema with actual Snowflake schema
        
        Args:
            documented_schema: Schema extracted from documentation
            snowflake_tables: Optional list of specific tables to check
            
        Returns:
            DriftReport with all identified issues
        """
        report = DriftReport()
        report.documented_tables = len(documented_schema.get("tables", []))
        
        # Get table names from documentation
        doc_table_names = {t["table_name"].upper() for t in documented_schema.get("tables", [])}
        
        # If not specified, use all documented tables
        if snowflake_tables is None:
            snowflake_tables = list(doc_table_names)
        
        # Fetch actual schema from Snowflake
        actual_schema = self.snowflake_client.get_existing_schema(snowflake_tables)
        actual_table_names = {t["table_name"].upper() for t in actual_schema.get("tables", [])}
        
        report.snowflake_tables = len(actual_table_names)
        
        # Check for tables missing in Snowflake
        for table_name in doc_table_names - actual_table_names:
            report.issues.append(DriftIssue(
                drift_type=DriftType.TABLE_MISSING_IN_SNOWFLAKE,
                severity=DriftSeverity.HIGH,
                table_name=table_name,
                message=f"Table {table_name} is documented but does not exist in Snowflake"
            ))
        
        # Check for tables missing in docs (optional warning)
        for table_name in actual_table_names - doc_table_names:
            report.issues.append(DriftIssue(
                drift_type=DriftType.TABLE_MISSING_IN_DOCS,
                severity=DriftSeverity.MEDIUM,
                table_name=table_name,
                message=f"Table {table_name} exists in Snowflake but is not documented"
            ))
        
        # Compare columns for tables that exist in both
        common_tables = doc_table_names & actual_table_names
        
        for table_name in common_tables:
            # Get documented table
            doc_table = next(
                (t for t in documented_schema["tables"] if t["table_name"].upper() == table_name),
                None
            )
            # Get actual table
            actual_table = next(
                (t for t in actual_schema["tables"] if t["table_name"].upper() == table_name),
                None
            )
            
            if doc_table and actual_table:
                column_issues = self._compare_columns(table_name, doc_table, actual_table)
                report.issues.extend(column_issues)
        
        # Calculate totals
        report.total_issues = len(report.issues)
        report.high_severity = sum(1 for i in report.issues if i.severity == DriftSeverity.HIGH)
        report.medium_severity = sum(1 for i in report.issues if i.severity == DriftSeverity.MEDIUM)
        report.low_severity = sum(1 for i in report.issues if i.severity == DriftSeverity.LOW)
        
        return report
    
    def _compare_columns(
        self,
        table_name: str,
        doc_table: dict,
        actual_table: dict
    ) -> list[DriftIssue]:
        """Compare columns between documented and actual table"""
        issues = []
        
        doc_columns = {c["column_name"].upper(): c for c in doc_table.get("columns", [])}
        actual_columns = {c["column_name"].upper(): c for c in actual_table.get("columns", [])}
        
        # Columns missing in Snowflake
        for col_name in set(doc_columns.keys()) - set(actual_columns.keys()):
            issues.append(DriftIssue(
                drift_type=DriftType.COLUMN_MISSING_IN_SNOWFLAKE,
                severity=DriftSeverity.HIGH,
                table_name=table_name,
                column_name=col_name,
                message=f"Column {table_name}.{col_name} is documented but does not exist"
            ))
        
        # Columns missing in docs
        for col_name in set(actual_columns.keys()) - set(doc_columns.keys()):
            issues.append(DriftIssue(
                drift_type=DriftType.COLUMN_MISSING_IN_DOCS,
                severity=DriftSeverity.MEDIUM,
                table_name=table_name,
                column_name=col_name,
                message=f"Column {table_name}.{col_name} exists but is not documented"
            ))
        
        # Compare common columns
        for col_name in set(doc_columns.keys()) & set(actual_columns.keys()):
            doc_col = doc_columns[col_name]
            actual_col = actual_columns[col_name]
            
            # Check data type
            doc_type = (doc_col.get("data_type") or "").upper()
            actual_type = (actual_col.get("data_type") or "").upper()
            
            if doc_type and actual_type and doc_type != actual_type:
                # Allow some common type variations
                if not self._types_compatible(doc_type, actual_type):
                    issues.append(DriftIssue(
                        drift_type=DriftType.TYPE_MISMATCH,
                        severity=DriftSeverity.HIGH,
                        table_name=table_name,
                        column_name=col_name,
                        expected_value=doc_type,
                        actual_value=actual_type,
                        message=f"Type mismatch for {table_name}.{col_name}: documented as {doc_type}, actual is {actual_type}"
                    ))
            
            # Check nullable
            doc_nullable = doc_col.get("nullable", True)
            actual_nullable = actual_col.get("nullable", True)
            
            if doc_nullable != actual_nullable:
                issues.append(DriftIssue(
                    drift_type=DriftType.NULLABLE_MISMATCH,
                    severity=DriftSeverity.MEDIUM,
                    table_name=table_name,
                    column_name=col_name,
                    expected_value=str(doc_nullable),
                    actual_value=str(actual_nullable),
                    message=f"Nullable mismatch for {table_name}.{col_name}"
                ))
        
        return issues
    
    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two types are compatible (accounting for aliases)"""
        type_groups = [
            {"VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER"},
            {"INT", "INTEGER", "BIGINT", "SMALLINT", "NUMBER"},
            {"FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC"},
            {"BOOL", "BOOLEAN"},
            {"DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"},
        ]
        
        for group in type_groups:
            if type1 in group and type2 in group:
                return True
        
        return type1 == type2
    
    def generate_report(self, report: DriftReport) -> str:
        """Generate a formatted drift report"""
        lines = [
            "=" * 70,
            "SCHEMA DRIFT REPORT",
            f"Generated: {report.generated_at}",
            "=" * 70,
            "",
            f"Documented Tables: {report.documented_tables}",
            f"Snowflake Tables:  {report.snowflake_tables}",
            "",
            f"Total Issues:      {report.total_issues}",
            f"  High Severity:   {report.high_severity}",
            f"  Medium Severity: {report.medium_severity}",
            f"  Low Severity:    {report.low_severity}",
            "",
        ]
        
        if report.issues:
            lines.append("-" * 70)
            lines.append("ISSUES:")
            lines.append("-" * 70)
            
            # Group by severity
            for severity in [DriftSeverity.HIGH, DriftSeverity.MEDIUM, DriftSeverity.LOW]:
                severity_issues = [i for i in report.issues if i.severity == severity]
                if severity_issues:
                    lines.append(f"\n[{severity.value.upper()}]")
                    for issue in severity_issues:
                        lines.append(f"  - {issue.message}")
        else:
            lines.append("No drift detected. Schema is in sync!")
        
        return "\n".join(lines)
    
    def display_report(self, report: DriftReport):
        """Display drift report in the console with rich formatting"""
        # Summary table
        summary = RichTable(title="Schema Drift Summary", show_header=True)
        summary.add_column("Metric", style="cyan")
        summary.add_column("Value", style="white")
        
        summary.add_row("Documented Tables", str(report.documented_tables))
        summary.add_row("Snowflake Tables", str(report.snowflake_tables))
        summary.add_row("Total Issues", str(report.total_issues))
        summary.add_row("High Severity", f"[red]{report.high_severity}[/red]")
        summary.add_row("Medium Severity", f"[yellow]{report.medium_severity}[/yellow]")
        summary.add_row("Low Severity", f"[green]{report.low_severity}[/green]")
        
        console.print(summary)
        
        # Issues table
        if report.issues:
            console.print()
            issues_table = RichTable(title="Drift Issues", show_header=True)
            issues_table.add_column("Severity", style="bold")
            issues_table.add_column("Type")
            issues_table.add_column("Table")
            issues_table.add_column("Column")
            issues_table.add_column("Message")
            
            for issue in report.issues:
                severity_color = {
                    DriftSeverity.HIGH: "red",
                    DriftSeverity.MEDIUM: "yellow",
                    DriftSeverity.LOW: "green"
                }[issue.severity]
                
                issues_table.add_row(
                    f"[{severity_color}]{issue.severity.value.upper()}[/{severity_color}]",
                    issue.drift_type.value,
                    issue.table_name,
                    issue.column_name or "-",
                    issue.message[:50] + "..." if len(issue.message) > 50 else issue.message
                )
            
            console.print(issues_table)
