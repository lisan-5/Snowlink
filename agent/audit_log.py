"""
Comprehensive audit logging for all sync operations
Maintains complete history for compliance and debugging
"""

import os
import json
import sqlite3
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict
from enum import Enum
from rich.console import Console
from rich.table import Table as RichTable

console = Console()


class AuditAction(Enum):
    """Types of auditable actions"""
    SYNC_STARTED = "sync_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_FAILED = "sync_failed"
    SCHEMA_EXTRACTED = "schema_extracted"
    COMMENT_WRITTEN = "comment_written"
    DBT_GENERATED = "dbt_generated"
    DIAGRAM_GENERATED = "diagram_generated"
    DRIFT_DETECTED = "drift_detected"
    QUALITY_CHECK_RUN = "quality_check_run"
    CONFIG_CHANGED = "config_changed"


@dataclass
class AuditEntry:
    """A single audit log entry"""
    id: Optional[int] = None
    action: str = ""
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    table_name: Optional[str] = None
    user: str = "system"
    details: Optional[str] = None
    status: str = "success"
    error_message: Optional[str] = None
    created_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()


class AuditLogger:
    """
    SQLite-based audit logging for compliance and debugging.
    Maintains complete history of all sync operations.
    """
    
    def __init__(self, db_path: str = "data/audit.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                source_type TEXT,
                source_id TEXT,
                table_name TEXT,
                user TEXT DEFAULT 'system',
                details TEXT,
                status TEXT DEFAULT 'success',
                error_message TEXT,
                created_at TEXT NOT NULL
            )
        """)
        
        # Create indexes for common queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_action ON audit_log(action)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_source ON audit_log(source_type, source_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON audit_log(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_table ON audit_log(table_name)")
        
        conn.commit()
        conn.close()
    
    def log(
        self,
        action: AuditAction,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None,
        table_name: Optional[str] = None,
        user: str = "system",
        details: Optional[dict] = None,
        status: str = "success",
        error_message: Optional[str] = None
    ) -> int:
        """
        Log an audit entry
        
        Returns:
            ID of the created audit entry
        """
        entry = AuditEntry(
            action=action.value,
            source_type=source_type,
            source_id=source_id,
            table_name=table_name,
            user=user,
            details=json.dumps(details) if details else None,
            status=status,
            error_message=error_message
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO audit_log 
            (action, source_type, source_id, table_name, user, details, status, error_message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.action,
            entry.source_type,
            entry.source_id,
            entry.table_name,
            entry.user,
            entry.details,
            entry.status,
            entry.error_message,
            entry.created_at
        ))
        
        entry_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return entry_id
    
    def log_sync_start(self, source_type: str, source_id: str, user: str = "system") -> int:
        """Log the start of a sync operation"""
        return self.log(
            action=AuditAction.SYNC_STARTED,
            source_type=source_type,
            source_id=source_id,
            user=user,
            details={"started_at": datetime.now().isoformat()}
        )
    
    def log_sync_complete(
        self,
        source_type: str,
        source_id: str,
        tables_updated: int,
        columns_updated: int,
        user: str = "system"
    ) -> int:
        """Log successful sync completion"""
        return self.log(
            action=AuditAction.SYNC_COMPLETED,
            source_type=source_type,
            source_id=source_id,
            user=user,
            details={
                "tables_updated": tables_updated,
                "columns_updated": columns_updated,
                "completed_at": datetime.now().isoformat()
            }
        )
    
    def log_sync_failed(
        self,
        source_type: str,
        source_id: str,
        error: str,
        user: str = "system"
    ) -> int:
        """Log sync failure"""
        return self.log(
            action=AuditAction.SYNC_FAILED,
            source_type=source_type,
            source_id=source_id,
            user=user,
            status="failed",
            error_message=error
        )
    
    def log_comment_written(
        self,
        table_name: str,
        column_name: Optional[str] = None,
        comment: str = "",
        user: str = "system"
    ) -> int:
        """Log a comment written to Snowflake"""
        return self.log(
            action=AuditAction.COMMENT_WRITTEN,
            table_name=table_name,
            user=user,
            details={
                "column_name": column_name,
                "comment": comment[:500]  # Truncate long comments
            }
        )
    
    def get_entries(
        self,
        action: Optional[AuditAction] = None,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None,
        table_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> list[AuditEntry]:
        """
        Query audit entries with filters
        
        Returns:
            List of matching AuditEntry objects
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM audit_log WHERE 1=1"
        params = []
        
        if action:
            query += " AND action = ?"
            params.append(action.value)
        
        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)
        
        if source_id:
            query += " AND source_id = ?"
            params.append(source_id)
        
        if table_name:
            query += " AND table_name = ?"
            params.append(table_name)
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        if start_date:
            query += " AND created_at >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND created_at <= ?"
            params.append(end_date)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        # Convert to AuditEntry objects
        columns = ["id", "action", "source_type", "source_id", "table_name", 
                   "user", "details", "status", "error_message", "created_at"]
        
        entries = []
        for row in rows:
            entry_dict = dict(zip(columns, row))
            entries.append(AuditEntry(**entry_dict))
        
        return entries
    
    def get_sync_history(self, limit: int = 50) -> list[dict]:
        """Get recent sync history with aggregated info"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                source_type,
                source_id,
                action,
                status,
                details,
                error_message,
                created_at
            FROM audit_log
            WHERE action IN ('sync_started', 'sync_completed', 'sync_failed')
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            details = json.loads(row[4]) if row[4] else {}
            history.append({
                "source_type": row[0],
                "source_id": row[1],
                "action": row[2],
                "status": row[3],
                "details": details,
                "error": row[5],
                "timestamp": row[6]
            })
        
        return history
    
    def get_stats(self, days: int = 30) -> dict:
        """Get statistics for the last N days"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate date threshold
        from datetime import timedelta
        threshold = (datetime.now() - timedelta(days=days)).isoformat()
        
        # Total syncs
        cursor.execute("""
            SELECT COUNT(*) FROM audit_log 
            WHERE action = 'sync_completed' AND created_at >= ?
        """, (threshold,))
        total_syncs = cursor.fetchone()[0]
        
        # Failed syncs
        cursor.execute("""
            SELECT COUNT(*) FROM audit_log 
            WHERE action = 'sync_failed' AND created_at >= ?
        """, (threshold,))
        failed_syncs = cursor.fetchone()[0]
        
        # Tables updated
        cursor.execute("""
            SELECT COUNT(DISTINCT table_name) FROM audit_log 
            WHERE action = 'comment_written' AND created_at >= ?
        """, (threshold,))
        tables_updated = cursor.fetchone()[0]
        
        # By source type
        cursor.execute("""
            SELECT source_type, COUNT(*) FROM audit_log 
            WHERE action = 'sync_completed' AND created_at >= ?
            GROUP BY source_type
        """, (threshold,))
        by_source = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            "period_days": days,
            "total_syncs": total_syncs,
            "failed_syncs": failed_syncs,
            "success_rate": round((total_syncs - failed_syncs) / max(total_syncs, 1) * 100, 1),
            "tables_updated": tables_updated,
            "syncs_by_source": by_source
        }
    
    def display_history(self, limit: int = 20):
        """Display recent audit history with rich formatting"""
        entries = self.get_entries(limit=limit)
        
        table = RichTable(title="Audit Log", show_header=True)
        table.add_column("Time", style="dim")
        table.add_column("Action", style="cyan")
        table.add_column("Source")
        table.add_column("Table")
        table.add_column("Status")
        table.add_column("User", style="dim")
        
        for entry in entries:
            status_color = "green" if entry.status == "success" else "red"
            
            source = ""
            if entry.source_type and entry.source_id:
                source = f"{entry.source_type}:{entry.source_id}"
            
            # Parse timestamp for display
            try:
                dt = datetime.fromisoformat(entry.created_at)
                time_str = dt.strftime("%Y-%m-%d %H:%M")
            except:
                time_str = entry.created_at[:16]
            
            table.add_row(
                time_str,
                entry.action,
                source[:30],
                entry.table_name or "-",
                f"[{status_color}]{entry.status}[/{status_color}]",
                entry.user
            )
        
        console.print(table)
    
    def export_csv(self, filepath: str, days: int = 30):
        """Export audit log to CSV"""
        import csv
        from datetime import timedelta
        
        threshold = (datetime.now() - timedelta(days=days)).isoformat()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM audit_log WHERE created_at >= ? ORDER BY created_at
        """, (threshold,))
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        conn.close()
        
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
        
        console.print(f"[green]Exported {len(rows)} entries to {filepath}[/green]")
