"""
Snowflake client for writing table and column comments
"""

import os
from typing import Optional
import snowflake.connector
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

console = Console()


class SnowflakeClient:
    """Client for Snowflake operations - primarily writing comments"""
    
    def __init__(self, config: dict):
        self.config = config
        self.database = config.get("database", os.getenv("SF_DATABASE", "ANALYTICS"))
        self.schema = config.get("schema", os.getenv("SF_SCHEMA", "PUBLIC"))
        self.warehouse = config.get("warehouse", os.getenv("SF_WAREHOUSE", "COMPUTE_WH"))
        self.dry_run = config.get("dry_run", False)
        self._connection = None
    
    def _get_connection(self):
        """Get or create Snowflake connection"""
        if self._connection is None or self._connection.is_closed():
            self._connection = snowflake.connector.connect(
                user=os.getenv("SF_USER"),
                password=os.getenv("SF_PASSWORD"),
                account=os.getenv("SF_ACCOUNT"),
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=os.getenv("SF_ROLE", "ACCOUNTADMIN"),
            )
        return self._connection
    
    def test_connection(self) -> bool:
        """Test the Snowflake connection"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except Exception as e:
            console.print(f"[red]Snowflake connection error: {e}[/red]")
            return False
    
    def _escape_string(self, value: str) -> str:
        """Escape single quotes for SQL strings"""
        if value is None:
            return ""
        return value.replace("'", "''")
    
    def _table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if a table exists in Snowflake"""
        schema = schema_name or self.schema
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {self.database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table_name.upper()}'
            """)
            result = cursor.fetchone()
            return result[0] > 0
        finally:
            cursor.close()
    
    def _column_exists(self, table_name: str, column_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if a column exists in a table"""
        schema = schema_name or self.schema
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {self.database}.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table_name.upper()}'
                AND COLUMN_NAME = '{column_name.upper()}'
            """)
            result = cursor.fetchone()
            return result[0] > 0
        finally:
            cursor.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def write_comments(self, schema: dict) -> dict:
        """
        Write table and column comments to Snowflake
        
        Args:
            schema: Extracted schema dictionary with tables and columns
            
        Returns:
            Result dictionary with success status and counts
        """
        result = {
            "success": True,
            "tables_updated": 0,
            "columns_updated": 0,
            "errors": [],
            "skipped": []
        }
        
        if self.dry_run:
            console.print("[yellow]🔍 Dry run mode - no changes will be written[/yellow]")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            for table in schema.get("tables", []):
                table_name = table["table_name"].upper()
                table_schema = table.get("schema_name", self.schema).upper()
                full_table_name = f"{self.database}.{table_schema}.{table_name}"
                
                # Check if table exists
                if not self._table_exists(table_name, table_schema):
                    result["skipped"].append(f"Table {full_table_name} does not exist")
                    continue
                
                # Write table comment
                table_desc = self._escape_string(table.get("description", ""))
                if table_desc:
                    sql = f"COMMENT ON TABLE {full_table_name} IS '{table_desc}'"
                    
                    if self.dry_run:
                        console.print(f"[dim]Would execute: {sql[:100]}...[/dim]")
                    else:
                        try:
                            cursor.execute(sql)
                            result["tables_updated"] += 1
                        except Exception as e:
                            result["errors"].append(f"Table {table_name}: {str(e)}")
                
                # Write column comments
                for column in table.get("columns", []):
                    column_name = column["column_name"].upper()
                    column_desc = self._escape_string(column.get("description", ""))
                    
                    if not column_desc:
                        continue
                    
                    # Check if column exists
                    if not self._column_exists(table_name, column_name, table_schema):
                        result["skipped"].append(f"Column {table_name}.{column_name} does not exist")
                        continue
                    
                    sql = f"COMMENT ON COLUMN {full_table_name}.{column_name} IS '{column_desc}'"
                    
                    if self.dry_run:
                        console.print(f"[dim]Would execute: {sql[:100]}...[/dim]")
                    else:
                        try:
                            cursor.execute(sql)
                            result["columns_updated"] += 1
                        except Exception as e:
                            result["errors"].append(f"Column {table_name}.{column_name}: {str(e)}")
            
            if not self.dry_run:
                conn.commit()
                
        except Exception as e:
            result["success"] = False
            result["error"] = str(e)
            
        finally:
            cursor.close()
        
        return result
    
    def get_existing_schema(self, table_names: list[str]) -> dict:
        """Fetch existing schema information from Snowflake"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        tables = []
        
        try:
            for table_name in table_names:
                # Get table info
                cursor.execute(f"""
                    SELECT TABLE_NAME, COMMENT
                    FROM {self.database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{self.schema}'
                    AND TABLE_NAME = '{table_name.upper()}'
                """)
                table_row = cursor.fetchone()
                
                if not table_row:
                    continue
                
                # Get columns
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT
                    FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{self.schema}'
                    AND TABLE_NAME = '{table_name.upper()}'
                    ORDER BY ORDINAL_POSITION
                """)
                columns = cursor.fetchall()
                
                tables.append({
                    "table_name": table_row[0],
                    "description": table_row[1] or "",
                    "columns": [
                        {
                            "column_name": col[0],
                            "data_type": col[1],
                            "nullable": col[2] == "YES",
                            "description": col[3] or ""
                        }
                        for col in columns
                    ]
                })
                
        finally:
            cursor.close()
        
        return {"tables": tables}
    
    def close(self):
        """Close the Snowflake connection"""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
