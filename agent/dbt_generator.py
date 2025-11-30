"""
Generate dbt model files from extracted schema
"""

import os
from datetime import datetime
from typing import Optional
import yaml
from rich.console import Console

console = Console()


class DBTGenerator:
    """Generate dbt model files (.sql and schema.yml) from extracted schema"""
    
    def __init__(self, config: dict):
        self.config = config
        self.enabled = config.get("enabled", True)
        self.output_dir = config.get("output_dir", "output/dbt_models")
        self.materialization = config.get("materialization", "table")
        self.generate_sources = config.get("generate_sources", True)
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def generate(self, schema: dict) -> dict:
        """
        Generate dbt model files from extracted schema
        
        Args:
            schema: Extracted schema dictionary with tables
            
        Returns:
            Dictionary with generated file paths
        """
        if not self.enabled:
            return {"generated": False, "reason": "dbt generation disabled"}
        
        result = {
            "generated": True,
            "sql_files": [],
            "schema_file": None,
            "sources_file": None
        }
        
        tables = schema.get("tables", [])
        if not tables:
            return {"generated": False, "reason": "No tables in schema"}
        
        # Generate SQL files for each table
        for table in tables:
            sql_path = self._generate_sql_file(table)
            if sql_path:
                result["sql_files"].append(sql_path)
        
        # Generate schema.yml
        schema_path = self._generate_schema_yml(tables)
        if schema_path:
            result["schema_file"] = schema_path
        
        # Generate sources.yml
        if self.generate_sources:
            sources_path = self._generate_sources_yml(tables)
            if sources_path:
                result["sources_file"] = sources_path
        
        return result
    
    def _generate_sql_file(self, table: dict) -> Optional[str]:
        """Generate a .sql file for a single table"""
        table_name = table["table_name"].lower()
        filename = f"{table_name}.sql"
        filepath = os.path.join(self.output_dir, filename)
        
        # Build column list
        columns = table.get("columns", [])
        column_lines = []
        for col in columns:
            col_name = col["column_name"]
            col_desc = col.get("description", "")
            comment = f"  -- {col_desc}" if col_desc else ""
            column_lines.append(f"    {col_name}{comment}")
        
        columns_str = ",\n".join(column_lines) if column_lines else "    *"
        
        # Determine source
        schema_name = table.get("schema_name", "raw").lower()
        
        sql_content = f'''{{{{ config(
    materialized='{self.materialization}',
    tags=['auto-generated', 'snowlink-ai']
) }}}}

/*
    Model: {table_name}
    Description: {table.get("description", "Auto-generated model")}
    Owner: {table.get("owner", "Unknown")}
    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by snowlink-ai
*/

SELECT
{columns_str}
FROM {{{{ source('{schema_name}', '{table_name}') }}}}
'''
        
        try:
            with open(filepath, "w") as f:
                f.write(sql_content)
            console.print(f"[green]✅ Generated {filepath}[/green]")
            return filepath
        except Exception as e:
            console.print(f"[red]❌ Failed to generate {filepath}: {e}[/red]")
            return None
    
    def _generate_schema_yml(self, tables: list[dict]) -> Optional[str]:
        """Generate schema.yml file for all models"""
        filepath = os.path.join(self.output_dir, "schema.yml")
        
        models = []
        for table in tables:
            model = {
                "name": table["table_name"].lower(),
                "description": table.get("description", ""),
                "meta": {
                    "owner": table.get("owner", "Unknown"),
                    "generated_by": "snowlink-ai",
                    "generated_at": datetime.now().isoformat()
                },
                "columns": []
            }
            
            for col in table.get("columns", []):
                column_def = {
                    "name": col["column_name"],
                    "description": col.get("description", "")
                }
                
                # Add tests based on column properties
                tests = []
                if col.get("primary_key"):
                    tests.extend(["unique", "not_null"])
                if col.get("foreign_key"):
                    tests.append("not_null")
                if col.get("pii"):
                    column_def["meta"] = {"pii": True}
                
                if tests:
                    column_def["tests"] = tests
                
                model["columns"].append(column_def)
            
            models.append(model)
        
        schema_content = {
            "version": 2,
            "models": models
        }
        
        try:
            with open(filepath, "w") as f:
                yaml.dump(schema_content, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            console.print(f"[green]✅ Generated {filepath}[/green]")
            return filepath
        except Exception as e:
            console.print(f"[red]❌ Failed to generate {filepath}: {e}[/red]")
            return None
    
    def _generate_sources_yml(self, tables: list[dict]) -> Optional[str]:
        """Generate sources.yml file"""
        filepath = os.path.join(self.output_dir, "sources.yml")
        
        # Group tables by schema
        schemas = {}
        for table in tables:
            schema_name = table.get("schema_name", "raw").lower()
            if schema_name not in schemas:
                schemas[schema_name] = []
            schemas[schema_name].append(table)
        
        sources = []
        for schema_name, schema_tables in schemas.items():
            source = {
                "name": schema_name,
                "description": f"Source tables from {schema_name} schema",
                "meta": {
                    "generated_by": "snowlink-ai"
                },
                "tables": []
            }
            
            for table in schema_tables:
                table_def = {
                    "name": table["table_name"].lower(),
                    "description": table.get("description", ""),
                    "columns": [
                        {
                            "name": col["column_name"],
                            "description": col.get("description", "")
                        }
                        for col in table.get("columns", [])
                    ]
                }
                source["tables"].append(table_def)
            
            sources.append(source)
        
        sources_content = {
            "version": 2,
            "sources": sources
        }
        
        try:
            with open(filepath, "w") as f:
                yaml.dump(sources_content, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            console.print(f"[green]✅ Generated {filepath}[/green]")
            return filepath
        except Exception as e:
            console.print(f"[red]❌ Failed to generate {filepath}: {e}[/red]")
            return None
