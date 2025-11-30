"""
Generate ER diagrams from extracted schema using Mermaid
"""

import os
from datetime import datetime
from typing import Optional
from rich.console import Console

console = Console()


class ERDiagramGenerator:
    """Generate ER diagrams from extracted schema"""
    
    def __init__(self, config: dict):
        self.config = config
        self.enabled = config.get("enabled", True)
        self.format = config.get("format", "mermaid")
        self.output_dir = config.get("output_dir", "output/diagrams")
        self.post_to_confluence = config.get("post_to_confluence", False)
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def generate(self, schema: dict) -> Optional[str]:
        """
        Generate an ER diagram from extracted schema
        
        Args:
            schema: Extracted schema dictionary with tables
            
        Returns:
            Path to the generated diagram file
        """
        if not self.enabled:
            return None
        
        tables = schema.get("tables", [])
        if not tables:
            return None
        
        if self.format == "mermaid":
            return self._generate_mermaid(tables)
        elif self.format == "graphviz":
            return self._generate_graphviz(tables)
        else:
            console.print(f"[red]Unknown diagram format: {self.format}[/red]")
            return None
    
    def _generate_mermaid(self, tables: list[dict]) -> Optional[str]:
        """Generate a Mermaid ER diagram"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"er_diagram_{timestamp}.mmd"
        filepath = os.path.join(self.output_dir, filename)
        
        lines = ["erDiagram"]
        
        # Process each table
        for table in tables:
            table_name = table["table_name"].upper()
            
            # Add table with columns
            lines.append(f"    {table_name} {{")
            
            for col in table.get("columns", []):
                col_name = col["column_name"]
                data_type = col.get("data_type", "string").upper()
                
                # Add key indicators
                key_indicator = ""
                if col.get("primary_key"):
                    key_indicator = " PK"
                elif col.get("foreign_key"):
                    key_indicator = " FK"
                
                # Sanitize description for Mermaid (remove special chars)
                desc = col.get("description", "")[:40].replace('"', "'")
                
                lines.append(f"        {data_type} {col_name}{key_indicator} \"{desc}\"")
            
            lines.append("    }")
            
            # Add relationships
            for rel in table.get("relationships", []):
                lines.append(f"    {table_name} ||--o{{ {rel} : relates")
        
        # If no explicit relationships, try to infer from foreign keys
        for table in tables:
            table_name = table["table_name"].upper()
            for col in table.get("columns", []):
                if col.get("foreign_key"):
                    fk_table = col["foreign_key"].split(".")[0].upper()
                    lines.append(f"    {fk_table} ||--o{{ {table_name} : has")
        
        mermaid_content = "\n".join(lines)
        
        try:
            with open(filepath, "w") as f:
                f.write(mermaid_content)
            console.print(f"[green]✅ Generated Mermaid diagram: {filepath}[/green]")
            
            # Also save the raw content for posting to Confluence
            self._last_diagram_content = mermaid_content
            
            return filepath
        except Exception as e:
            console.print(f"[red]❌ Failed to generate Mermaid diagram: {e}[/red]")
            return None
    
    def _generate_graphviz(self, tables: list[dict]) -> Optional[str]:
        """Generate a Graphviz DOT diagram"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"er_diagram_{timestamp}.dot"
        filepath = os.path.join(self.output_dir, filename)
        
        lines = [
            "digraph ERDiagram {",
            "    graph [rankdir=LR, splines=ortho];",
            "    node [shape=record, fontname=Helvetica, fontsize=10];",
            "    edge [fontname=Helvetica, fontsize=8];",
            ""
        ]
        
        # Process each table as a node
        for table in tables:
            table_name = table["table_name"]
            
            # Build label for table
            label_parts = [f"<table_name> {table_name}"]
            
            for col in table.get("columns", []):
                col_name = col["column_name"]
                data_type = col.get("data_type", "")
                
                key_marker = ""
                if col.get("primary_key"):
                    key_marker = "🔑 "
                elif col.get("foreign_key"):
                    key_marker = "🔗 "
                
                label_parts.append(f"{key_marker}{col_name}: {data_type}")
            
            label = "|".join(label_parts)
            lines.append(f'    {table_name} [label="{{{label}}}"];')
        
        lines.append("")
        
        # Add relationships/edges
        for table in tables:
            table_name = table["table_name"]
            for col in table.get("columns", []):
                if col.get("foreign_key"):
                    fk_parts = col["foreign_key"].split(".")
                    fk_table = fk_parts[0] if fk_parts else ""
                    if fk_table:
                        lines.append(f'    {fk_table} -> {table_name} [label="{col["column_name"]}"];')
        
        lines.append("}")
        
        dot_content = "\n".join(lines)
        
        try:
            with open(filepath, "w") as f:
                f.write(dot_content)
            console.print(f"[green]✅ Generated Graphviz diagram: {filepath}[/green]")
            
            self._last_diagram_content = dot_content
            
            return filepath
        except Exception as e:
            console.print(f"[red]❌ Failed to generate Graphviz diagram: {e}[/red]")
            return None
    
    def get_last_diagram_content(self) -> Optional[str]:
        """Get the content of the last generated diagram"""
        return getattr(self, "_last_diagram_content", None)
    
    def generate_preview(self, schema: dict) -> str:
        """
        Generate a text-based preview of the ER diagram
        
        Args:
            schema: Extracted schema dictionary
            
        Returns:
            ASCII representation of the schema
        """
        lines = ["=" * 60, "ER Diagram Preview", "=" * 60, ""]
        
        for table in schema.get("tables", []):
            table_name = table["table_name"]
            desc = table.get("description", "")[:50]
            
            lines.append(f"┌{'─' * 58}┐")
            lines.append(f"│ {table_name:<56} │")
            if desc:
                lines.append(f"│ {desc:<56} │")
            lines.append(f"├{'─' * 58}┤")
            
            for col in table.get("columns", []):
                col_name = col["column_name"]
                data_type = col.get("data_type", "???")
                
                marker = "  "
                if col.get("primary_key"):
                    marker = "PK"
                elif col.get("foreign_key"):
                    marker = "FK"
                
                col_desc = col.get("description", "")[:30]
                line = f"│ {marker} {col_name:<20} {data_type:<10} {col_desc:<20}│"
                lines.append(line[:60] + "│")
            
            lines.append(f"└{'─' * 58}┘")
            lines.append("")
        
        return "\n".join(lines)
