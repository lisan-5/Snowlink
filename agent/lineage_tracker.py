"""
Data lineage tracking for understanding relationships between tables,
documentation, and transformations
"""

import os
import json
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, asdict
from rich.console import Console
from rich.tree import Tree

console = Console()


@dataclass
class LineageNode:
    """A node in the lineage graph"""
    id: str
    type: str  # table, column, document, transformation
    name: str
    metadata: dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class LineageEdge:
    """An edge connecting two lineage nodes"""
    source_id: str
    target_id: str
    relationship: str  # derives_from, documented_in, transforms_to, references
    metadata: dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


class LineageTracker:
    """
    Track data lineage between documentation, tables, and transformations.
    Enables impact analysis and understanding of data flow.
    """
    
    def __init__(self, storage_path: str = "data/lineage"):
        self.storage_path = storage_path
        self.nodes_file = os.path.join(storage_path, "nodes.json")
        self.edges_file = os.path.join(storage_path, "edges.json")
        
        os.makedirs(storage_path, exist_ok=True)
        
        self.nodes: dict[str, LineageNode] = {}
        self.edges: list[LineageEdge] = []
        
        self._load()
    
    def _load(self):
        """Load lineage data from disk"""
        if os.path.exists(self.nodes_file):
            with open(self.nodes_file, "r") as f:
                data = json.load(f)
                self.nodes = {k: LineageNode(**v) for k, v in data.items()}
        
        if os.path.exists(self.edges_file):
            with open(self.edges_file, "r") as f:
                data = json.load(f)
                self.edges = [LineageEdge(**e) for e in data]
    
    def _save(self):
        """Save lineage data to disk"""
        with open(self.nodes_file, "w") as f:
            json.dump({k: asdict(v) for k, v in self.nodes.items()}, f, indent=2)
        
        with open(self.edges_file, "w") as f:
            json.dump([asdict(e) for e in self.edges], f, indent=2)
    
    def add_table(
        self,
        table_name: str,
        database: str = "ANALYTICS",
        schema: str = "PUBLIC",
        owner: Optional[str] = None,
        description: Optional[str] = None
    ) -> str:
        """Add a table node to the lineage graph"""
        node_id = f"table:{database}.{schema}.{table_name}".upper()
        
        self.nodes[node_id] = LineageNode(
            id=node_id,
            type="table",
            name=table_name.upper(),
            metadata={
                "database": database,
                "schema": schema,
                "owner": owner,
                "description": description
            }
        )
        
        self._save()
        return node_id
    
    def add_column(
        self,
        table_name: str,
        column_name: str,
        data_type: Optional[str] = None,
        database: str = "ANALYTICS",
        schema: str = "PUBLIC"
    ) -> str:
        """Add a column node to the lineage graph"""
        node_id = f"column:{database}.{schema}.{table_name}.{column_name}".upper()
        table_id = f"table:{database}.{schema}.{table_name}".upper()
        
        self.nodes[node_id] = LineageNode(
            id=node_id,
            type="column",
            name=column_name.upper(),
            metadata={
                "table": table_name.upper(),
                "data_type": data_type,
                "database": database,
                "schema": schema
            }
        )
        
        # Add relationship to parent table
        self.add_edge(node_id, table_id, "belongs_to")
        
        self._save()
        return node_id
    
    def add_document(
        self,
        source_type: str,
        source_id: str,
        title: str,
        url: Optional[str] = None
    ) -> str:
        """Add a documentation node to the lineage graph"""
        node_id = f"doc:{source_type}:{source_id}"
        
        self.nodes[node_id] = LineageNode(
            id=node_id,
            type="document",
            name=title,
            metadata={
                "source_type": source_type,
                "source_id": source_id,
                "url": url
            }
        )
        
        self._save()
        return node_id
    
    def add_transformation(
        self,
        name: str,
        transformation_type: str = "dbt",
        file_path: Optional[str] = None
    ) -> str:
        """Add a transformation node (dbt model, SQL script, etc.)"""
        node_id = f"transform:{transformation_type}:{name}"
        
        self.nodes[node_id] = LineageNode(
            id=node_id,
            type="transformation",
            name=name,
            metadata={
                "transformation_type": transformation_type,
                "file_path": file_path
            }
        )
        
        self._save()
        return node_id
    
    def add_edge(
        self,
        source_id: str,
        target_id: str,
        relationship: str,
        metadata: Optional[dict] = None
    ):
        """Add a relationship between two nodes"""
        # Check if edge already exists
        for edge in self.edges:
            if edge.source_id == source_id and edge.target_id == target_id:
                return
        
        self.edges.append(LineageEdge(
            source_id=source_id,
            target_id=target_id,
            relationship=relationship,
            metadata=metadata or {}
        ))
        
        self._save()
    
    def link_table_to_document(
        self,
        table_name: str,
        source_type: str,
        source_id: str,
        database: str = "ANALYTICS",
        schema: str = "PUBLIC"
    ):
        """Link a table to its documentation source"""
        table_id = f"table:{database}.{schema}.{table_name}".upper()
        doc_id = f"doc:{source_type}:{source_id}"
        
        self.add_edge(table_id, doc_id, "documented_in")
    
    def link_transformation(
        self,
        transformation_name: str,
        source_tables: list[str],
        target_tables: list[str],
        transformation_type: str = "dbt"
    ):
        """Link a transformation to its source and target tables"""
        transform_id = f"transform:{transformation_type}:{transformation_name}"
        
        for source in source_tables:
            self.add_edge(transform_id, f"table:ANALYTICS.PUBLIC.{source}".upper(), "reads_from")
        
        for target in target_tables:
            self.add_edge(f"table:ANALYTICS.PUBLIC.{target}".upper(), transform_id, "derives_from")
    
    def get_upstream(self, node_id: str, depth: int = 3) -> list[dict]:
        """Get all upstream dependencies of a node"""
        visited = set()
        result = []
        
        def traverse(current_id: str, current_depth: int):
            if current_depth > depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            for edge in self.edges:
                if edge.target_id == current_id:
                    if edge.source_id in self.nodes:
                        node = self.nodes[edge.source_id]
                        result.append({
                            "id": node.id,
                            "name": node.name,
                            "type": node.type,
                            "relationship": edge.relationship,
                            "depth": current_depth
                        })
                        traverse(edge.source_id, current_depth + 1)
        
        traverse(node_id, 1)
        return result
    
    def get_downstream(self, node_id: str, depth: int = 3) -> list[dict]:
        """Get all downstream dependencies of a node"""
        visited = set()
        result = []
        
        def traverse(current_id: str, current_depth: int):
            if current_depth > depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            for edge in self.edges:
                if edge.source_id == current_id:
                    if edge.target_id in self.nodes:
                        node = self.nodes[edge.target_id]
                        result.append({
                            "id": node.id,
                            "name": node.name,
                            "type": node.type,
                            "relationship": edge.relationship,
                            "depth": current_depth
                        })
                        traverse(edge.target_id, current_depth + 1)
        
        traverse(node_id, 1)
        return result
    
    def impact_analysis(self, table_name: str, database: str = "ANALYTICS", schema: str = "PUBLIC") -> dict:
        """
        Perform impact analysis on a table.
        Shows what would be affected if this table changes.
        """
        table_id = f"table:{database}.{schema}.{table_name}".upper()
        
        downstream = self.get_downstream(table_id)
        
        # Categorize impacts
        impacted_tables = [d for d in downstream if d["type"] == "table"]
        impacted_transforms = [d for d in downstream if d["type"] == "transformation"]
        impacted_docs = [d for d in downstream if d["type"] == "document"]
        
        return {
            "table": table_name,
            "total_impacted": len(downstream),
            "impacted_tables": impacted_tables,
            "impacted_transformations": impacted_transforms,
            "impacted_documents": impacted_docs
        }
    
    def visualize(self, node_id: Optional[str] = None) -> str:
        """Generate a text visualization of the lineage graph"""
        tree = Tree(f"[bold cyan]Data Lineage Graph[/bold cyan]")
        
        if node_id:
            # Show specific node's lineage
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node_tree = tree.add(f"[bold]{node.name}[/bold] ({node.type})")
                
                # Add upstream
                upstream_tree = node_tree.add("[yellow]Upstream[/yellow]")
                for dep in self.get_upstream(node_id):
                    upstream_tree.add(f"{dep['name']} ({dep['type']}) - {dep['relationship']}")
                
                # Add downstream
                downstream_tree = node_tree.add("[green]Downstream[/green]")
                for dep in self.get_downstream(node_id):
                    downstream_tree.add(f"{dep['name']} ({dep['type']}) - {dep['relationship']}")
        else:
            # Show all tables
            tables_tree = tree.add("[bold]Tables[/bold]")
            for node_id, node in self.nodes.items():
                if node.type == "table":
                    tables_tree.add(f"{node.name}")
        
        # Return as string for display
        from io import StringIO
        output = StringIO()
        temp_console = Console(file=output, force_terminal=True)
        temp_console.print(tree)
        return output.getvalue()
    
    def export_graphviz(self) -> str:
        """Export lineage as Graphviz DOT format"""
        lines = [
            "digraph Lineage {",
            "    rankdir=LR;",
            "    node [shape=box];",
            ""
        ]
        
        # Add nodes with colors by type
        colors = {
            "table": "#3b82f6",
            "column": "#94a3b8",
            "document": "#22c55e",
            "transformation": "#f59e0b"
        }
        
        for node_id, node in self.nodes.items():
            color = colors.get(node.type, "#6b7280")
            safe_id = node_id.replace(":", "_").replace(".", "_")
            lines.append(f'    {safe_id} [label="{node.name}" style=filled fillcolor="{color}"];')
        
        lines.append("")
        
        # Add edges
        for edge in self.edges:
            source_safe = edge.source_id.replace(":", "_").replace(".", "_")
            target_safe = edge.target_id.replace(":", "_").replace(".", "_")
            lines.append(f'    {source_safe} -> {target_safe} [label="{edge.relationship}"];')
        
        lines.append("}")
        
        return "\n".join(lines)
    
    def get_stats(self) -> dict:
        """Get statistics about the lineage graph"""
        type_counts = {}
        for node in self.nodes.values():
            type_counts[node.type] = type_counts.get(node.type, 0) + 1
        
        relationship_counts = {}
        for edge in self.edges:
            relationship_counts[edge.relationship] = relationship_counts.get(edge.relationship, 0) + 1
        
        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "nodes_by_type": type_counts,
            "edges_by_relationship": relationship_counts
        }
