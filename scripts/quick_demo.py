#!/usr/bin/env python3
"""
Quick demo script - Run this to test snowlink-ai in 5 minutes

This demonstrates the core functionality:
1. Fetches a Confluence page
2. Extracts schema using GPT-4o
3. Writes comments to Snowflake
"""

import os
import sys
import json
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

console = Console()

def main():
    console.print(Panel.fit(
        "[bold cyan]❄️ snowlink-ai Quick Demo[/bold cyan]\n"
        "This script demonstrates the core sync functionality",
        border_style="cyan"
    ))
    
    # Check environment variables
    required_vars = ["OPENAI_API_KEY", "CONFLUENCE_URL", "CONFLUENCE_USER", "CONFLUENCE_API_TOKEN"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        console.print(f"[red]❌ Missing environment variables: {', '.join(missing)}[/red]")
        console.print("[yellow]Please configure your .env file first[/yellow]")
        return
    
    # Demo mode - use sample content if no Confluence access
    demo_content = """
    <h1>Customer Data Model</h1>
    <p>This page documents our customer data warehouse tables.</p>
    
    <h2>DIM_CUSTOMER</h2>
    <p>Master table containing all customer demographic data. Owned by the Data Team.</p>
    <table>
        <tr><th>Column</th><th>Type</th><th>Description</th></tr>
        <tr><td>CUSTOMER_ID</td><td>VARCHAR(50)</td><td>Unique customer identifier from Salesforce</td></tr>
        <tr><td>CUSTOMER_NAME</td><td>VARCHAR(200)</td><td>Full legal name of the customer</td></tr>
        <tr><td>EMAIL</td><td>VARCHAR(100)</td><td>Primary email address for communications</td></tr>
        <tr><td>CREATED_DATE</td><td>TIMESTAMP</td><td>Date when customer was first created</td></tr>
        <tr><td>SEGMENT</td><td>VARCHAR(20)</td><td>Customer segment: ENTERPRISE, SMB, or CONSUMER</td></tr>
    </table>
    
    <h2>FACT_ORDERS</h2>
    <p>Transaction fact table with all customer orders. Owned by Finance Team.</p>
    <table>
        <tr><th>Column</th><th>Type</th><th>Description</th></tr>
        <tr><td>ORDER_ID</td><td>VARCHAR(50)</td><td>Unique order identifier</td></tr>
        <tr><td>CUSTOMER_ID</td><td>VARCHAR(50)</td><td>Foreign key to DIM_CUSTOMER</td></tr>
        <tr><td>ORDER_DATE</td><td>DATE</td><td>Date order was placed</td></tr>
        <tr><td>TOTAL_AMOUNT</td><td>DECIMAL(18,2)</td><td>Total order value in USD</td></tr>
        <tr><td>STATUS</td><td>VARCHAR(20)</td><td>Order status: PENDING, SHIPPED, DELIVERED, CANCELLED</td></tr>
    </table>
    """
    
    console.print("\n[cyan]Step 1: Sample Content (simulating Confluence page)[/cyan]")
    console.print(Panel(demo_content[:500] + "...", title="Sample Content"))
    
    # Extract schema using GPT-4o
    console.print("\n[cyan]Step 2: Extracting schema with GPT-4o...[/cyan]")
    
    from openai import OpenAI
    
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    prompt = open(os.path.join(os.path.dirname(__file__), "..", "prompts", "extract_schema.txt")).read()
    
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": demo_content}
        ]
    )
    
    schema = json.loads(response.choices[0].message.content)
    
    console.print(Panel(
        Syntax(json.dumps(schema, indent=2), "json", theme="monokai"),
        title="[green]Extracted Schema[/green]"
    ))
    
    console.print(f"\n[green]✅ Found {len(schema.get('tables', []))} tables![/green]")
    
    for table in schema.get("tables", []):
        console.print(f"  📊 [bold]{table['table_name']}[/bold] - {len(table.get('columns', []))} columns")
    
    # Show what would be written to Snowflake
    console.print("\n[cyan]Step 3: SQL Commands (would be executed in Snowflake)[/cyan]")
    
    for table in schema.get("tables", []):
        table_name = table["table_name"]
        desc = table.get("description", "")[:100].replace("'", "''")
        console.print(f"[dim]COMMENT ON TABLE {table_name} IS '{desc}';[/dim]")
        
        for col in table.get("columns", [])[:3]:  # Show first 3 columns
            col_name = col["column_name"]
            col_desc = col.get("description", "")[:80].replace("'", "''")
            console.print(f"[dim]COMMENT ON COLUMN {table_name}.{col_name} IS '{col_desc}';[/dim]")
        
        if len(table.get("columns", [])) > 3:
            console.print(f"[dim]... and {len(table['columns']) - 3} more columns[/dim]")
    
    console.print("\n" + "=" * 60)
    console.print("[bold green]✅ Demo complete![/bold green]")
    console.print("\nNext steps:")
    console.print("  1. Configure your .env with Snowflake credentials")
    console.print("  2. Run: python main.py --confluence-page YOUR_PAGE_ID")
    console.print("  3. Watch the magic happen! ✨")


if __name__ == "__main__":
    main()
