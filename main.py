#!/usr/bin/env python3
"""
snowlink-ai - Intelligent bi-directional sync between Atlassian and Snowflake
Advanced CLI with full feature support
"""

import os
import sys
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from dotenv import load_dotenv
import yaml

from agent.orchestrator import SyncOrchestrator
from agent.audit_log import AuditLogger

load_dotenv()

app = typer.Typer(
    name="snowlink-ai",
    help="Intelligent bi-directional sync between Atlassian and Snowflake",
    add_completion=False,
)
console = Console()


def load_config() -> dict:
    """Load configuration from config.yaml"""
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def print_banner():
    """Print the application banner"""
    banner = """
[bold cyan]╔═══════════════════════════════════════════════════════════════╗
║                                                                 ║
║   ❄️  [bold white]snowlink-ai[/bold white] v2.0                                       ║
║                                                                 ║
║   Intelligent bi-directional sync between                       ║
║   Atlassian (Jira + Confluence) ↔ Snowflake                    ║
║                                                                 ║
║   [dim]Features: Multi-LLM | Vector Search | Lineage Tracking[/dim]     ║
║   [dim]          Schema Drift | Data Quality | Audit Logging[/dim]      ║
║                                                                 ║
╚═══════════════════════════════════════════════════════════════╝[/bold cyan]
    """
    console.print(banner)


@app.command()
def sync(
    confluence_page: str = typer.Option(None, "--confluence-page", "-c", help="Sync a specific Confluence page ID"),
    jira_issue: str = typer.Option(None, "--jira-issue", "-j", help="Sync a specific Jira issue key"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Preview changes without writing to Snowflake"),
    skip_drift: bool = typer.Option(False, "--skip-drift", help="Skip schema drift detection"),
    skip_quality: bool = typer.Option(False, "--skip-quality", help="Skip data quality checks"),
    post_diagram: bool = typer.Option(False, "--post-diagram", help="Post ER diagram back to Confluence"),
    watch: bool = typer.Option(False, "--watch", "-w", help="Enable continuous watch mode"),
    web: bool = typer.Option(False, "--web", help="Start web dashboard"),
):
    """
    Main sync command - process Atlassian content and sync to Snowflake
    """
    print_banner()
    config = load_config()
    
    if dry_run:
        console.print("[yellow]Dry run mode enabled - no changes will be written to Snowflake[/yellow]\n")

    # Initialize orchestrator
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task(description="Initializing components...", total=None)
        orchestrator = SyncOrchestrator(config)

    # Handle specific page/issue sync
    if confluence_page:
        console.print(f"\n[cyan]Processing Confluence page: {confluence_page}[/cyan]")
        result = orchestrator.sync_confluence_page(
            confluence_page,
            dry_run=dry_run,
            skip_drift_check=skip_drift,
            skip_quality_check=skip_quality,
            post_diagram=post_diagram
        )
        display_sync_result(result)
        return

    if jira_issue:
        console.print(f"\n[cyan]Processing Jira issue: {jira_issue}[/cyan]")
        result = orchestrator.sync_jira_issue(
            jira_issue,
            dry_run=dry_run,
            skip_drift_check=skip_drift
        )
        display_sync_result(result)
        return

    # Start web dashboard
    if web:
        console.print("\n[cyan]Starting web dashboard...[/cyan]")
        start_web_dashboard(config)
        return

    # Watch mode
    if watch:
        console.print("\n[cyan]Starting watch mode...[/cyan]")
        start_watch_mode(config, orchestrator)
        return

    # Interactive mode
    interactive_mode(config, orchestrator)


def display_sync_result(result):
    """Display sync result in a nice table"""
    table = Table(title="Sync Result", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    
    status_color = "green" if result.success else "red"
    table.add_row("Status", f"[{status_color}]{'Success' if result.success else 'Failed'}[/{status_color}]")
    table.add_row("Source", f"{result.source_type}:{result.source_id}")
    table.add_row("Tables Found", str(result.tables_found))
    table.add_row("Tables Updated", str(result.tables_updated))
    table.add_row("Columns Updated", str(result.columns_updated))
    table.add_row("Drift Issues", str(result.drift_issues))
    table.add_row("Quality Failures", str(result.quality_failures))
    table.add_row("Duration", f"{result.duration_seconds:.2f}s")
    
    console.print(table)
    
    if result.errors:
        console.print("\n[red]Errors:[/red]")
        for error in result.errors:
            console.print(f"  - {error}")
    
    if result.warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for warning in result.warnings:
            console.print(f"  - {warning}")


@app.command()
def batch(
    confluence_pages: str = typer.Option(None, "--confluence", "-c", help="Comma-separated Confluence page IDs"),
    jira_issues: str = typer.Option(None, "--jira", "-j", help="Comma-separated Jira issue keys"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Preview changes"),
    parallel: bool = typer.Option(True, "--parallel/--sequential", help="Run in parallel or sequential"),
):
    """
    Batch sync multiple sources
    """
    print_banner()
    config = load_config()
    orchestrator = SyncOrchestrator(config)
    
    pages = confluence_pages.split(",") if confluence_pages else None
    issues = jira_issues.split(",") if jira_issues else None
    
    if not pages and not issues:
        console.print("[red]Please specify at least one source to sync[/red]")
        return
    
    result = orchestrator.batch_sync(
        confluence_pages=pages,
        jira_issues=issues,
        dry_run=dry_run,
        parallel=parallel
    )
    
    console.print(Panel(
        f"[green]Batch sync complete![/green]\n\n"
        f"Total: {result.total_sources}\n"
        f"Successful: {result.successful}\n"
        f"Failed: {result.failed}\n"
        f"Duration: {result.duration_seconds:.2f}s",
        title="Batch Result"
    ))


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    n_results: int = typer.Option(5, "--limit", "-n", help="Number of results"),
):
    """
    Search indexed documentation using semantic search
    """
    config = load_config()
    orchestrator = SyncOrchestrator(config)
    
    if not orchestrator.vector_store:
        console.print("[red]Vector store not enabled in configuration[/red]")
        return
    
    results = orchestrator.search_documentation(query, n_results)
    
    if not results:
        console.print("[yellow]No results found[/yellow]")
        return
    
    for i, result in enumerate(results, 1):
        console.print(Panel(
            f"[dim]Source:[/dim] {result['source_type']}:{result['source_id']}\n"
            f"[dim]Similarity:[/dim] {result['similarity']:.1%}\n\n"
            f"{result['content'][:300]}...",
            title=f"Result {i}"
        ))


@app.command()
def lineage(
    table_name: str = typer.Argument(..., help="Table name to analyze"),
):
    """
    View data lineage for a table
    """
    config = load_config()
    orchestrator = SyncOrchestrator(config)
    
    if not orchestrator.lineage:
        console.print("[red]Lineage tracking not enabled in configuration[/red]")
        return
    
    result = orchestrator.get_table_lineage(table_name)
    
    console.print(f"\n[bold cyan]Lineage for {table_name}[/bold cyan]\n")
    
    if result.get("upstream"):
        console.print("[bold]Upstream Dependencies:[/bold]")
        for dep in result["upstream"]:
            console.print(f"  ← {dep['name']} ({dep['type']}) - {dep['relationship']}")
    
    if result.get("downstream"):
        console.print("\n[bold]Downstream Dependencies:[/bold]")
        for dep in result["downstream"]:
            console.print(f"  → {dep['name']} ({dep['type']}) - {dep['relationship']}")
    
    if result.get("impact_analysis"):
        impact = result["impact_analysis"]
        console.print(f"\n[bold]Impact Analysis:[/bold]")
        console.print(f"  Total impacted: {impact['total_impacted']}")
        console.print(f"  Tables: {len(impact['impacted_tables'])}")
        console.print(f"  Transformations: {len(impact['impacted_transformations'])}")


@app.command()
def quality(
    table_names: str = typer.Option(None, "--tables", "-t", help="Comma-separated table names"),
):
    """
    Run data quality checks
    """
    config = load_config()
    orchestrator = SyncOrchestrator(config)
    
    if not orchestrator.quality_checker:
        console.print("[red]Data quality checker not enabled in configuration[/red]")
        return
    
    tables = table_names.split(",") if table_names else None
    result = orchestrator.run_quality_checks(tables)
    
    table = Table(title="Quality Check Results", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("Total Checks", str(result.get("total_checks", 0)))
    table.add_row("Passed", f"[green]{result.get('passed', 0)}[/green]")
    table.add_row("Failed", f"[red]{result.get('failed', 0)}[/red]")
    table.add_row("Warnings", f"[yellow]{result.get('warnings', 0)}[/yellow]")
    
    console.print(table)


@app.command()
def audit(
    days: int = typer.Option(30, "--days", "-d", help="Number of days to show"),
    export: str = typer.Option(None, "--export", "-e", help="Export to CSV file"),
):
    """
    View or export audit log
    """
    config = load_config()
    audit_logger = AuditLogger(config.get("audit", {}).get("db_path", "data/audit.db"))
    
    if export:
        audit_logger.export_csv(export, days)
        return
    
    stats = audit_logger.get_stats(days)
    
    table = Table(title=f"Audit Statistics (Last {days} Days)", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("Total Syncs", str(stats.get("total_syncs", 0)))
    table.add_row("Failed Syncs", str(stats.get("failed_syncs", 0)))
    table.add_row("Success Rate", f"{stats.get('success_rate', 0)}%")
    table.add_row("Tables Updated", str(stats.get("tables_updated", 0)))
    
    console.print(table)
    
    console.print("\n[bold]Recent Activity:[/bold]")
    audit_logger.display_history(10)


@app.command()
def test_connection():
    """Test connections to all services"""
    print_banner()
    console.print("\n[bold]Testing connections...[/bold]\n")
    
    config = load_config()
    
    # Test OpenAI
    with console.status("[cyan]Testing OpenAI connection...[/cyan]"):
        try:
            from openai import OpenAI
            client = OpenAI()
            client.models.list()
            console.print("[green]OpenAI: Connected[/green]")
        except Exception as e:
            console.print(f"[red]OpenAI: {str(e)}[/red]")
    
    # Test other connections via orchestrator
    orchestrator = SyncOrchestrator(config)
    
    with console.status("[cyan]Testing Confluence...[/cyan]"):
        if orchestrator.confluence.test_connection():
            console.print("[green]Confluence: Connected[/green]")
        else:
            console.print("[red]Confluence: Failed[/red]")
    
    with console.status("[cyan]Testing Jira...[/cyan]"):
        if orchestrator.jira.test_connection():
            console.print("[green]Jira: Connected[/green]")
        else:
            console.print("[red]Jira: Failed[/red]")
    
    with console.status("[cyan]Testing Snowflake...[/cyan]"):
        if orchestrator.snowflake.test_connection():
            console.print("[green]Snowflake: Connected[/green]")
        else:
            console.print("[red]Snowflake: Failed[/red]")
    
    # Check optional features
    console.print("\n[bold]Optional Features:[/bold]")
    console.print(f"  Vector Store: {'[green]Enabled[/green]' if orchestrator.vector_store else '[dim]Disabled[/dim]'}")
    console.print(f"  Lineage Tracking: {'[green]Enabled[/green]' if orchestrator.lineage else '[dim]Disabled[/dim]'}")
    console.print(f"  Drift Detection: {'[green]Enabled[/green]' if orchestrator.drift_detector else '[dim]Disabled[/dim]'}")
    console.print(f"  Quality Checks: {'[green]Enabled[/green]' if orchestrator.quality_checker else '[dim]Disabled[/dim]'}")
    console.print(f"  Notifications: {'[green]Enabled[/green]' if orchestrator.notifications else '[dim]Disabled[/dim]'}")
    console.print(f"  Audit Logging: {'[green]Enabled[/green]' if orchestrator.audit else '[dim]Disabled[/dim]'}")


def start_watch_mode(config, orchestrator):
    """Start continuous watch mode"""
    from apscheduler.schedulers.blocking import BlockingScheduler
    
    scheduler = BlockingScheduler()
    interval = config["sync"].get("interval_seconds", 300)
    
    def sync_job():
        console.print(f"\n[cyan]Running sync at {__import__('datetime').datetime.now().isoformat()}[/cyan]")
        result = orchestrator.run_full_sync()
        console.print(f"[green]Synced {result.successful}/{result.total_sources} sources[/green]")
    
    scheduler.add_job(sync_job, 'interval', seconds=interval)
    console.print(f"[green]Watch mode started - syncing every {interval} seconds[/green]")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        console.print("\n[yellow]Watch mode stopped[/yellow]")


def start_web_dashboard(config):
    """Start the FastAPI web dashboard"""
    import uvicorn
    from agent.web_dashboard import create_app
    
    app = create_app(config)
    host = config.get("api", {}).get("host", "0.0.0.0")
    port = config.get("api", {}).get("port", 8000)
    
    console.print(f"[green]Dashboard available at http://{host}:{port}[/green]")
    uvicorn.run(app, host=host, port=port)


def interactive_mode(config, orchestrator):
    """Interactive CLI mode"""
    console.print("\n[bold]Select an option:[/bold]")
    console.print("  1. Sync specific Confluence page")
    console.print("  2. Sync specific Jira issue")
    console.print("  3. Run full sync")
    console.print("  4. Search documentation")
    console.print("  5. View table lineage")
    console.print("  6. Run quality checks")
    console.print("  7. View audit log")
    console.print("  8. Start watch mode")
    console.print("  9. Start web dashboard")
    console.print("  0. Exit")
    
    choice = typer.prompt("\nEnter choice", default="1")
    
    if choice == "1":
        page_id = typer.prompt("Enter Confluence page ID")
        result = orchestrator.sync_confluence_page(page_id)
        display_sync_result(result)
    elif choice == "2":
        issue_key = typer.prompt("Enter Jira issue key (e.g., PROJ-123)")
        result = orchestrator.sync_jira_issue(issue_key)
        display_sync_result(result)
    elif choice == "3":
        result = orchestrator.run_full_sync()
        console.print(f"[green]Full sync complete: {result.successful}/{result.total_sources}[/green]")
    elif choice == "4":
        query = typer.prompt("Enter search query")
        results = orchestrator.search_documentation(query)
        for r in results:
            console.print(f"- {r['source_type']}:{r['source_id']} ({r['similarity']:.1%})")
    elif choice == "5":
        table_name = typer.prompt("Enter table name")
        result = orchestrator.get_table_lineage(table_name)
        console.print(result)
    elif choice == "6":
        result = orchestrator.run_quality_checks()
        console.print(f"Passed: {result.get('passed', 0)}, Failed: {result.get('failed', 0)}")
    elif choice == "7":
        if orchestrator.audit:
            orchestrator.audit.display_history()
    elif choice == "8":
        start_watch_mode(config, orchestrator)
    elif choice == "9":
        start_web_dashboard(config)
    else:
        console.print("[yellow]Goodbye![/yellow]")
        sys.exit(0)


if __name__ == "__main__":
    app()
