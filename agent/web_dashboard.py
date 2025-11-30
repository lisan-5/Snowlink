"""
Advanced FastAPI web dashboard with real-time updates via WebSocket
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Import orchestrator and components
from .orchestrator import SyncOrchestrator


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass


manager = ConnectionManager()


# Request/Response models
class SyncRequest(BaseModel):
    source_type: str
    source_id: str
    dry_run: bool = False
    skip_drift_check: bool = False
    post_diagram: bool = False


class BatchSyncRequest(BaseModel):
    confluence_pages: Optional[list[str]] = None
    jira_issues: Optional[list[str]] = None
    dry_run: bool = False
    parallel: bool = True


class SearchRequest(BaseModel):
    query: str
    n_results: int = 5
    source_type: Optional[str] = None


class QualityCheckRequest(BaseModel):
    table_names: Optional[list[str]] = None


def create_app(config: dict) -> FastAPI:
    """Create and configure the advanced FastAPI application"""
    
    orchestrator = SyncOrchestrator(config)
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        yield
        # Shutdown
        orchestrator.close()
    
    app = FastAPI(
        title="snowlink-ai",
        description="Intelligent bi-directional sync between Atlassian and Snowflake",
        version="2.0.0",
        lifespan=lifespan
    )
    
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.get("api", {}).get("cors_origins", ["*"]),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Dashboard HTML
    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>snowlink-ai Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <style>
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .pulse { animation: pulse 2s infinite; }
        .glass { background: rgba(15, 23, 42, 0.8); backdrop-filter: blur(12px); }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
    <div class="container mx-auto px-6 py-8 max-w-7xl">
        <!-- Header -->
        <header class="flex items-center justify-between mb-8">
            <div class="flex items-center gap-3">
                <div class="w-10 h-10 bg-gradient-to-br from-cyan-400 to-blue-500 rounded-lg flex items-center justify-center">
                    <i data-lucide="snowflake" class="w-6 h-6"></i>
                </div>
                <div>
                    <h1 class="text-2xl font-bold">snowlink-ai</h1>
                    <p class="text-slate-400 text-sm">Atlassian ↔ Snowflake Sync</p>
                </div>
            </div>
            <div id="connection-status" class="flex items-center gap-2 px-3 py-1.5 rounded-full bg-slate-800">
                <span class="w-2 h-2 rounded-full bg-yellow-400 pulse"></span>
                <span class="text-sm">Connecting...</span>
            </div>
        </header>

        <!-- Stats Grid -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
            <div class="glass rounded-xl p-5 border border-slate-800">
                <div class="flex items-center justify-between">
                    <span class="text-slate-400 text-sm">Total Syncs (30d)</span>
                    <i data-lucide="refresh-cw" class="w-4 h-4 text-cyan-400"></i>
                </div>
                <p id="stat-syncs" class="text-3xl font-bold mt-2">-</p>
            </div>
            <div class="glass rounded-xl p-5 border border-slate-800">
                <div class="flex items-center justify-between">
                    <span class="text-slate-400 text-sm">Success Rate</span>
                    <i data-lucide="check-circle" class="w-4 h-4 text-green-400"></i>
                </div>
                <p id="stat-success" class="text-3xl font-bold mt-2">-</p>
            </div>
            <div class="glass rounded-xl p-5 border border-slate-800">
                <div class="flex items-center justify-between">
                    <span class="text-slate-400 text-sm">Tables Updated</span>
                    <i data-lucide="database" class="w-4 h-4 text-blue-400"></i>
                </div>
                <p id="stat-tables" class="text-3xl font-bold mt-2">-</p>
            </div>
            <div class="glass rounded-xl p-5 border border-slate-800">
                <div class="flex items-center justify-between">
                    <span class="text-slate-400 text-sm">Drift Issues</span>
                    <i data-lucide="alert-triangle" class="w-4 h-4 text-yellow-400"></i>
                </div>
                <p id="stat-drift" class="text-3xl font-bold mt-2">-</p>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <!-- Sync Panel -->
            <div class="lg:col-span-2">
                <div class="glass rounded-xl border border-slate-800 overflow-hidden">
                    <div class="p-5 border-b border-slate-800">
                        <h2 class="font-semibold flex items-center gap-2">
                            <i data-lucide="play-circle" class="w-5 h-5 text-cyan-400"></i>
                            Manual Sync
                        </h2>
                    </div>
                    <div class="p-5">
                        <form id="sync-form" class="space-y-4">
                            <div class="grid grid-cols-2 gap-4">
                                <div>
                                    <label class="block text-sm text-slate-400 mb-1.5">Source Type</label>
                                    <select id="source-type" class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 focus:ring-2 focus:ring-cyan-400 focus:border-transparent">
                                        <option value="confluence">Confluence Page</option>
                                        <option value="jira">Jira Issue</option>
                                    </select>
                                </div>
                                <div>
                                    <label class="block text-sm text-slate-400 mb-1.5">Source ID</label>
                                    <input type="text" id="source-id" placeholder="12345678 or PROJ-123" 
                                        class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 focus:ring-2 focus:ring-cyan-400 focus:border-transparent">
                                </div>
                            </div>
                            <div class="flex items-center gap-6">
                                <label class="flex items-center gap-2 cursor-pointer">
                                    <input type="checkbox" id="dry-run" class="rounded bg-slate-800 border-slate-700 text-cyan-400 focus:ring-cyan-400">
                                    <span class="text-sm">Dry Run</span>
                                </label>
                                <label class="flex items-center gap-2 cursor-pointer">
                                    <input type="checkbox" id="post-diagram" class="rounded bg-slate-800 border-slate-700 text-cyan-400 focus:ring-cyan-400">
                                    <span class="text-sm">Post ER Diagram</span>
                                </label>
                            </div>
                            <button type="submit" class="w-full bg-gradient-to-r from-cyan-500 to-blue-500 text-white font-medium py-2.5 px-4 rounded-lg hover:opacity-90 transition flex items-center justify-center gap-2">
                                <i data-lucide="zap" class="w-4 h-4"></i>
                                Start Sync
                            </button>
                        </form>
                        <div id="sync-result" class="mt-4 hidden"></div>
                    </div>
                </div>

                <!-- Recent Activity -->
                <div class="glass rounded-xl border border-slate-800 overflow-hidden mt-6">
                    <div class="p-5 border-b border-slate-800 flex items-center justify-between">
                        <h2 class="font-semibold flex items-center gap-2">
                            <i data-lucide="activity" class="w-5 h-5 text-cyan-400"></i>
                            Recent Activity
                        </h2>
                        <button onclick="loadHistory()" class="text-sm text-cyan-400 hover:text-cyan-300">Refresh</button>
                    </div>
                    <div id="activity-feed" class="divide-y divide-slate-800 max-h-96 overflow-y-auto">
                        <div class="p-5 text-center text-slate-500">Loading...</div>
                    </div>
                </div>
            </div>

            <!-- Sidebar -->
            <div class="space-y-6">
                <!-- Search -->
                <div class="glass rounded-xl border border-slate-800 overflow-hidden">
                    <div class="p-5 border-b border-slate-800">
                        <h2 class="font-semibold flex items-center gap-2">
                            <i data-lucide="search" class="w-5 h-5 text-cyan-400"></i>
                            Semantic Search
                        </h2>
                    </div>
                    <div class="p-5">
                        <form id="search-form">
                            <input type="text" id="search-query" placeholder="Search documentation..." 
                                class="w-full bg-slate-800 border border-slate-700 rounded-lg px-4 py-2.5 focus:ring-2 focus:ring-cyan-400 focus:border-transparent mb-3">
                            <button type="submit" class="w-full bg-slate-700 hover:bg-slate-600 py-2 rounded-lg transition">Search</button>
                        </form>
                        <div id="search-results" class="mt-4 space-y-2"></div>
                    </div>
                </div>

                <!-- Connection Status -->
                <div class="glass rounded-xl border border-slate-800 overflow-hidden">
                    <div class="p-5 border-b border-slate-800">
                        <h2 class="font-semibold flex items-center gap-2">
                            <i data-lucide="plug" class="w-5 h-5 text-cyan-400"></i>
                            Connections
                        </h2>
                    </div>
                    <div id="connections" class="p-5 space-y-3">
                        <div class="animate-pulse space-y-2">
                            <div class="h-4 bg-slate-800 rounded w-3/4"></div>
                            <div class="h-4 bg-slate-800 rounded w-1/2"></div>
                        </div>
                    </div>
                </div>

                <!-- Quick Actions -->
                <div class="glass rounded-xl border border-slate-800 overflow-hidden">
                    <div class="p-5 border-b border-slate-800">
                        <h2 class="font-semibold flex items-center gap-2">
                            <i data-lucide="terminal" class="w-5 h-5 text-cyan-400"></i>
                            Quick Actions
                        </h2>
                    </div>
                    <div class="p-5 space-y-2">
                        <button onclick="runFullSync()" class="w-full text-left px-4 py-2.5 bg-slate-800 hover:bg-slate-700 rounded-lg transition flex items-center gap-2">
                            <i data-lucide="refresh-cw" class="w-4 h-4"></i>
                            Full Sync
                        </button>
                        <button onclick="runQualityChecks()" class="w-full text-left px-4 py-2.5 bg-slate-800 hover:bg-slate-700 rounded-lg transition flex items-center gap-2">
                            <i data-lucide="shield-check" class="w-4 h-4"></i>
                            Quality Checks
                        </button>
                        <button onclick="checkDrift()" class="w-full text-left px-4 py-2.5 bg-slate-800 hover:bg-slate-700 rounded-lg transition flex items-center gap-2">
                            <i data-lucide="git-compare" class="w-4 h-4"></i>
                            Check Drift
                        </button>
                        <button onclick="viewLineage()" class="w-full text-left px-4 py-2.5 bg-slate-800 hover:bg-slate-700 rounded-lg transition flex items-center gap-2">
                            <i data-lucide="git-branch" class="w-4 h-4"></i>
                            View Lineage
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Initialize Lucide icons
        lucide.createIcons();

        // WebSocket connection
        let ws;
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                document.getElementById('connection-status').innerHTML = `
                    <span class="w-2 h-2 rounded-full bg-green-400"></span>
                    <span class="text-sm">Connected</span>
                `;
            };
            
            ws.onclose = () => {
                document.getElementById('connection-status').innerHTML = `
                    <span class="w-2 h-2 rounded-full bg-red-400"></span>
                    <span class="text-sm">Disconnected</span>
                `;
                setTimeout(connectWebSocket, 3000);
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                handleWebSocketMessage(data);
            };
        }
        
        function handleWebSocketMessage(data) {
            if (data.type === 'sync_update') {
                loadHistory();
                loadStats();
            }
        }
        
        // Load stats
        async function loadStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('stat-syncs').textContent = data.total_syncs || 0;
                document.getElementById('stat-success').textContent = (data.success_rate || 0) + '%';
                document.getElementById('stat-tables').textContent = data.tables_updated || 0;
                document.getElementById('stat-drift').textContent = data.drift_issues || 0;
            } catch (e) {
                console.error('Failed to load stats:', e);
            }
        }
        
        // Load connection status
        async function loadConnections() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                const container = document.getElementById('connections');
                container.innerHTML = Object.entries(data).map(([name, status]) => `
                    <div class="flex items-center justify-between">
                        <span class="capitalize">${name}</span>
                        <span class="flex items-center gap-1.5">
                            <span class="w-2 h-2 rounded-full ${status ? 'bg-green-400' : 'bg-red-400'}"></span>
                            <span class="text-sm ${status ? 'text-green-400' : 'text-red-400'}">${status ? 'Connected' : 'Disconnected'}</span>
                        </span>
                    </div>
                `).join('');
            } catch (e) {
                console.error('Failed to load connections:', e);
            }
        }
        
        // Load history
        async function loadHistory() {
            try {
                const res = await fetch('/api/history');
                const data = await res.json();
                const container = document.getElementById('activity-feed');
                
                if (!data.length) {
                    container.innerHTML = '<div class="p-5 text-center text-slate-500">No recent activity</div>';
                    return;
                }
                
                container.innerHTML = data.map(item => `
                    <div class="p-4 hover:bg-slate-800/50 transition">
                        <div class="flex items-center justify-between mb-1">
                            <span class="text-xs px-2 py-0.5 rounded ${item.source_type === 'confluence' ? 'bg-blue-500/20 text-blue-400' : 'bg-purple-500/20 text-purple-400'}">
                                ${item.source_type}
                            </span>
                            <span class="text-xs text-slate-500">${new Date(item.timestamp).toLocaleString()}</span>
                        </div>
                        <p class="font-medium">${item.source_id}</p>
                        <p class="text-sm text-slate-400">${item.action} - ${item.status}</p>
                    </div>
                `).join('');
            } catch (e) {
                console.error('Failed to load history:', e);
            }
        }
        
        // Sync form
        document.getElementById('sync-form').onsubmit = async (e) => {
            e.preventDefault();
            const resultDiv = document.getElementById('sync-result');
            resultDiv.className = 'mt-4 p-4 rounded-lg bg-slate-800';
            resultDiv.innerHTML = '<div class="flex items-center gap-2"><span class="animate-spin">⏳</span> Syncing...</div>';
            
            try {
                const res = await fetch('/api/sync', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        source_type: document.getElementById('source-type').value,
                        source_id: document.getElementById('source-id').value,
                        dry_run: document.getElementById('dry-run').checked,
                        post_diagram: document.getElementById('post-diagram').checked
                    })
                });
                
                const data = await res.json();
                resultDiv.className = `mt-4 p-4 rounded-lg ${data.success ? 'bg-green-500/20 border border-green-500/30' : 'bg-red-500/20 border border-red-500/30'}`;
                resultDiv.innerHTML = `
                    <div class="flex items-center gap-2 mb-2">
                        ${data.success ? '✅' : '❌'} <span class="font-medium">${data.success ? 'Sync Successful' : 'Sync Failed'}</span>
                    </div>
                    <p class="text-sm text-slate-400">Tables: ${data.tables_updated || 0} | Columns: ${data.columns_updated || 0}</p>
                    ${data.errors?.length ? `<p class="text-sm text-red-400 mt-2">${data.errors.join(', ')}</p>` : ''}
                `;
                loadHistory();
                loadStats();
            } catch (e) {
                resultDiv.className = 'mt-4 p-4 rounded-lg bg-red-500/20 border border-red-500/30';
                resultDiv.innerHTML = `<p class="text-red-400">Error: ${e.message}</p>`;
            }
        };
        
        // Search form
        document.getElementById('search-form').onsubmit = async (e) => {
            e.preventDefault();
            const query = document.getElementById('search-query').value;
            const resultsDiv = document.getElementById('search-results');
            
            try {
                const res = await fetch(`/api/search?query=${encodeURIComponent(query)}`);
                const data = await res.json();
                
                if (!data.length) {
                    resultsDiv.innerHTML = '<p class="text-slate-500 text-sm">No results found</p>';
                    return;
                }
                
                resultsDiv.innerHTML = data.map(item => `
                    <div class="p-3 bg-slate-800 rounded-lg">
                        <div class="flex items-center gap-2 mb-1">
                            <span class="text-xs px-1.5 py-0.5 rounded bg-slate-700">${item.source_type}</span>
                            <span class="text-xs text-cyan-400">${(item.similarity * 100).toFixed(0)}% match</span>
                        </div>
                        <p class="text-sm text-slate-300 line-clamp-2">${item.content.substring(0, 150)}...</p>
                    </div>
                `).join('');
            } catch (e) {
                resultsDiv.innerHTML = `<p class="text-red-400 text-sm">Error: ${e.message}</p>`;
            }
        };
        
        // Quick actions
        async function runFullSync() {
            if (!confirm('Run full sync of all configured sources?')) return;
            try {
                const res = await fetch('/api/sync/full', { method: 'POST' });
                const data = await res.json();
                alert(`Full sync completed: ${data.successful}/${data.total_sources} successful`);
                loadHistory();
                loadStats();
            } catch (e) {
                alert('Error: ' + e.message);
            }
        }
        
        async function runQualityChecks() {
            try {
                const res = await fetch('/api/quality/run', { method: 'POST' });
                const data = await res.json();
                alert(`Quality checks: ${data.passed} passed, ${data.failed} failed`);
            } catch (e) {
                alert('Error: ' + e.message);
            }
        }
        
        async function checkDrift() {
            alert('Navigate to /api/drift to view drift report');
        }
        
        async function viewLineage() {
            const table = prompt('Enter table name:');
            if (!table) return;
            window.open(`/api/lineage/${table}`, '_blank');
        }
        
        // Initialize
        connectWebSocket();
        loadStats();
        loadConnections();
        loadHistory();
    </script>
</body>
</html>
        """
    
    # WebSocket endpoint
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            while True:
                # Keep connection alive
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)
    
    # API Endpoints
    @app.get("/api/status")
    async def get_status():
        """Check connection status for all services"""
        return {
            "openai": True,
            "confluence": orchestrator.confluence.test_connection(),
            "jira": orchestrator.jira.test_connection(),
            "snowflake": orchestrator.snowflake.test_connection(),
            "vector_store": orchestrator.vector_store is not None,
            "lineage": orchestrator.lineage is not None
        }
    
    @app.get("/api/stats")
    async def get_stats():
        """Get dashboard statistics"""
        stats = orchestrator.get_audit_stats(30) if orchestrator.audit else {}
        return {
            "total_syncs": stats.get("total_syncs", 0),
            "success_rate": stats.get("success_rate", 0),
            "tables_updated": stats.get("tables_updated", 0),
            "drift_issues": 0,
            "syncs_by_source": stats.get("syncs_by_source", {})
        }
    
    @app.post("/api/sync")
    async def sync(request: SyncRequest, background_tasks: BackgroundTasks):
        """Perform a sync operation"""
        try:
            if request.source_type == "confluence":
                result = orchestrator.sync_confluence_page(
                    request.source_id,
                    dry_run=request.dry_run,
                    skip_drift_check=request.skip_drift_check,
                    post_diagram=request.post_diagram
                )
            elif request.source_type == "jira":
                result = orchestrator.sync_jira_issue(
                    request.source_id,
                    dry_run=request.dry_run,
                    skip_drift_check=request.skip_drift_check
                )
            else:
                raise HTTPException(400, "Invalid source type")
            
            # Broadcast update via WebSocket
            await manager.broadcast({
                "type": "sync_update",
                "data": {
                    "source_type": result.source_type,
                    "source_id": result.source_id,
                    "success": result.success
                }
            })
            
            return {
                "success": result.success,
                "tables_found": result.tables_found,
                "tables_updated": result.tables_updated,
                "columns_updated": result.columns_updated,
                "drift_issues": result.drift_issues,
                "errors": result.errors,
                "warnings": result.warnings,
                "duration_seconds": result.duration_seconds
            }
            
        except Exception as e:
            return {"success": False, "errors": [str(e)]}
    
    @app.post("/api/sync/batch")
    async def batch_sync(request: BatchSyncRequest):
        """Perform batch sync operation"""
        result = orchestrator.batch_sync(
            confluence_pages=request.confluence_pages,
            jira_issues=request.jira_issues,
            dry_run=request.dry_run,
            parallel=request.parallel
        )
        
        return {
            "total_sources": result.total_sources,
            "successful": result.successful,
            "failed": result.failed,
            "duration_seconds": result.duration_seconds
        }
    
    @app.post("/api/sync/full")
    async def full_sync():
        """Run full sync of all configured sources"""
        result = orchestrator.run_full_sync()
        return {
            "total_sources": result.total_sources,
            "successful": result.successful,
            "failed": result.failed,
            "duration_seconds": result.duration_seconds
        }
    
    @app.get("/api/search")
    async def search(query: str = Query(...), n_results: int = 5):
        """Search indexed documentation"""
        results = orchestrator.search_documentation(query, n_results)
        return results
    
    @app.get("/api/lineage/{table_name}")
    async def get_lineage(table_name: str):
        """Get lineage for a table"""
        return orchestrator.get_table_lineage(table_name)
    
    @app.post("/api/quality/run")
    async def run_quality(request: QualityCheckRequest = None):
        """Run data quality checks"""
        table_names = request.table_names if request else None
        return orchestrator.run_quality_checks(table_names)
    
    @app.get("/api/history")
    async def get_history():
        """Get sync history"""
        if orchestrator.audit:
            return orchestrator.audit.get_sync_history(20)
        return []
    
    @app.get("/api/audit/export")
    async def export_audit(days: int = 30):
        """Export audit log"""
        if not orchestrator.audit:
            raise HTTPException(400, "Audit logging not enabled")
        
        import tempfile
        filepath = tempfile.mktemp(suffix=".csv")
        orchestrator.audit.export_csv(filepath, days)
        
        def iterfile():
            with open(filepath, "rb") as f:
                yield from f
            os.unlink(filepath)
        
        return StreamingResponse(
            iterfile(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=audit_log_{days}d.csv"}
        )
    
    return app
