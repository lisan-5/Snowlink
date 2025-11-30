"""
Confluence watcher for monitoring page updates
"""

import os
from datetime import datetime, timedelta
from typing import Optional
from atlassian import Confluence
from rich.console import Console

console = Console()


class ConfluenceWatcher:
    """Watch Confluence spaces for new or updated pages"""
    
    def __init__(self, config: dict):
        self.config = config
        self.spaces = config.get("spaces", [])
        self._client = None
        self._last_check = datetime.now() - timedelta(days=1)
    
    def _get_client(self) -> Confluence:
        """Get or create Confluence client"""
        if self._client is None:
            self._client = Confluence(
                url=os.getenv("CONFLUENCE_URL"),
                username=os.getenv("CONFLUENCE_USER"),
                password=os.getenv("CONFLUENCE_API_TOKEN"),
                cloud=True
            )
        return self._client
    
    def test_connection(self) -> bool:
        """Test the Confluence connection"""
        try:
            client = self._get_client()
            # Try to get spaces to verify connection
            client.get_all_spaces(limit=1)
            return True
        except Exception as e:
            console.print(f"[red]Confluence connection error: {e}[/red]")
            return False
    
    def get_page(self, page_id: str) -> Optional[dict]:
        """
        Fetch a specific Confluence page by ID
        
        Args:
            page_id: The Confluence page ID
            
        Returns:
            Dictionary with page data including content
        """
        try:
            client = self._get_client()
            page = client.get_page_by_id(
                page_id,
                expand="body.storage,version,history"
            )
            
            return {
                "id": page["id"],
                "title": page["title"],
                "content": page["body"]["storage"]["value"],
                "version": page["version"]["number"],
                "last_updated": page["version"]["when"],
                "space_key": page.get("space", {}).get("key", ""),
                "url": f"{os.getenv('CONFLUENCE_URL')}/pages/viewpage.action?pageId={page_id}"
            }
            
        except Exception as e:
            console.print(f"[red]Failed to fetch Confluence page {page_id}: {e}[/red]")
            return None
    
    def check_updates(self) -> list[dict]:
        """
        Check for updated pages in configured spaces since last check
        
        Returns:
            List of page data dictionaries that have been updated
        """
        updates = []
        client = self._get_client()
        
        for space_config in self.spaces:
            space_key = space_config.get("key")
            title_patterns = space_config.get("page_title_patterns", [])
            
            try:
                # Get recently modified pages in this space
                cql = f'space="{space_key}" AND lastModified > now("-1d")'
                
                results = client.cql(cql, limit=50)
                
                for result in results.get("results", []):
                    page_id = result["content"]["id"]
                    title = result["content"]["title"]
                    
                    # Filter by title patterns if specified
                    if title_patterns:
                        matched = False
                        for pattern in title_patterns:
                            pattern_clean = pattern.replace("*", "").lower()
                            if pattern_clean in title.lower():
                                matched = True
                                break
                        if not matched:
                            continue
                    
                    # Fetch full page content
                    page_data = self.get_page(page_id)
                    if page_data:
                        updates.append(page_data)
                        
            except Exception as e:
                console.print(f"[red]Error checking space {space_key}: {e}[/red]")
        
        self._last_check = datetime.now()
        return updates
    
    def get_pages_by_label(self, label: str, space_key: Optional[str] = None) -> list[dict]:
        """
        Get pages with a specific label
        
        Args:
            label: The Confluence label to search for
            space_key: Optional space to limit search to
            
        Returns:
            List of page data dictionaries
        """
        client = self._get_client()
        pages = []
        
        cql = f'label="{label}"'
        if space_key:
            cql += f' AND space="{space_key}"'
        
        try:
            results = client.cql(cql, limit=100)
            
            for result in results.get("results", []):
                page_id = result["content"]["id"]
                page_data = self.get_page(page_id)
                if page_data:
                    pages.append(page_data)
                    
        except Exception as e:
            console.print(f"[red]Error fetching pages by label: {e}[/red]")
        
        return pages
    
    def post_diagram_to_page(self, page_id: str, diagram_content: str, diagram_type: str = "mermaid") -> bool:
        """
        Post an ER diagram back to a Confluence page
        
        Args:
            page_id: The page ID to update
            diagram_content: The diagram code/content
            diagram_type: 'mermaid' or 'graphviz'
            
        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            
            # Get current page content
            page = client.get_page_by_id(page_id, expand="body.storage,version")
            current_content = page["body"]["storage"]["value"]
            
            # Create diagram HTML block
            if diagram_type == "mermaid":
                diagram_html = f"""
                <ac:structured-macro ac:name="html">
                    <ac:plain-text-body><![CDATA[
                        <div class="mermaid">
                        {diagram_content}
                        </div>
                        <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
                        <script>mermaid.initialize({{startOnLoad:true}});</script>
                    ]]></ac:plain-text-body>
                </ac:structured-macro>
                """
            else:
                diagram_html = f"""
                <ac:structured-macro ac:name="code">
                    <ac:parameter ac:name="language">text</ac:parameter>
                    <ac:plain-text-body><![CDATA[{diagram_content}]]></ac:plain-text-body>
                </ac:structured-macro>
                """
            
            # Check if diagram section exists, update or append
            diagram_marker = "<!-- snowlink-er-diagram -->"
            if diagram_marker in current_content:
                # Replace existing diagram
                import re
                pattern = f"{diagram_marker}.*?{diagram_marker}"
                replacement = f"{diagram_marker}\n{diagram_html}\n{diagram_marker}"
                new_content = re.sub(pattern, replacement, current_content, flags=re.DOTALL)
            else:
                # Append diagram section
                new_content = current_content + f"""
                <h2>Auto-Generated ER Diagram</h2>
                <p><em>Generated by snowlink-ai on {datetime.now().strftime('%Y-%m-%d %H:%M')}</em></p>
                {diagram_marker}
                {diagram_html}
                {diagram_marker}
                """
            
            # Update page
            client.update_page(
                page_id=page_id,
                title=page["title"],
                body=new_content,
                minor_edit=True
            )
            
            console.print(f"[green]✅ Posted diagram to Confluence page {page_id}[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]Failed to post diagram to page {page_id}: {e}[/red]")
            return False
