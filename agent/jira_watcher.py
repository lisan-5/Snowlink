"""
Jira watcher for monitoring issue updates
"""

import os
from datetime import datetime, timedelta
from typing import Optional
from atlassian import Jira
from rich.console import Console

console = Console()


class JiraWatcher:
    """Watch Jira projects for new or updated issues"""
    
    def __init__(self, config: dict):
        self.config = config
        self.projects = config.get("projects", [])
        self._client = None
        self._last_check = datetime.now() - timedelta(days=1)
    
    def _get_client(self) -> Jira:
        """Get or create Jira client"""
        if self._client is None:
            self._client = Jira(
                url=os.getenv("JIRA_URL"),
                username=os.getenv("JIRA_USER"),
                password=os.getenv("JIRA_API_TOKEN"),
                cloud=True
            )
        return self._client
    
    def test_connection(self) -> bool:
        """Test the Jira connection"""
        try:
            client = self._get_client()
            client.get_server_info()
            return True
        except Exception as e:
            console.print(f"[red]Jira connection error: {e}[/red]")
            return False
    
    def get_issue(self, issue_key: str) -> Optional[dict]:
        """
        Fetch a specific Jira issue by key
        
        Args:
            issue_key: The Jira issue key (e.g., PROJ-123)
            
        Returns:
            Dictionary with issue data including description and comments
        """
        try:
            client = self._get_client()
            issue = client.get_issue(issue_key, expand="renderedFields,changelog")
            
            fields = issue["fields"]
            
            # Combine description and comments into content
            content_parts = []
            
            # Add summary
            if fields.get("summary"):
                content_parts.append(f"# {fields['summary']}")
            
            # Add description
            if fields.get("description"):
                content_parts.append(fields["description"])
            
            # Add comments
            comments = client.get_issue_comments(issue_key)
            for comment in comments.get("comments", []):
                content_parts.append(f"Comment by {comment['author']['displayName']}:\n{comment['body']}")
            
            # Add any custom fields that might contain schema info
            for field_key, field_value in fields.items():
                if field_key.startswith("customfield_") and field_value:
                    if isinstance(field_value, str) and len(field_value) > 50:
                        content_parts.append(field_value)
            
            return {
                "key": issue["key"],
                "summary": fields.get("summary", ""),
                "content": "\n\n".join(content_parts),
                "status": fields["status"]["name"],
                "issue_type": fields["issuetype"]["name"],
                "project": fields["project"]["key"],
                "last_updated": fields.get("updated", ""),
                "labels": fields.get("labels", []),
                "url": f"{os.getenv('JIRA_URL')}/browse/{issue_key}"
            }
            
        except Exception as e:
            console.print(f"[red]Failed to fetch Jira issue {issue_key}: {e}[/red]")
            return None
    
    def check_updates(self) -> list[dict]:
        """
        Check for updated issues in configured projects since last check
        
        Returns:
            List of issue data dictionaries that have been updated
        """
        updates = []
        client = self._get_client()
        
        for project_config in self.projects:
            project_key = project_config.get("key")
            issue_types = project_config.get("issue_types", [])
            labels_filter = project_config.get("labels_filter", [])
            
            try:
                # Build JQL query
                jql_parts = [f'project = "{project_key}"']
                jql_parts.append('updated >= -1d')
                
                if issue_types:
                    types_str = ", ".join([f'"{t}"' for t in issue_types])
                    jql_parts.append(f'issuetype IN ({types_str})')
                
                if labels_filter:
                    labels_str = ", ".join([f'"{l}"' for l in labels_filter])
                    jql_parts.append(f'labels IN ({labels_str})')
                
                jql = " AND ".join(jql_parts)
                
                # Search for issues
                results = client.jql(jql, limit=50)
                
                for issue in results.get("issues", []):
                    issue_key = issue["key"]
                    issue_data = self.get_issue(issue_key)
                    if issue_data:
                        updates.append(issue_data)
                        
            except Exception as e:
                console.print(f"[red]Error checking project {project_key}: {e}[/red]")
        
        self._last_check = datetime.now()
        return updates
    
    def get_issues_by_label(self, label: str, project_key: Optional[str] = None) -> list[dict]:
        """
        Get issues with a specific label
        
        Args:
            label: The Jira label to search for
            project_key: Optional project to limit search to
            
        Returns:
            List of issue data dictionaries
        """
        client = self._get_client()
        issues = []
        
        jql_parts = [f'labels = "{label}"']
        if project_key:
            jql_parts.append(f'project = "{project_key}"')
        
        jql = " AND ".join(jql_parts)
        
        try:
            results = client.jql(jql, limit=100)
            
            for issue in results.get("issues", []):
                issue_data = self.get_issue(issue["key"])
                if issue_data:
                    issues.append(issue_data)
                    
        except Exception as e:
            console.print(f"[red]Error fetching issues by label: {e}[/red]")
        
        return issues
    
    def add_comment(self, issue_key: str, comment: str) -> bool:
        """
        Add a comment to a Jira issue
        
        Args:
            issue_key: The issue key
            comment: The comment text
            
        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.issue_add_comment(issue_key, comment)
            return True
        except Exception as e:
            console.print(f"[red]Failed to add comment to {issue_key}: {e}[/red]")
            return False
