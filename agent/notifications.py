"""
Multi-channel notifications for sync events
Supports Slack, Microsoft Teams, Email, and Webhooks
"""

import os
import json
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from dataclasses import dataclass
import httpx
from rich.console import Console

console = Console()


@dataclass
class NotificationEvent:
    """Event to be notified about"""
    event_type: str  # sync_complete, sync_failed, drift_detected, quality_failed
    title: str
    message: str
    severity: str = "info"  # info, warning, error
    metadata: dict = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.metadata is None:
            self.metadata = {}


class SlackNotifier:
    """Send notifications to Slack"""
    
    def __init__(self, webhook_url: Optional[str] = None, channel: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self.channel = channel or os.getenv("SLACK_CHANNEL", "#data-sync")
    
    def send(self, event: NotificationEvent) -> bool:
        """Send notification to Slack"""
        if not self.webhook_url:
            console.print("[yellow]Slack webhook URL not configured[/yellow]")
            return False
        
        # Build Slack message with blocks
        color_map = {
            "info": "#36a64f",
            "warning": "#ffa500",
            "error": "#ff0000"
        }
        
        emoji_map = {
            "info": ":white_check_mark:",
            "warning": ":warning:",
            "error": ":x:"
        }
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji_map.get(event.severity, '')} {event.title}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": event.message
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Event:* {event.event_type} | *Time:* {event.timestamp}"
                    }
                ]
            }
        ]
        
        # Add metadata fields if present
        if event.metadata:
            fields = []
            for key, value in event.metadata.items():
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:* {value}"
                })
            if fields:
                blocks.append({
                    "type": "section",
                    "fields": fields[:10]  # Slack limit
                })
        
        payload = {
            "channel": self.channel,
            "attachments": [
                {
                    "color": color_map.get(event.severity, "#36a64f"),
                    "blocks": blocks
                }
            ]
        }
        
        try:
            response = httpx.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            console.print(f"[green]Slack notification sent[/green]")
            return True
        except Exception as e:
            console.print(f"[red]Failed to send Slack notification: {e}[/red]")
            return False


class TeamsNotifier:
    """Send notifications to Microsoft Teams"""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("TEAMS_WEBHOOK_URL")
    
    def send(self, event: NotificationEvent) -> bool:
        """Send notification to Teams"""
        if not self.webhook_url:
            console.print("[yellow]Teams webhook URL not configured[/yellow]")
            return False
        
        theme_color = {
            "info": "00FF00",
            "warning": "FFA500",
            "error": "FF0000"
        }
        
        # Build Teams adaptive card
        card = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": theme_color.get(event.severity, "00FF00"),
            "summary": event.title,
            "sections": [
                {
                    "activityTitle": event.title,
                    "activitySubtitle": event.timestamp,
                    "text": event.message,
                    "facts": [
                        {"name": k, "value": str(v)}
                        for k, v in (event.metadata or {}).items()
                    ][:5]  # Limit facts
                }
            ]
        }
        
        try:
            response = httpx.post(
                self.webhook_url,
                json=card,
                timeout=10
            )
            response.raise_for_status()
            console.print(f"[green]Teams notification sent[/green]")
            return True
        except Exception as e:
            console.print(f"[red]Failed to send Teams notification: {e}[/red]")
            return False


class EmailNotifier:
    """Send email notifications via SMTP"""
    
    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        from_email: Optional[str] = None,
        to_emails: Optional[list[str]] = None
    ):
        self.smtp_host = smtp_host or os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", smtp_port))
        self.username = username or os.getenv("SMTP_USERNAME")
        self.password = password or os.getenv("SMTP_PASSWORD")
        self.from_email = from_email or os.getenv("EMAIL_FROM")
        self.to_emails = to_emails or os.getenv("EMAIL_TO", "").split(",")
    
    def send(self, event: NotificationEvent) -> bool:
        """Send email notification"""
        if not all([self.smtp_host, self.username, self.password, self.from_email, self.to_emails]):
            console.print("[yellow]Email configuration incomplete[/yellow]")
            return False
        
        # Build email
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[snowlink-ai] {event.title}"
        msg["From"] = self.from_email
        msg["To"] = ", ".join(self.to_emails)
        
        # Plain text version
        text_content = f"""
{event.title}
{'=' * len(event.title)}

{event.message}

Event Type: {event.event_type}
Severity: {event.severity}
Time: {event.timestamp}

Metadata:
{json.dumps(event.metadata or {}, indent=2)}

--
snowlink-ai
        """
        
        # HTML version
        severity_colors = {
            "info": "#22c55e",
            "warning": "#f59e0b",
            "error": "#ef4444"
        }
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: {severity_colors.get(event.severity, '#22c55e')}; color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
                .content {{ background: #f8fafc; padding: 20px; border: 1px solid #e2e8f0; border-top: none; }}
                .metadata {{ background: #1e293b; color: #e2e8f0; padding: 15px; border-radius: 6px; font-family: monospace; }}
                .footer {{ color: #64748b; font-size: 12px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2 style="margin: 0;">{event.title}</h2>
                </div>
                <div class="content">
                    <p>{event.message}</p>
                    <p><strong>Event:</strong> {event.event_type}<br>
                    <strong>Severity:</strong> {event.severity}<br>
                    <strong>Time:</strong> {event.timestamp}</p>
                    
                    {f'<div class="metadata"><pre>{json.dumps(event.metadata, indent=2)}</pre></div>' if event.metadata else ''}
                    
                    <div class="footer">
                        Sent by snowlink-ai
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))
        
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.sendmail(self.from_email, self.to_emails, msg.as_string())
            
            console.print(f"[green]Email notification sent to {len(self.to_emails)} recipients[/green]")
            return True
        except Exception as e:
            console.print(f"[red]Failed to send email notification: {e}[/red]")
            return False


class WebhookNotifier:
    """Send notifications to custom webhooks"""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("CUSTOM_WEBHOOK_URL")
    
    def send(self, event: NotificationEvent) -> bool:
        """Send notification to custom webhook"""
        if not self.webhook_url:
            return False
        
        payload = {
            "event_type": event.event_type,
            "title": event.title,
            "message": event.message,
            "severity": event.severity,
            "metadata": event.metadata,
            "timestamp": event.timestamp
        }
        
        try:
            response = httpx.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return True
        except Exception as e:
            console.print(f"[red]Failed to send webhook: {e}[/red]")
            return False


class NotificationManager:
    """Manage all notification channels"""
    
    def __init__(self, config: dict):
        self.config = config
        self.channels = []
        
        # Initialize enabled channels
        notif_config = config.get("notifications", {})
        
        if notif_config.get("slack", {}).get("enabled"):
            self.channels.append(("slack", SlackNotifier()))
        
        if notif_config.get("teams", {}).get("enabled"):
            self.channels.append(("teams", TeamsNotifier()))
        
        if notif_config.get("email", {}).get("enabled"):
            self.channels.append(("email", EmailNotifier()))
        
        if notif_config.get("webhook", {}).get("enabled"):
            self.channels.append(("webhook", WebhookNotifier()))
    
    def notify(
        self,
        event_type: str,
        title: str,
        message: str,
        severity: str = "info",
        metadata: Optional[dict] = None,
        channels: Optional[list[str]] = None
    ):
        """
        Send notification to all configured channels
        
        Args:
            event_type: Type of event (sync_complete, drift_detected, etc.)
            title: Notification title
            message: Notification message
            severity: info, warning, or error
            metadata: Additional data to include
            channels: Specific channels to notify (None = all)
        """
        event = NotificationEvent(
            event_type=event_type,
            title=title,
            message=message,
            severity=severity,
            metadata=metadata
        )
        
        for channel_name, notifier in self.channels:
            if channels is None or channel_name in channels:
                notifier.send(event)
    
    def notify_sync_complete(
        self,
        source_type: str,
        source_id: str,
        tables_updated: int,
        columns_updated: int
    ):
        """Notify about successful sync completion"""
        self.notify(
            event_type="sync_complete",
            title="Sync Completed Successfully",
            message=f"Successfully synced {source_type} {source_id}",
            severity="info",
            metadata={
                "Source Type": source_type,
                "Source ID": source_id,
                "Tables Updated": tables_updated,
                "Columns Updated": columns_updated
            }
        )
    
    def notify_sync_failed(
        self,
        source_type: str,
        source_id: str,
        error: str
    ):
        """Notify about sync failure"""
        self.notify(
            event_type="sync_failed",
            title="Sync Failed",
            message=f"Failed to sync {source_type} {source_id}: {error}",
            severity="error",
            metadata={
                "Source Type": source_type,
                "Source ID": source_id,
                "Error": error
            }
        )
    
    def notify_drift_detected(
        self,
        drift_report: dict
    ):
        """Notify about detected schema drift"""
        self.notify(
            event_type="drift_detected",
            title="Schema Drift Detected",
            message=f"Found {drift_report.get('total_issues', 0)} schema drift issues",
            severity="warning" if drift_report.get('high_severity', 0) == 0 else "error",
            metadata={
                "Total Issues": drift_report.get("total_issues", 0),
                "High Severity": drift_report.get("high_severity", 0),
                "Medium Severity": drift_report.get("medium_severity", 0)
            }
        )
    
    def notify_quality_failed(
        self,
        quality_report: dict
    ):
        """Notify about failed quality checks"""
        self.notify(
            event_type="quality_failed",
            title="Data Quality Check Failed",
            message=f"{quality_report.get('failed', 0)} quality checks failed",
            severity="error",
            metadata={
                "Total Checks": quality_report.get("total_checks", 0),
                "Passed": quality_report.get("passed", 0),
                "Failed": quality_report.get("failed", 0)
            }
        )
