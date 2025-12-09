"""
Email Sender - Send reports via email
"""
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import ssl
from datetime import datetime
from jinja2 import Template, Environment, FileSystemLoader
from src.core.config import settings
from src.core.logger import logger


class EmailSender:
    """
    Class for sending emails with attachments
    """

    def __init__(self, smtp_host: str = None, smtp_port: int = None,
                username: str = None, password: str = None,
                use_tls: bool = True):
        """
        Initialize email sender

        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            username: SMTP username
            password: SMTP password
            use_tls: Whether to use TLS
        """
        self.smtp_host = smtp_host or settings.SMTP_HOST
        self.smtp_port = smtp_port or settings.SMTP_PORT
        self.username = username or settings.SMTP_USER
        self.password = password or settings.SMTP_PASSWORD
        self.use_tls = use_tls
        self.from_email = settings.EMAIL_FROM
        self.logger = logger
        self.template_dir = settings.CONFIG_DIR / 'email_templates'
        
        if self.template_dir.exists():
            self.env = Environment(loader=FileSystemLoader(str(self.template_dir)))
        else:
            self.env = None

    def send(self, to: Union[str, List[str]], subject: str, body: str,
            attachments: List[Path] = None, cc: List[str] = None,
            bcc: List[str] = None, html: bool = False) -> bool:
        """
        Send an email

        Args:
            to: Recipient(s)
            subject: Email subject
            body: Email body
            attachments: List of file paths to attach
            cc: CC recipients
            bcc: BCC recipients
            html: Whether body is HTML

        Returns:
            True if sent successfully
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to) if isinstance(to, list) else to
            msg['Subject'] = subject
            
            if cc:
                msg['Cc'] = ', '.join(cc)
            
            # Add body
            content_type = 'html' if html else 'plain'
            msg.attach(MIMEText(body, content_type))
            
            # Add attachments
            if attachments:
                for attachment in attachments:
                    self._add_attachment(msg, attachment)
            
            # Get all recipients
            recipients = []
            if isinstance(to, list):
                recipients.extend(to)
            else:
                recipients.append(to)
            if cc:
                recipients.extend(cc)
            if bcc:
                recipients.extend(bcc)
            
            # Send email
            self._send_email(msg, recipients)
            
            self.logger.info(f"Email sent successfully to {to}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False

    def send_report(self, to: Union[str, List[str]], report_path: Path,
                   subject: str = None, template: str = None,
                   context: Dict[str, Any] = None) -> bool:
        """
        Send a report via email

        Args:
            to: Recipient(s)
            report_path: Path to report file
            subject: Email subject
            template: Email template name
            context: Template context variables

        Returns:
            True if sent successfully
        """
        # Generate subject
        if not subject:
            report_name = report_path.stem.replace('_', ' ').title()
            date_str = datetime.now().strftime(settings.DATE_FORMAT)
            subject = f"{report_name} - {date_str}"
        
        # Generate body
        if template and self.env:
            try:
                tmpl = self.env.get_template(template)
                ctx = context or {}
                ctx['report_name'] = report_path.name
                ctx['date'] = datetime.now().strftime(settings.DATETIME_FORMAT)
                body = tmpl.render(**ctx)
                html = True
            except Exception as e:
                self.logger.warning(f"Template error, using default body: {str(e)}")
                body = self._default_body(report_path)
                html = False
        else:
            body = self._default_body(report_path)
            html = False
        
        return self.send(
            to=to,
            subject=subject,
            body=body,
            attachments=[report_path],
            html=html
        )

    def _default_body(self, report_path: Path) -> str:
        """Generate default email body"""
        return f"""
Hello,

Please find attached the report: {report_path.name}

Generated: {datetime.now().strftime(settings.DATETIME_FORMAT)}

This is an automated message from the Report Automation System.

Best regards,
Report Automation System
"""

    def _add_attachment(self, msg: MIMEMultipart, file_path: Path) -> None:
        """Add attachment to message"""
        if not file_path.exists():
            self.logger.warning(f"Attachment not found: {file_path}")
            return
            
        with open(file_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{file_path.name}"'
            )
            msg.attach(part)

    def _send_email(self, msg: MIMEMultipart, recipients: List[str]) -> None:
        """Send email via SMTP"""
        context = ssl.create_default_context()
        
        if self.use_tls:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.username, self.password)
                server.sendmail(self.from_email, recipients, msg.as_string())
        else:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context) as server:
                server.login(self.username, self.password)
                server.sendmail(self.from_email, recipients, msg.as_string())

    def test_connection(self) -> bool:
        """
        Test SMTP connection

        Returns:
            True if connection successful
        """
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls(context=context)
                server.login(self.username, self.password)
            self.logger.info("SMTP connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"SMTP connection test failed: {str(e)}")
            return False
