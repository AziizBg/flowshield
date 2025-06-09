"""
Alert management module
"""

import datetime
import logging
import os
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests

logger = logging.getLogger(__name__)

class AlertManager:
    """Manages alerting functionality for events"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.alert_email = os.getenv('ALERT_EMAIL')
        self.alert_email_password = os.getenv('ALERT_EMAIL_PASSWORD')
        self.alert_recipient = os.getenv('ALERT_RECIPIENT')
        self.webhook_url = os.getenv('ALERT_WEBHOOK_URL')
        
        # Alert thresholds
        self.earthquake_thresholds = {
            'Low': 2.0,
            'Moderate': 4.0,
            'High': 6.0,
            'Extreme': 7.0
        }
        self.fire_thresholds = {
            'Low': 1.0,
            'Moderate': 5.0,
            'High': 50.0,
            'Extreme': 100.0
        }
        
        self.alert_cooldown = 60  # 1 minute
        self.last_alert_time = {}
        
        # Validate configurations
        self._validate_configurations()
    
    def _validate_configurations(self):
        """Validate alert configurations"""
        if not all([self.alert_email, self.alert_email_password, self.alert_recipient]):
            logger.warning("Email alert configuration is incomplete. Email alerts will be disabled.")
        else:
            logger.info("Email alert configuration is complete.")
        
        if not self.webhook_url:
            logger.warning("Webhook URL not configured. Webhook alerts will be disabled.")
        else:
            logger.info("Webhook alert configuration is complete.")
    
    def should_alert(self, event_type: str, severity: str, value: float) -> bool:
        """Determine if an alert should be sent based on severity and cooldown"""
        current_time = time.time()
        alert_key = f"{event_type}_{severity}"
        
        if alert_key in self.last_alert_time:
            if current_time - self.last_alert_time[alert_key] < self.alert_cooldown:
                logger.debug(f"Alert {alert_key} is in cooldown period")
                return False
        
        thresholds = self.earthquake_thresholds if event_type == 'earthquake' else self.fire_thresholds
        threshold = thresholds.get(severity)
        
        if threshold and value >= threshold:
            self.last_alert_time[alert_key] = current_time
            logger.info(f"{event_type.title()} alert triggered: {severity} (value {value})")
            return True
        
        return False
    
    def send_email_alert(self, event_type: str, severity: str, value: float, location: str):
        """Send email alert"""
        if not all([self.alert_email, self.alert_email_password, self.alert_recipient]):
            logger.warning("Email alert configuration incomplete")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.alert_email
            msg['To'] = self.alert_recipient
            msg['Subject'] = f"ALERT: {severity} {event_type.title()} Detected"
            
            body = f"""
            ALERT: {severity} {event_type.title()} Detected
            
            Details:
            - Type: {event_type}
            - Severity: {severity}
            - Value: {value}
            - Location: {location}
            - Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            This is an automated alert from FlowShield.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.alert_email, self.alert_email_password)
                server.send_message(msg)
            
            logger.info(f"Email alert sent for {event_type} {severity}")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
    
    def send_webhook_alert(self, event_type: str, severity: str, value: float, location: str):
        """Send webhook alert"""
        if not self.webhook_url:
            logger.warning("Webhook URL not configured")
            return
        
        try:
            payload = {
                "text": f"🚨 *{severity} {event_type.title()} Alert*\n" +
                       f"*Value:* {value}\n" +
                       f"*Location:* {location}\n" +
                       f"*Time:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Webhook alert sent for {event_type} {severity}")
            else:
                logger.error(f"Failed to send webhook alert: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {str(e)}")
    
    def process_alert(self, event_type: str, severity: str, value: float, location: str):
        """Process and send alerts if conditions are met"""
        if self.should_alert(event_type, severity, value):
            self.send_email_alert(event_type, severity, value, location)
            self.send_webhook_alert(event_type, severity, value, location) 