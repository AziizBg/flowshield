"""
Configuration module for the streaming application
"""

import os
import logging

logger = logging.getLogger(__name__)

class StreamingConfig:
    """Configuration class for the streaming application"""
    
    def __init__(self):
        # Kafka configuration
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.EARTHQUAKE_TOPIC = os.getenv('EARTHQUAKE_TOPIC', 'earthquakes')
        self.FIRE_TOPIC = os.getenv('FIRE_TOPIC', 'fires')
        
        # Storage configuration
        self.OUTPUT_MODE = os.getenv('OUTPUT_MODE', 'hbase_rest')
        self.OUTPUT_PATH = os.getenv('OUTPUT_PATH', '/tmp/flowshield_output')
        self.HBASE_REST_URL = os.getenv('HBASE_REST_URL', 'http://localhost:8080')
        
        # Processing configuration
        self.CHECKPOINT_DIR = os.getenv('CHECKPOINT_DIR', './spark_checkpoints')
        self.PROCESSING_TIME = os.getenv('PROCESSING_TIME', '60 seconds')
        self.WATERMARK_THRESHOLD = os.getenv('WATERMARK_THRESHOLD', '2 hours')
        self.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
        self.MAX_RECORDS_PER_TRIGGER = int(os.getenv('MAX_RECORDS_PER_TRIGGER', '1000'))
        
        # Alert configuration
        self.SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
        self.ALERT_EMAIL = os.getenv('ALERT_EMAIL', 'your-email@gmail.com')
        self.ALERT_EMAIL_PASSWORD = os.getenv('ALERT_EMAIL_PASSWORD', 'your-app-password')
        self.ALERT_RECIPIENT = os.getenv('ALERT_RECIPIENT', 'recipient@example.com')
        self.ALERT_WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL', '')
        
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