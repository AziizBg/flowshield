"""
HBase REST API client module
"""

import base64
import json
import logging
import requests
import time

logger = logging.getLogger(__name__)

class HBaseRestClient:
    """Simple HBase REST API client"""
    
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        logger.info(f"Initializing HBase REST client with URL: {self.base_url}")
        
    def write_row(self, table_name, row_key, data):
        """Write a single row to HBase via REST API"""
        try:
            url = f"{self.base_url}/{table_name}/{row_key}"
            logger.debug(f"Writing to HBase URL: {url}")
            
            # Convert data to HBase REST format
            cells = []
            for key, value in data.items():
                if value is not None:
                    cells.append({
                        "column": base64.b64encode(f"info:{key}".encode()).decode(),
                        "timestamp": int(time.time() * 1000),
                        "$": base64.b64encode(str(value).encode()).decode()
                    })
            
            payload = {
                "Row": [{
                    "key": base64.b64encode(row_key.encode()).decode(),
                    "Cell": cells
                }]
            }
            
            response = requests.post(
                url, 
                json=payload, 
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                logger.debug(f"Successfully wrote row {row_key} to table {table_name}")
                return True
            else:
                logger.error(f"Failed to write to HBase. Status code: {response.status_code}, Response: {response.text}")
                return False
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while writing to HBase: {str(e)}")
            return False
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while writing to HBase: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while writing to HBase: {str(e)}")
            return False 