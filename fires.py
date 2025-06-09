"""
Fire Detection and Monitoring System

This module provides functionality to fetch and process real-time fire data from NASA's FIRMS (Fire Information for Resource Management System) API.
It processes satellite data to detect and track fire events worldwide, including their location, intensity, and temporal information.

Key Features:
- Real-time fire detection using NASA FIRMS API
- Geocoding of fire locations to city and country level
- Fire severity assessment
- Batch processing of fire events
- Robust error handling and retry mechanisms
"""

from datetime import datetime, timedelta
import requests
import csv
from geopy.geocoders import Nominatim
from helpers import get_fire_severity, get_location_info
import logging
import time
import random
from collections import deque

# Configure logging with timestamp, log level, and message format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize geocoder with custom settings for better reliability
geolocator = Nominatim(
    user_agent="flowshield",  # Custom user agent for API requests
    timeout=10  # 10 seconds timeout for geocoding requests
)

def process_batch(rows, headers, three_hours_ago, numberOfMinutes):
    """
    Process a batch of fire detection data and convert it into structured fire events.
    
    Args:
        rows (list): List of rows containing fire detection data
        headers (list): Column headers for the data
        three_hours_ago (datetime): Timestamp for filtering recent fires
        numberOfMinutes (int): Time window in minutes for filtering fires
        
    Returns:
        list: List of processed fire events with location and metadata
    """
    fire_events = []
    for row in rows:
        try:
            # Convert row data to dictionary using headers
            fire_data = dict(zip(headers, row))
            acq_date = fire_data.get('acq_date')
            acq_time = fire_data.get('acq_time')

            # Skip rows with missing date or time
            if not (acq_date and acq_time):
                continue

            # Parse timestamp and filter based on time window
            fire_dt = datetime.strptime(f"{acq_date} {acq_time.zfill(4)}", "%Y-%m-%d %H%M")
            if (three_hours_ago - fire_dt).total_seconds() > (numberOfMinutes * 60):
                continue

            # Extract and process location data
            latitude = fire_data.get('latitude')
            longitude = fire_data.get('longitude')
            try:
                city, country = get_location_info(float(latitude), float(longitude))
            except Exception as e:
                logger.warning(f"Error getting location info: {e}")
                city, country = "Unknown", "Unknown"

            # Create structured fire event
            fire_events.append({
                "id": f"{latitude}_{longitude}_{acq_date}_{acq_time}",
                "time": fire_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "latitude": latitude,
                "longitude": longitude,
                "country": country,
                "city": city,
                "frp": float(fire_data.get('frp', 0)),  # Fire Radiative Power
                "type": "fire"
            })
        except Exception as e:
            logger.warning(f"Error processing fire row: {e}")
            
    return fire_events

def fetch_fires(numberOfMinutes=1, max_retries=3):
    """
    Fetch and process fire data from NASA FIRMS API.
    
    This function retrieves real-time fire detection data from NASA's FIRMS API,
    processes it to extract relevant information, and returns structured fire events.
    It includes retry logic for handling API failures and network issues.
    
    Args:
        numberOfMinutes (int, optional): Time window in minutes for filtering fires. Defaults to 1.
        max_retries (int, optional): Maximum number of retry attempts for API calls. Defaults to 3.
        
    Returns:
        list: List of processed fire events with the following structure:
            - id: Unique identifier for the fire event
            - time: Timestamp of the fire detection
            - latitude: Geographic latitude
            - longitude: Geographic longitude
            - country: Country where the fire was detected
            - city: Nearest city to the fire
            - frp: Fire Radiative Power (intensity measure)
            - type: Event type (always "fire")
    """
    # NASA FIRMS API endpoint with authentication token
    url = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/477785c4a607ad274bbbb9cbdcdd6bef/VIIRS_SNPP_NRT/world/1/{datetime.utcnow().strftime("%Y-%m-%d")}'
    
    # Initialize processed fires list if not exists (persists between function calls)
    if not hasattr(fetch_fires, "processed_fires"):
        fetch_fires.processed_fires = []
        logger.info("Initialized processed_fires list")

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching fires from {numberOfMinutes} minutes ago (Attempt {attempt + 1}/{max_retries})")
            
            # Make API request with timeout
            response = requests.get(url, timeout=30, verify=False)
            if response.status_code != 200:
                logger.error(f"Failed to fetch fire data. Status code: {response.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                return []

            # Process CSV response
            decoded_content = response.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            headers = next(cr)
            all_rows = list(cr)
            
            # Process fires within specified time window
            three_hours_ago = datetime.utcnow() - timedelta(hours=3)
            fire_events = process_batch(all_rows, headers, three_hours_ago, numberOfMinutes)
            
            logger.info(f"Processed {len(all_rows)} rows and found {len(fire_events)} new fire events")
            return fire_events

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching fire data (Attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching fire data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in fetch_fires: {e}")
            
        if attempt < max_retries - 1:
            time.sleep(5)
            continue
            
    return fetch_fires.processed_fires

# Initialize the processed_fires list for tracking processed fire events
fetch_fires.processed_fires = []
