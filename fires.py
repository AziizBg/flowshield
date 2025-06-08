from datetime import datetime, timedelta
import requests
import csv
from geopy.geocoders import Nominatim
from helpers import get_fire_severity, get_location_info
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize geocoder with longer timeout and user agent
geolocator = Nominatim(
    user_agent="flowshield",
    timeout=10  # 10 seconds timeout
)

def fetch_fires(numberOfMinutes=1, max_retries=3):
    """
    Fetch fire data from NASA FIRMS API.
    
    Args:
        numberOfMinutes (int): Number of minutes of data to fetch
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        list: List of fire events
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    url = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/477785c4a607ad274bbbb9cbdcdd6bef/VIIRS_SNPP_NRT/world/1/{today}'
    
    # Keep track of processed fires
    if not hasattr(fetch_fires, "processed_fires"):
        fetch_fires.processed_fires = []
        logger.info("Initialized processed_fires list")

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching fires from {numberOfMinutes} minutes starting from one hour ago (Attempt {attempt + 1}/{max_retries})")
            logger.info(f"Requesting data from URL: {url}")

            # Add timeout and verify=False to handle SSL issues
            response = requests.get(url, timeout=30, verify=False)
            logger.info(f"Received response with status code: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch fire data. Status code: {response.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Wait before retrying
                    continue
                return []

            decoded_content = response.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            headers = next(cr)
            logger.info(f"CSV headers: {headers}")

            # Get the reference time as 3 hours ago
            three_hours_ago = datetime.utcnow() - timedelta(hours=3)
            logger.info(f"Processing fires after: {three_hours_ago}")

            fire_count = 0
            row_count = 0
            for row in cr:
                row_count += 1
                if row_count % 1000 == 0:
                    logger.info(f"Processed {row_count} rows so far...")
                    
                try:
                    fire_data = dict(zip(headers, row))
                    acq_date = fire_data.get('acq_date')
                    acq_time = fire_data.get('acq_time')

                    if acq_date and acq_time:
                        acq_time = acq_time.zfill(4)  # Pad to 4 digits (HHMM)
                        timestamp_str = f"{acq_date} {acq_time}"
                        fire_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H%M")
                        
                        # Only process fires within the time window
                        time_diff = three_hours_ago - fire_dt
                        if time_diff.total_seconds() <= (numberOfMinutes * 60):
                            latitude = fire_data.get('latitude')
                            longitude = fire_data.get('longitude')
                            
                            try:
                                # Add a small delay between geocoding requests to respect rate limits
                                time.sleep(0.1)  # 100ms delay
                                city, country = get_location_info(float(latitude), float(longitude))
                            except Exception as e:
                                logger.warning(f"Error getting location info: {e}")
                                city, country = "Unknown", "Unknown"
                                
                            frp = float(fire_data.get('frp', 0))

                            # Create a unique ID
                            fire_id = f"{latitude}_{longitude}_{acq_date}_{acq_time}"

                            fire_info = {
                                "id": fire_id,
                                "time": fire_dt.strftime("%Y-%m-%d %H:%M:%S"),
                                "latitude": latitude,
                                "longitude": longitude,
                                "country": country,
                                "city": city,
                                "frp": frp,
                                "type": "fire"
                            }

                            fetch_fires.processed_fires.append(fire_info)
                            fire_count += 1
                except Exception as e:
                    logger.warning(f"Error processing fire row: {e}")
                    continue

            logger.info(f"Processed {row_count} total rows and found {fire_count} new fire events")
            return fetch_fires.processed_fires

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching fire data (Attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retrying
                continue
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching fire data: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retrying
                continue
            return []
        except Exception as e:
            logger.error(f"Unexpected error in fetch_fires: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retrying
                continue
            return []

    return fetch_fires.processed_fires

# Initialize the processed_fires list
fetch_fires.processed_fires = []
