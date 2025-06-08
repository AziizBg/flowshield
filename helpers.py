from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import pycountry
import logging
from functools import lru_cache
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize geocoder with longer timeout
geolocator = Nominatim(
    user_agent="flowshield",
    timeout=10  # 10 seconds timeout
)

@lru_cache(maxsize=10000)  # Increased cache size to 10000 entries
def get_location_info(lat, lon):
    """
    Get city and country information from latitude and longitude coordinates.
    Uses caching to avoid repeated lookups for the same coordinates.
    
    Args:
        lat (float): Latitude
        lon (float): Longitude
        
    Returns:
        tuple: (city, country) names
    """
    try:
        # Round coordinates to 2 decimal places for caching (more aggressive rounding)
        lat_rounded = round(float(lat), 2)
        lon_rounded = round(float(lon), 2)
        
        # Try to get location with retries
        max_retries = 3
        for attempt in range(max_retries):
            try:
                location = geolocator.reverse(
                    f"{lat_rounded}, {lon_rounded}",
                    language='en',
                    timeout=10
                )
                
                if location and location.raw:
                    address = location.raw.get('address', {})
                    city = address.get('city') or address.get('town') or address.get('village') or "Unknown"
                    country = address.get('country') or "Unknown"
                    return city, country
                return "Unknown", "Unknown"
                
            except GeocoderTimedOut:
                if attempt < max_retries - 1:
                    logger.warning(f"Geocoding timeout, retrying... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(1)  # Wait before retry
                    continue
                logger.warning(f"Geocoding failed after {max_retries} attempts")
                return "Unknown", "Unknown"
                
    except Exception as e:
        logger.warning(f"Error in geocoding: {e}")
        return "Unknown", "Unknown"

def get_fire_severity(frp):
    """
    Determine fire severity based on Fire Radiative Power (FRP).
    
    Args:
        frp (float): Fire Radiative Power value
        
    Returns:
        str: Severity level ('Low', 'Moderate', or 'High')
    """
    if frp < 5:
        severity = "Low"
    elif frp < 15:
        severity = "Moderate"
    else:
        severity = "High"
    return severity