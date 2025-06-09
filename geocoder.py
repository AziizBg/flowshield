"""
Geocoding utility module
"""

import logging
import time
from functools import lru_cache
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

logger = logging.getLogger(__name__)

class GeoCoder:
    """Geocoding utility class with caching and retry logic"""
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="flowshield")
        self.cache = {}
        self.max_retries = 3
        self.timeout = 10  # 10 seconds timeout
    
    @lru_cache(maxsize=1000)
    def get_location_info(self, lat: float, lon: float) -> tuple:
        """Get city and country from coordinates with caching and retry logic"""
        for attempt in range(self.max_retries):
            try:
                location = self.geolocator.reverse(
                    f"{lat}, {lon}", 
                    language='en',
                    timeout=self.timeout
                )
                if location:
                    address = location.raw.get('address', {})
                    city = address.get('city') or address.get('town') or address.get('village') or "Unknown"
                    country = address.get('country') or "Unknown"
                    return city, country
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                if attempt < self.max_retries - 1:
                    time.sleep(1)  # Wait 1 second before retrying
                    continue
                logger.warning(f"Geocoding failed for coordinates ({lat}, {lon}) after {self.max_retries} attempts: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error during geocoding for coordinates ({lat}, {lon}): {str(e)}")
                break
        
        return "Unknown", "Unknown" 