from geopy.geocoders import Nominatim
import pycountry

def get_location_info(lat, lon):
    """
    Get city and country information from latitude and longitude coordinates.
    
    Args:
        lat (float): Latitude
        lon (float): Longitude
        
    Returns:
        tuple: (city, country) names
    """
    try:
        # Initialize Nominatim geocoder
        geolocator = Nominatim(user_agent="flowshield")
        # Reverse geocode the coordinates
        location = geolocator.reverse(f"{lat}, {lon}", language='en')
        
        if location and location.raw:
            address = location.raw.get('address', {})
            city = address.get('city') or address.get('town') or address.get('village') or "Unknown"
            country = address.get('country') or "Unknown"
            return city, country
        return "Unknown", "Unknown"
    except Exception as e:
        print(f"Error in geocoding: {e}")
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