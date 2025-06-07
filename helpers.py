import reverse_geocoder as rg
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
        # Search for location information
        coordinates = (float(lat), float(lon))
        result = rg.search(coordinates)[0]
        city = result['name']
        country_code = result['cc']
        country = pycountry.countries.get(alpha_2=country_code)
        country_name = country.name if country else "Unknown"
        return city, country_name
    except Exception as e:
        print(f"Error in reverse_geocoder: {e}")
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