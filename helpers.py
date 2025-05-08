import reverse_geocoder as rg
import pycountry

def get_location_info(lat, lon):
    try:
        result = rg.search((lat, lon), mode=1)[0]
        city = result['name']
        country_code = result['cc']
        country = pycountry.countries.get(alpha_2=country_code)
        country_name = country.name if country else "Unknown"
        return city, country_name
    except Exception as e:
        print(f"Error in reverse_geocoder: {e}")
        return "Unknown", "Unknown"

def get_fire_severity(frp):
  # Define severity levels
  if frp < 5:
    severity = "Low"
  elif frp < 15:
    severity = "Moderate"
  else:
    severity = "High"
  return severity