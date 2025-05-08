from datetime import datetime
import requests


def fetch_earthquakes():
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
    try:
        response = requests.get(url)
        data = response.json()

        if not hasattr(fetch_earthquakes, "quakes"):
            fetch_earthquakes.quakes = []

        for feature in data['features']:
                quake_id = feature['id']
                props = feature['properties']
                timestamp_ms = props['time']
                formatted_time = datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
                
                # Add relevant info to new_quakes
                quake_info = {
                    "id": quake_id,
                    "time": feature['properties']['time'],
                    'magnitude': props.get('mag'),
                    'place': props.get('place'),
                    'time': formatted_time,
                    'url': props.get('url'),
                    'type': props.get('type'),
                    'status': props.get('status'),
                    'magType': props.get('magType')
                }
                fetch_earthquakes.quakes.append(quake_info)
        
        return fetch_earthquakes.quakes
    except Exception as e:
        print(f"Error fetching earthquakes: {e}")
        return []
