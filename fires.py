from datetime import datetime, timedelta
import requests
import csv
from geopy.geocoders import Nominatim
from helpers import get_fire_severity, get_location_info


def fetch_fires(numberOfMinutes=1):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    url = f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/477785c4a607ad274bbbb9cbdcdd6bef/VIIRS_SNPP_NRT/world/1/{today}'
    # skipped = 0
    # matched = 0

    try:
        # print(f"fetching fires from {numberOfMinutes} minutes starting from one hour ago")

        response = requests.get(url, timeout=10)
        decoded_content = response.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        headers = next(cr)

        # Keep track of processed fires
        if not hasattr(fetch_fires, "processed_fires"):
            fetch_fires.processed_fires = []

        # Get the reference time as 3 hours ago
        three_hours_ago = datetime.utcnow() - timedelta(hours=3)

        for row in cr:
            fire_data = dict(zip(headers, row))
            acq_date = fire_data.get('acq_date')
            acq_time = fire_data.get('acq_time')

            if acq_date and acq_time:
                acq_time = acq_time.zfill(4)  # Pad to 4 digits (HHMM)

                timestamp_str = f"{acq_date} {acq_time}"
                fire_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H%M")
                latitude = fire_data.get('latitude')
                longitude = fire_data.get('longitude')
                city, country = get_location_info(float(latitude), float(longitude))
                frp = float(fire_data.get('frp', 0))  # Get FRP, default to 0 if missing
                # severity = get_fire_severity(frp)

                # Create a unique ID
                fire_id = f"{fire_data.get('latitude')}_{fire_data.get('longitude')}_{acq_date}_{acq_time}"

                fire_info={
                    "id": fire_id,
                    "time": fire_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "latitude": latitude,
                    "longitude": longitude,
                    "country": country,
                    "city": city,
                    "frp": frp,
                    # "severity": severity,
                    "type":"fire"
                }

                # Filter: only fires within the last x minutes starting from 1 hour ago
                time_diff = three_hours_ago - fire_dt
                if time_diff.total_seconds() <= (numberOfMinutes * 60):
                  # if fire_id not in fetch_fires.processed_fires:
                      # print(f"[FIRE] Time: {fire_dt}, Latitude: {latitude}, Longitude: {longitude}, Country: {country}, City: {city}, FRP: {frp}, Severity: {severity}")
                      fetch_fires.processed_fires.append(fire_info)
                    #   matched += 1
                  # else:
                  #     skipped += 1

        # print(f"Finished processing fires: {matched} matched, {skipped} skipped")

    except Exception as e:
        print(f"Error fetching fires: {e}")

    return fetch_fires.processed_fires

fetch_fires.processed_fires = []
fetch_fires(numberOfMinutes=1)
