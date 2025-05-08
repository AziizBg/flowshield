import requests
from datetime import datetime, timedelta

# Initial timestamp = last day
last_fetch_time_eonet = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')

def fetch_eonet_disasters():
    global last_fetch_time_eonet

    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    api_url = "https://eonet.gsfc.nasa.gov/api/v3/events"

    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    params = {
      'days':10,
      'status':'closed'
    }

    try:
        # print(f"Fetching EONET disaster updates from {current_date}...")

        if not hasattr(fetch_eonet_disasters, "processed_ids"):
            fetch_eonet_disasters.processed_ids = set()

        response = requests.get(api_url, params=params)
        data = response.json()

        new_events = []

        for event in data.get("events", []):
            event_id = event.get("id", "unknown")
            title = event.get("title", "N/A")
            updated = event.get("geometry", [{}])[-1].get("date", "N/A")
            updated_datetime = datetime.strptime(updated, "%Y-%m-%dT%H:%M:%SZ")
            # print(f"id:{event_id} \t\t updated:{updated_datetime}")

            if event_id not in fetch_eonet_disasters.processed_ids:
                fetch_eonet_disasters.processed_ids.add(event_id)

                categories = ', '.join([cat.get("title", "N/A") for cat in event.get("categories", [])])
                geometry = event.get("geometry", [{}])
                coordinates = geometry[-1].get("coordinates", [])
                location = f"{coordinates[1]:.2f}, {coordinates[0]:.2f}" if len(coordinates) == 2 else "N/A"

                new_events.append({
                    "source": "EONET",
                    "time_now": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    "last_modified": updated,
                    "event_name": title,
                    "alert_level": "N/A",
                    "country": location,
                    "type": categories,
                    "from_date": geometry[0].get("date", "N/A"),
                    "to_date": geometry[-1].get("date", "N/A"),
                    "affected_countries": "N/A",
                    "url": event.get("link", "N/A")
                })

        if new_events:
            # print("\nNew Disaster Events:")
            # print(f"{'Time'.ljust(25)} {'Last Modified'.ljust(25)} {'Source'.ljust(15)} {'Event Name'.ljust(30)} "
            #       f"{'Alert'.ljust(10)} {'Country'.ljust(25)} {'Severity'.ljust(50)} "
            #       f"{'From Date'.ljust(20)} {'To Date'.ljust(20)} "
            #       f"{'Affected Countries'.ljust(40)} URL")
            # print("-" * 210)

            for e in new_events:
                print(f"{e['time_now'].ljust(25)} {e['last_modified'].ljust(25)} {e['source'].ljust(15)} "
                      f"{e['event_name'].ljust(30)} {e['alert_level'].ljust(10)} "
                      f"{e['country'].ljust(25)} {e['severity'].ljust(50)} "
                      f"{e['from_date'].ljust(20)} {e['to_date'].ljust(20)} "
                      f"{e['affected_countries'].ljust(40)} {e['url']}")

            # Update last fetch time
            last_fetch_time_eonet = max(e['last_modified'] for e in new_events)

        else:
            print("No new events found.")
    except Exception as e:
        print(f"Error: {e}")

# Run the function
# fetch_eonet_disasters.processed_ids = set()
fetch_eonet_disasters()
