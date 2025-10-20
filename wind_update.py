import requests
import json
from datetime import datetime, timedelta, timezone
import pytz
import os

# ----------------------------
# Config
# ----------------------------

# Asset Panda configuration
ASSET_PANDA_GROUP_ID = "208975"
ASSET_PANDA_BEARER_TOKEN = os.environ.get('ASSET_PANDA_BEARER_TOKEN')
ASSET_PANDA_FETCH_URL = f"https://api.assetpanda.com/v3/groups/{ASSET_PANDA_GROUP_ID}/search/objects"
ASSET_PANDA_UPDATE_URL = f"https://api.assetpanda.com/v3/groups/{ASSET_PANDA_GROUP_ID}/objects"

# Svantek API configuration
SVANTEK_API_KEY = os.environ.get('SVANTEK_API_KEY')
PROJECTS_URL = "https://svannet.com/api/v2.5/projects-get-data.php"
DATA_URL = "https://svannet.com/api/v2.5/projects-get-result-data.php"

LOCAL_TZ = pytz.timezone("America/Denver")

# ----------------------------
# Utilities
# ----------------------------

def parse_utc(ts: str) -> datetime:
    """
    Parse timestamps that may include:
      - space or 'T' separator
      - optional fractional seconds
      - optional trailing 'Z' or explicit offset
    Return timezone-aware UTC datetime.
    """
    if not ts:
        raise ValueError("Empty timestamp")
    s = ts.strip().replace('T', ' ').replace('Z', '')
    # Try common precise then coarse formats
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    # Fallback: ISO with offset like +00:00
    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

# ----------------------------
# Asset Panda
# ----------------------------

def get_asset_panda_data():
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ASSET_PANDA_BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"search": {}}

    try:
        response = requests.post(ASSET_PANDA_FETCH_URL, headers=headers, json=payload, timeout=20)
        if response.status_code == 200:
            data = response.json()
            objects = data.get('objects', [])
            return [
                {
                    'id': item.get('id', 'Not specified'),
                    'Pad Name': item.get('data', {}).get('field_1', 'Not specified'),
                    'GPS': item.get('data', {}).get('field_2', 'Not specified'),
                    'Status': item.get('data', {}).get('field_68', {}).get('value', 'Not specified')
                }
                for item in objects
            ]
        else:
            print(f"Asset Panda Fetch Error: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Asset Panda Fetch Request Error: {e}")
        return []

def update_asset_panda_wind_speeds(results):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ASSET_PANDA_BEARER_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "objects": [
            {
                "id": result["object_id"],
                "field_108": result["max_wind_speed_str"]  # "##.## mph at YYYY-MM-DD HH:MM:SS TZ"
            }
            for result in results if result["object_id"] != "Not specified"
        ]
    }

    try:
        response = requests.put(ASSET_PANDA_UPDATE_URL, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            print("\nSuccessfully updated Asset Panda with wind speeds.")
        else:
            print(f"\nAsset Panda Update Error: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"\nAsset Panda Update Request Error: {e}")

# ----------------------------
# Helpers
# ----------------------------

def parse_gps(gps_str):
    if gps_str == "Not specified":
        return None, None
    try:
        lat, lon = map(float, gps_str.split(","))
        return lat, lon
    except (ValueError, AttributeError):
        return None, None

# ----------------------------
# Open-Meteo
# ----------------------------

def get_openmeteo_max_gusts(locations):
    results = []
    for location in locations:
        latitude, longitude, name, status, obj_id = location
        if latitude is None or longitude is None:
            continue

        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={latitude}&longitude={longitude}"
            "&hourly=windgusts_10m&past_days=1&timezone=UTC"
        )

        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"Open-Meteo error for {name}: {e}")
            continue

        if "hourly" in data and "windgusts_10m" in data["hourly"]:
            gusts_kmh = data["hourly"]["windgusts_10m"]
            timestamps = data["hourly"]["time"]

            utc_times = [datetime.strptime(ts, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc) for ts in timestamps]
            now_utc = datetime.now(timezone.utc)
            start_time = now_utc - timedelta(hours=24)

            recent_indices = [i for i, t in enumerate(utc_times) if start_time <= t <= now_utc]
            if not recent_indices:
                continue

            recent_gusts = [gusts_kmh[i] for i in recent_indices]
            max_gust_kmh = max(recent_gusts)
            max_index = recent_gusts.index(max_gust_kmh)
            max_timestamp_utc = utc_times[recent_indices[max_index]]

            max_gust_mph = round(max_gust_kmh * 0.621371, 2)
            local_time = max_timestamp_utc.astimezone(LOCAL_TZ)
            local_time_str = local_time.strftime("%Y-%m-%d %H:%M:%S %Z")

            max_wind_speed_str = f"{max_gust_mph:.2f} mph at {local_time_str}"

            results.append({
                "source": "Open-Meteo",
                "name": name.upper(),
                "max_wind_speed_mph": max_gust_mph,
                "formatted": f"{name.upper()} (Open-Meteo): Max Wind Gust = {max_gust_mph:.2f} mph at {local_time_str} (Status: {status})",
                "object_id": obj_id,
                "max_wind_speed_str": max_wind_speed_str
            })
    return results

# ----------------------------
# Svantek
# ----------------------------

def fetch_projects_and_points():
    payload = {"key": SVANTEK_API_KEY}
    try:
        response = requests.post(PROJECTS_URL, data=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "ok":
            project_data = {}
            for project in data["projects"]:
                project_id = project["id"]
                project_name = project["project_name"]
                points = [point["point_id"] for point in project["stations"]]
                project_data[project_id] = {"name": project_name, "points": points}
            return project_data
        return {}
    except requests.RequestException as e:
        print(f"Svantek project fetch error: {e}")
        return {}

def fetch_svantek_wind_data(project_id, point_id):
    now = datetime.now(timezone.utc)
    time_to = now.strftime("%Y-%m-%d %H:%M:%S")
    time_from = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "key": SVANTEK_API_KEY,
        "project": project_id,
        "point": point_id,
        "time_from": time_from,
        "time_to": time_to,
        "results": json.dumps(["meteo_wind_speed_max-T"])
    }

    try:
        response = requests.post(DATA_URL, data=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "ok":
            return data
        return None
    except requests.RequestException as e:
        print(f"Svantek data fetch error (project {project_id}, point {point_id}): {e}")
        return None

def find_svantek_max_wind_speed(project_id, project_name, data):
    if not data or "results" not in data:
        return None, None

    max_wind_speed_ms = 0.0
    max_timestamp = None

    for result in data["results"]:
        for record in result.get("data", []):
            values = record.get("values", [])
            if not values:
                continue
            wind_speed_str = values[0]
            if wind_speed_str != "---":
                try:
                    wind_speed = float(wind_speed_str)
                except ValueError:
                    continue
                if wind_speed > max_wind_speed_ms:
                    max_wind_speed_ms = wind_speed
                    max_timestamp = record.get("timestamp")

    if max_timestamp:
        try:
            utc_time = parse_utc(max_timestamp)
            mst_time = utc_time.astimezone(LOCAL_TZ)
            formatted_time = mst_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception as e:
            print(f"Timestamp parse error for Svantek '{max_timestamp}': {e}")
            formatted_time = "Invalid timestamp"
    else:
        formatted_time = "No timestamp available"

    return max_wind_speed_ms, formatted_time

def get_svantek_wind_speed(pad_data):
    project_data = fetch_projects_and_points()
    if not project_data:
        return []

    results = []
    for pad in pad_data:
        pad_name = pad['Pad Name'].upper()
        status = pad['Status']
        obj_id = pad['id']
        if status not in ["Active", "Idle"]:
            continue

        project_max_wind_ms = 0.0
        project_max_time = None

        for project_id, pdata in project_data.items():
            project_name = pdata["name"].upper()
            if pad_name in project_name or project_name in pad_name:
                points = pdata["points"]
                for point_id in points:
                    sv_data = fetch_svantek_wind_data(project_id, point_id)
                    if not sv_data:
                        continue
                    max_wind_ms, max_time = find_svantek_max_wind_speed(project_id, project_name, sv_data)
                    if max_wind_ms and max_wind_ms > project_max_wind_ms:
                        project_max_wind_ms = max_wind_ms
                        project_max_time = max_time

        if project_max_wind_ms > 0:
            max_wind_mph = round(project_max_wind_ms * 2.23694, 2)
            formatted_str = f"{pad_name} (Svantek): Max Wind Speed = {max_wind_mph:.2f} mph at {project_max_time} (Status: {status})"
            max_wind_speed_str = f"{max_wind_mph:.2f} mph at {project_max_time}"

            results.append({
                "source": "Svantek",
                "name": pad_name,
                "max_wind_speed_mph": max_wind_mph,
                "formatted": formatted_str,
                "object_id": obj_id,
                "max_wind_speed_str": max_wind_speed_str
            })

    return results

# ----------------------------
# Main
# ----------------------------

def main():
    # Fetch Asset Panda data
    pad_data = get_asset_panda_data()
    if not pad_data:
        print("Failed to fetch Asset Panda data. Exiting.")
        return

    # Filter for Active or Idle pads and prepare locations for Open-Meteo
    locations = []
    for pad in pad_data:
        if pad['Status'] in ["Active", "Idle"]:
            lat, lon = parse_gps(pad['GPS'])
            if lat is not None and lon is not None:
                locations.append((lat, lon, pad['Pad Name'], pad['Status'], pad['id']))

    # Fetch Svantek data first
    svantek_results = get_svantek_wind_speed(pad_data)
    svantek_pad_names = {r["name"] for r in svantek_results}

    # Fetch Open-Meteo data only for pads not covered by Svantek
    openmeteo_locations = [loc for loc in locations if loc[2].upper() not in svantek_pad_names]
    openmeteo_results = get_openmeteo_max_gusts(openmeteo_locations)

    # Combine results with Svantek prioritized
    all_results = svantek_results + openmeteo_results
    all_results.sort(key=lambda x: x["max_wind_speed_mph"], reverse=True)

    # Print report
    print(f"Run at {datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("Subject: Wind Speed Report (Last 24 Hours)\n")
    print("Below is the wind speed report for the last 24 hours (Active/Idle Pads Only):\n")
    for result in all_results:
        print(result["formatted"] + ";")

    if all_results:
        overall_max = max(all_results, key=lambda x: x["max_wind_speed_mph"])
        print(f"\nOverall max wind speed: {overall_max['formatted']};")

    # Update Asset Panda with wind speeds
    update_asset_panda_wind_speeds(all_results)

if __name__ == "__main__":
    main()
