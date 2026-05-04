"""
Dhaka AQI + Rain + Flood Data Collector
========================================
APIs used:
  - AQICN          → AQI, PM2.5, PM10 (needs free token)
  - Open-Meteo     → rainfall mm, river discharge/flood (NO key needed)
  - OpenWeatherMap → rain confirmation, storm flags (needs free token)

Run 3x daily via GitHub Actions (08:00, 14:00, 20:00 UTC).
"""

import os
import csv
import requests
from datetime import datetime, timezone, timedelta

# ── Credentials (set as GitHub Secrets) ──────────────────────────────────────
AQICN_TOKEN = os.environ.get("AQICN_TOKEN", "")
OWM_TOKEN   = os.environ.get("OWM_TOKEN", "")     # openweathermap.org free key

# ── Dhaka coordinates ─────────────────────────────────────────────────────────
DHAKA_LAT  = 23.8103
DHAKA_LON  = 90.4125

# ── Buriganga river gauge station (Open-Meteo flood API) ─────────────────────
# Nearest GloFAS river discharge station to Dhaka
RIVER_LAT  = 23.72
RIVER_LON  = 90.38

BST_OFFSET = timedelta(hours=6)
CSV_FILE   = "dhaka_air_quality.csv"

# Thresholds
RAIN_MM_THRESHOLD       = 0.5    # mm/hr  → rain event
FLOOD_DISCHARGE_THRESHOLD = 3000  # m³/s  → flood alert (Buriganga baseline ~800)
FLOOD_HUMIDITY_THRESHOLD  = 90    # % fallback

FIELDNAMES = [
    "datetime_bst", "day_of_week", "session", "city", "season",
    # AQI
    "aqi", "pm25", "pm10",
    # Weather
    "temperature_c", "humidity_pct", "wind_speed_ms",
    # Rain
    "rainfall_mm_openmeteo", "rainfall_mm_owm", "rain_event",
    # Flood
    "river_discharge_m3s", "flood_event",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_season(month):
    if month in (12, 1, 2):      return "Winter"
    elif month in (3, 4, 5):     return "Summer"
    elif month in (6, 7, 8, 9):  return "Monsoon"
    else:                         return "Post-monsoon"

def get_session(hour):
    if 5 <= hour < 12:    return "Morning"
    elif 12 <= hour < 18: return "Afternoon"
    else:                  return "Night"

def safe_get(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict): return default
        d = d.get(k, {})
    return d if d != {} else default

def get(url, params=None, timeout=15):
    """GET request, returns parsed JSON or None on any error."""
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  [WARN] Request failed {url}: {e}")
        return None

# ── Source 1: AQICN ───────────────────────────────────────────────────────────

def fetch_aqicn():
    print("[1/3] Fetching AQICN...")
    if not AQICN_TOKEN:
        print("  [SKIP] AQICN_TOKEN not set")
        return {}
    data = get(f"https://api.waqi.info/feed/dhaka/?token={AQICN_TOKEN}")
    if not data or data.get("status") != "ok":
        return {}
    d    = data["data"]
    iaqi = d.get("iaqi", {})
    result = {
        "aqi"          : safe_get(d,    "aqi"),
        "pm25"         : safe_get(iaqi, "pm25", "v"),
        "pm10"         : safe_get(iaqi, "pm10", "v"),
        "temperature_c": safe_get(iaqi, "t",    "v"),
        "humidity_pct" : safe_get(iaqi, "h",    "v"),
        "wind_speed_ms": safe_get(iaqi, "w",    "v"),
    }
    print(f"  AQI={result['aqi']}  PM2.5={result['pm25']}  PM10={result['pm10']}")
    return result

# ── Source 2: Open-Meteo (rain + river discharge) ─────────────────────────────

def fetch_open_meteo():
    print("[2/3] Fetching Open-Meteo (rain + river discharge)...")
    rainfall_mm      = None
    river_discharge  = None

    # --- Hourly weather (rain) ---
    weather = get("https://api.open-meteo.com/v1/forecast", params={
        "latitude"       : DHAKA_LAT,
        "longitude"      : DHAKA_LON,
        "hourly"         : "precipitation",
        "forecast_days"  : 1,
        "timezone"       : "Asia/Dhaka",
    })
    if weather:
        hours  = weather.get("hourly", {}).get("time", [])
        precip = weather.get("hourly", {}).get("precipitation", [])
        now_bst = datetime.now(timezone.utc) + BST_OFFSET
        current_hour_str = now_bst.strftime("%Y-%m-%dT%H:00")
        for t, p in zip(hours, precip):
            if t == current_hour_str:
                rainfall_mm = p
                break
        if rainfall_mm is None and precip:
            rainfall_mm = precip[-1]   # latest available
    print(f"  rainfall_mm (Open-Meteo) = {rainfall_mm}")

    # --- River discharge (flood proxy) ---
    flood = get("https://flood-api.open-meteo.com/v1/flood", params={
        "latitude"     : RIVER_LAT,
        "longitude"    : RIVER_LON,
        "daily"        : "river_discharge",
        "forecast_days": 1,
    })
    if flood:
        discharge_list = flood.get("daily", {}).get("river_discharge", [])
        if discharge_list:
            river_discharge = discharge_list[0]
    print(f"  river_discharge (m³/s)   = {river_discharge}")

    return {
        "rainfall_mm_openmeteo": rainfall_mm,
        "river_discharge_m3s"  : river_discharge,
    }

# ── Source 3: OpenWeatherMap (rain confirmation) ──────────────────────────────

def fetch_owm():
    print("[3/3] Fetching OpenWeatherMap...")
    if not OWM_TOKEN:
        print("  [SKIP] OWM_TOKEN not set")
        return {"rainfall_mm_owm": None}
    data = get("https://api.openweathermap.org/data/2.5/weather", params={
        "lat"   : DHAKA_LAT,
        "lon"   : DHAKA_LON,
        "appid" : OWM_TOKEN,
        "units" : "metric",
    })
    if not data:
        return {"rainfall_mm_owm": None}

    # OWM rain is under data["rain"]["1h"] (may be absent on dry days)
    rain_1h = None
    rain_block = data.get("rain", {})
    if rain_block:
        rain_1h = rain_block.get("1h", rain_block.get("3h", 0))

    print(f"  rainfall_mm (OWM 1h) = {rain_1h}")
    return {"rainfall_mm_owm": rain_1h}

# ── Rain & Flood classification ───────────────────────────────────────────────

def classify_rain(rainfall_openmeteo, rainfall_owm, humidity_pct):
    """
    Combine both rain sources. Returns 'Yes' / 'No' / 'Unknown'.
    """
    readings = []
    if rainfall_openmeteo is not None:
        readings.append(float(rainfall_openmeteo))
    if rainfall_owm is not None:
        readings.append(float(rainfall_owm))

    if readings:
        avg = sum(readings) / len(readings)
        return "Yes" if avg >= RAIN_MM_THRESHOLD else "No"

    # Fallback: humidity
    if humidity_pct is not None:
        return "Yes" if float(humidity_pct) >= FLOOD_HUMIDITY_THRESHOLD else "No"

    return "Unknown"

def classify_flood(river_discharge_m3s):
    """
    Returns 'Yes' / 'No' / 'Unknown' based on river discharge.
    """
    if river_discharge_m3s is None:
        return "Unknown"
    return "Yes" if float(river_discharge_m3s) >= FLOOD_DISCHARGE_THRESHOLD else "No"

# ── Assemble & write row ──────────────────────────────────────────────────────

def build_row(aqicn, meteo, owm):
    now_bst = datetime.now(timezone.utc) + BST_OFFSET

    rainfall_om  = meteo.get("rainfall_mm_openmeteo")
    rainfall_owm = owm.get("rainfall_mm_owm")
    humidity     = aqicn.get("humidity_pct")
    discharge    = meteo.get("river_discharge_m3s")

    rain_event  = classify_rain(rainfall_om, rainfall_owm, humidity)
    flood_event = classify_flood(discharge)

    print(f"\n  >> rain_event  = {rain_event}")
    print(f"  >> flood_event = {flood_event}\n")

    return {
        "datetime_bst"         : now_bst.strftime("%Y-%m-%d %H:%M"),
        "day_of_week"          : now_bst.strftime("%A"),
        "session"              : get_session(now_bst.hour),
        "city"                 : "Dhaka",
        "season"               : get_season(now_bst.month),
        "aqi"                  : aqicn.get("aqi"),
        "pm25"                 : aqicn.get("pm25"),
        "pm10"                 : aqicn.get("pm10"),
        "temperature_c"        : aqicn.get("temperature_c"),
        "humidity_pct"         : humidity,
        "wind_speed_ms"        : aqicn.get("wind_speed_ms"),
        "rainfall_mm_openmeteo": rainfall_om,
        "rainfall_mm_owm"      : rainfall_owm,
        "rain_event"           : rain_event,
        "river_discharge_m3s"  : discharge,
        "flood_event"          : flood_event,
    }

def append_to_csv(row):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    print(f"[OK] Saved → {CSV_FILE}  ({row['datetime_bst']})")

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  Dhaka AQI + Rain + Flood Collector")
    print("=" * 50)
    aqicn = fetch_aqicn()
    meteo = fetch_open_meteo()
    owm   = fetch_owm()
    row   = build_row(aqicn, meteo, owm)
    append_to_csv(row)
    print("=== Done ===")

if __name__ == "__main__":
    main()
