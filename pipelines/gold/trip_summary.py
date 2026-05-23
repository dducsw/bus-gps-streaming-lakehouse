# trip_detector.py

import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import datetime

# =========================================================
# CONFIG
# =========================================================

TERMINAL_RADIUS_METERS = 150
MIN_START_SPEED = 5
MIN_END_SPEED = 3
MIN_STOP_TIME_SEC = 120

# =========================================================
# HAVERSINE DISTANCE
# =========================================================

def haversine(lat1, lon1, lat2, lon2):
    """
    Distance between 2 GPS points in meters
    """

    lon1, lat1, lon2, lat2 = map(
        radians,
        [lon1, lat1, lon2, lat2]
    )

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    )

    c = 2 * asin(sqrt(a))

    r = 6371000

    return c * r


# =========================================================
# LOAD DATA
# =========================================================

vehicle_route_mapping = pd.read_csv(
    "vehicle_route_mapping.csv"
)

route_stops = pd.read_json(
    "route_stops.json"
)

gps_df = pd.read_json(
    "sample_waypoints.json"
)

# =========================================================
# FLATTEN GPS
# =========================================================

gps_records = []

for row in gps_df.to_dict("records"):

    if row["msgType"] != "MsgType_BusWayPoint":
        continue

    msg = row["msgBusWayPoint"]

    gps_records.append({
        "vehicle": msg.get("vehicle"),
        "datetime": msg.get("datetime"),
        "speed": msg.get("speed", 0),
        "lng": msg.get("x"),
        "lat": msg.get("y")
    })

gps = pd.DataFrame(gps_records)

gps["datetime"] = pd.to_datetime(
    gps["datetime"],
    unit="s"
)

gps = gps.sort_values([
    "vehicle",
    "datetime"
])

# =========================================================
# BUILD TERMINALS
# =========================================================

terminals = {}

grouped = route_stops.groupby([
    "RouteId",
    "RouteVarId"
])

for (route_id, route_var_id), group in grouped:

    group = group.reset_index(drop=True)

    first_stop = group.iloc[0]
    last_stop = group.iloc[-1]

    terminals[(route_id, route_var_id)] = {
        "start": first_stop,
        "end": last_stop
    }

# =========================================================
# MAP VEHICLE -> ROUTE
# =========================================================

vehicle_to_route = {}

for _, row in vehicle_route_mapping.iterrows():

    vehicle_to_route[row["vehicle"]] = {
        "route_id": row["route_id"],
        "route_no": row["route_no"]
    }

# =========================================================
# TRIP DETECTION
# =========================================================

vehicle_states = {}

trip_results = []

trip_counter = 1

for _, row in gps.iterrows():

    vehicle = row["vehicle"]

    if vehicle not in vehicle_to_route:
        continue

    route_info = vehicle_to_route[vehicle]

    route_id = route_info["route_id"]

    possible_vars = []

    for key in terminals.keys():
        if key[0] == route_id:
            possible_vars.append(key)

    if len(possible_vars) == 0:
        continue

    best_var = None
    best_distance = 999999999

    for key in possible_vars:

        terminal = terminals[key]

        start_stop = terminal["start"]

        dist = haversine(
            row["lat"],
            row["lng"],
            start_stop["Lat"],
            start_stop["Lng"]
        )

        if dist < best_distance:
            best_distance = dist
            best_var = key

    route_var_id = best_var[1]

    terminal = terminals[best_var]

    start_stop = terminal["start"]
    end_stop = terminal["end"]

    dist_to_start = haversine(
        row["lat"],
        row["lng"],
        start_stop["Lat"],
        start_stop["Lng"]
    )

    dist_to_end = haversine(
        row["lat"],
        row["lng"],
        end_stop["Lat"],
        end_stop["Lng"]
    )

    if vehicle not in vehicle_states:

        vehicle_states[vehicle] = {
            "state": "IDLE",
            "trip": None
        }

    state = vehicle_states[vehicle]

    # =====================================================
    # START TRIP
    # =====================================================

    if state["state"] == "IDLE":

        if (
            dist_to_start <= TERMINAL_RADIUS_METERS
            and row["speed"] >= MIN_START_SPEED
        ):

            state["state"] = "RUNNING"

            state["trip"] = {
                "trip_id": trip_counter,
                "vehicle": vehicle,
                "route_id": route_id,
                "route_var_id": route_var_id,
                "start_time": row["datetime"],
                "start_stop": start_stop["Name"],
                "start_stop_id": start_stop["StopId"]
            }

            print(
                f"[START] Trip {trip_counter} | "
                f"Vehicle={vehicle[:8]} | "
                f"Time={row['datetime']}"
            )

            trip_counter += 1

    # =====================================================
    # END TRIP
    # =====================================================

    elif state["state"] == "RUNNING":

        if (
            dist_to_end <= TERMINAL_RADIUS_METERS
            and row["speed"] <= MIN_END_SPEED
        ):

            trip = state["trip"]

            trip["end_time"] = row["datetime"]
            trip["end_stop"] = end_stop["Name"]
            trip["end_stop_id"] = end_stop["StopId"]

            duration = (
                trip["end_time"]
                - trip["start_time"]
            ).total_seconds()

            trip["duration_sec"] = duration

            trip["completed"] = True

            trip_results.append(trip)

            print(
                f"[END] Trip {trip['trip_id']} | "
                f"Duration={duration/60:.1f} mins"
            )

            state["state"] = "IDLE"
            state["trip"] = None

# =========================================================
# SAVE RESULT
# =========================================================

trip_summary = pd.DataFrame(trip_results)

trip_summary.to_csv(
    "trip_summary.csv",
    index=False
)

print("\n==============================")
print("TRIP SUMMARY")
print("==============================")

print(trip_summary.head())

print("\nSaved to trip_summary.csv")