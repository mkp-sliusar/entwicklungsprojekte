
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from flask import Flask, jsonify, render_template_string, request
from psycopg.rows import dict_row


APP_HOST = os.getenv("GNSS_APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("GNSS_APP_PORT", "8050"))
APP_TITLE = os.getenv("GNSS_APP_TITLE", "GNSS Live & Historie")
LIVE_REFRESH_MS = int(os.getenv("GNSS_LIVE_REFRESH_MS", "1000"))

DB_HOST = os.getenv("GNSS_DB_HOST", "10.20.38.240")
DB_PORT = int(os.getenv("GNSS_DB_PORT", "5432"))
DB_NAME = os.getenv("GNSS_DB_NAME", "sensors_db")
DB_USER = os.getenv("GNSS_DB_USER", "iris_user")
DB_PASSWORD = os.getenv("GNSS_DB_PASSWORD", "")
DB_CONNECT_TIMEOUT = int(os.getenv("GNSS_DB_CONNECT_TIMEOUT", "10"))

DEFAULT_LIVE_MINUTES = int(os.getenv("GNSS_LIVE_MINUTES", "5"))
MAX_HISTORY_HOURS = int(os.getenv("GNSS_MAX_HISTORY_HOURS", "48"))
MAX_POINTS_PER_VEHICLE = int(os.getenv("GNSS_MAX_POINTS_PER_VEHICLE", "12000"))
MAX_INCREMENTAL_ROWS = int(os.getenv("GNSS_MAX_INCREMENTAL_ROWS", "5000"))

AXES_GEOJSON_PATH = Path(os.getenv("GNSS_AXES_GEOJSON", "axes.geojson"))
AXES_DEFAULT_VISIBLE = os.getenv("GNSS_AXES_VISIBLE", "true").strip().lower() in {
    "1", "true", "yes", "on",
}
AXES_WIDTH_M = float(os.getenv("GNSS_AXES_WIDTH_M", "24"))
PROJECTS_CONFIG_PATH = Path(
    os.getenv("GNSS_PROJECTS_CONFIG", "bridge_projects.json")
)
EARTH_RADIUS_M = 6_371_008.8


def optional_float(name: str) -> float | None:
    value = os.getenv(name, "").strip()
    return float(value) if value else None


AXIS_A_LAT = optional_float("GNSS_AXIS_A_LAT")
AXIS_A_LON = optional_float("GNSS_AXIS_A_LON")
AXIS_L_LAT = optional_float("GNSS_AXIS_L_LAT")
AXIS_L_LON = optional_float("GNSS_AXIS_L_LON")

# Achsabstaende gemaess Abbildung 4 des bereitgestellten Plans.
# Die Bezeichnung I wird im Plan uebersprungen.
PLAN_AXIS_STATIONS_M = (
    ("A", 0.0),
    ("B", 30.0),
    ("C", 61.5),
    ("D", 98.5),
    ("E", 132.0),
    ("F", 165.5),
    ("G", 199.0),
    ("H", 232.5),
    ("J", 271.0),
    ("K", 306.5),
    ("L", 323.5),
)

TABLE = 'public."GNSS_Position_Belastungsfahrzeug"'

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("gnss-viewer")

app = Flask(__name__)


def destination_point(
    lat: float,
    lon: float,
    bearing_deg: float,
    distance_m: float,
) -> tuple[float, float]:
    radius_m = 6_371_008.8
    angular_distance = distance_m / radius_m
    bearing = math.radians(bearing_deg)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)

    lat2 = math.asin(
        math.sin(lat1) * math.cos(angular_distance)
        + math.cos(lat1) * math.sin(angular_distance) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular_distance) * math.cos(lat1),
        math.cos(angular_distance) - math.sin(lat1) * math.sin(lat2),
    )

    return math.degrees(lat2), math.degrees(lon2)


def initial_bearing_deg(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_lon = math.radians(lon2 - lon1)
    y = math.sin(delta_lon) * math.cos(phi2)
    x = (
        math.cos(phi1) * math.sin(phi2)
        - math.sin(phi1) * math.cos(phi2) * math.cos(delta_lon)
    )
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def validate_axes_geojson(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise ValueError("Achsendatei muss eine GeoJSON FeatureCollection sein.")

    valid_features: list[dict[str, Any]] = []

    for feature in data.get("features", []):
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            continue

        geometry = feature.get("geometry") or {}
        properties = feature.get("properties") or {}
        axis = str(properties.get("axis") or properties.get("name") or "").strip()
        coordinates = geometry.get("coordinates")

        if geometry.get("type") != "LineString" or not axis:
            continue
        if not isinstance(coordinates, list) or len(coordinates) < 2:
            continue

        normalized_coordinates: list[list[float]] = []
        valid = True

        for coordinate in coordinates:
            if not isinstance(coordinate, list) or len(coordinate) < 2:
                valid = False
                break

            lon = float(coordinate[0])
            lat = float(coordinate[1])

            if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                valid = False
                break

            normalized_coordinates.append([lon, lat])

        if not valid:
            continue

        valid_features.append(
            {
                "type": "Feature",
                "properties": {
                    **properties,
                    "axis": axis,
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": normalized_coordinates,
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": valid_features,
    }


def generated_axes_geojson() -> dict[str, Any] | None:
    reference = (AXIS_A_LAT, AXIS_A_LON, AXIS_L_LAT, AXIS_L_LON)

    if any(value is None for value in reference):
        return None

    a_lat, a_lon, l_lat, l_lon = (float(value) for value in reference)
    bridge_bearing = initial_bearing_deg(a_lat, a_lon, l_lat, l_lon)
    total_station_m = PLAN_AXIS_STATIONS_M[-1][1]
    half_width_m = AXES_WIDTH_M / 2.0
    features: list[dict[str, Any]] = []

    for axis, station_m in PLAN_AXIS_STATIONS_M:
        fraction = station_m / total_station_m
        center_lat = a_lat + (l_lat - a_lat) * fraction
        center_lon = a_lon + (l_lon - a_lon) * fraction
        left_lat, left_lon = destination_point(
            center_lat,
            center_lon,
            bridge_bearing - 90.0,
            half_width_m,
        )
        right_lat, right_lon = destination_point(
            center_lat,
            center_lon,
            bridge_bearing + 90.0,
            half_width_m,
        )
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "axis": axis,
                    "station_m": station_m,
                    "source": "plan-stations-and-wgs84-reference-points",
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [left_lon, left_lat],
                        [right_lon, right_lat],
                    ],
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def load_axes_geojson() -> tuple[dict[str, Any], str]:
    if AXES_GEOJSON_PATH.is_file():
        with AXES_GEOJSON_PATH.open("r", encoding="utf-8") as source_file:
            geojson = validate_axes_geojson(json.load(source_file))

        if geojson["features"]:
            return geojson, "geojson"

    generated = generated_axes_geojson()

    if generated is not None:
        return generated, "plan-stations"

    return {"type": "FeatureCollection", "features": []}, "not-configured"


def load_projects_config() -> dict[str, Any]:
    with PROJECTS_CONFIG_PATH.open("r", encoding="utf-8") as source_file:
        config = json.load(source_file)

    projects = config.get("projects")
    if not isinstance(projects, list) or not projects:
        raise ValueError("Projektdatei muss eine nichtleere projects-Liste enthalten.")

    ids: set[str] = set()
    for project in projects:
        project_id = str(project.get("id", "")).strip()
        if not project_id or project_id in ids:
            raise ValueError("Jedes Brückenprojekt braucht eine eindeutige id.")
        ids.add(project_id)

    default_project = str(config.get("default_project", "")).strip()
    if default_project not in ids:
        raise ValueError("default_project verweist auf kein vorhandenes Projekt.")

    return config


def project_by_id(project_id: str | None) -> tuple[dict[str, Any], dict[str, Any]]:
    config = load_projects_config()
    selected_id = (project_id or config["default_project"]).strip()

    for project in config["projects"]:
        if project["id"] == selected_id:
            return config, project

    raise ValueError(f"Unbekanntes Brückenprojekt: {selected_id}")


def local_xy_m(
    lat: float,
    lon: float,
    origin_lat: float,
    origin_lon: float,
) -> tuple[float, float]:
    reference_lat = math.radians(origin_lat)
    x = math.radians(lon - origin_lon) * EARTH_RADIUS_M * math.cos(reference_lat)
    y = math.radians(lat - origin_lat) * EARTH_RADIUS_M
    return x, y


def local_lat_lon(
    x: float,
    y: float,
    origin_lat: float,
    origin_lon: float,
) -> tuple[float, float]:
    reference_lat = math.radians(origin_lat)
    lat = origin_lat + math.degrees(y / EARTH_RADIUS_M)
    lon = origin_lon + math.degrees(x / (EARTH_RADIUS_M * math.cos(reference_lat)))
    return lat, lon


def circular_arc_geometry(axes: dict[str, Any]) -> dict[str, Any]:
    start_lat, start_lon = (float(value) for value in axes["start"])
    end_lat, end_lon = (float(value) for value in axes["end"])
    radius_m = float(axes["radius_m"])
    end_x, end_y = local_xy_m(end_lat, end_lon, start_lat, start_lon)
    chord_m = math.hypot(end_x, end_y)

    if radius_m <= 0 or chord_m <= 0 or chord_m > 2 * radius_m:
        raise ValueError("Endpunkte und Radius bilden keinen gültigen Kreisbogen.")

    midpoint_x = end_x / 2.0
    midpoint_y = end_y / 2.0
    center_offset_m = math.sqrt(radius_m**2 - (chord_m / 2.0) ** 2)
    left_x = -end_y / chord_m
    left_y = end_x / chord_m
    side = -1.0 if axes.get("center_side", "left") == "right" else 1.0
    center_x = midpoint_x + side * center_offset_m * left_x
    center_y = midpoint_y + side * center_offset_m * left_y
    start_angle = math.atan2(-center_y, -center_x)
    end_angle = math.atan2(end_y - center_y, end_x - center_x)
    signed_delta = math.atan2(
        math.sin(end_angle - start_angle),
        math.cos(end_angle - start_angle),
    )
    direction = 1.0 if signed_delta >= 0 else -1.0
    total_angle = abs(signed_delta)
    geometric_length_m = radius_m * total_angle
    plan_length_m = float(axes["stations"][-1][1])

    return {
        "origin": [start_lat, start_lon],
        "center_xy_m": [center_x, center_y],
        "radius_m": radius_m,
        "start_angle_rad": start_angle,
        "direction": direction,
        "total_angle_rad": total_angle,
        "chord_m": chord_m,
        "geometric_length_m": geometric_length_m,
        "plan_length_m": plan_length_m,
        "closure_difference_m": geometric_length_m - plan_length_m,
    }


def generated_circular_axes(
    axes: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    geometry = circular_arc_geometry(axes)
    origin_lat, origin_lon = geometry["origin"]
    center_x, center_y = geometry["center_xy_m"]
    radius_m = geometry["radius_m"]
    start_angle = geometry["start_angle_rad"]
    direction = geometry["direction"]
    half_width_m = float(axes.get("width_m", 14.7)) / 2.0
    exact_end = bool(axes.get("force_exact_end_axis", False))
    stations = axes["stations"]
    features: list[dict[str, Any]] = []

    for index, station in enumerate(stations):
        axis, plan_station_m = str(station[0]), float(station[1])
        angle = start_angle + direction * plan_station_m / radius_m
        centerline_x = center_x + radius_m * math.cos(angle)
        centerline_y = center_y + radius_m * math.sin(angle)
        geometric_station_m = plan_station_m

        if exact_end and index == len(stations) - 1:
            end_lat, end_lon = (float(value) for value in axes["end"])
            centerline_x, centerline_y = local_xy_m(
                end_lat, end_lon, origin_lat, origin_lon
            )
            angle = math.atan2(centerline_y - center_y, centerline_x - center_x)
            geometric_station_m = geometry["geometric_length_m"]

        radial_x = math.cos(angle)
        radial_y = math.sin(angle)
        first_lat, first_lon = local_lat_lon(
            centerline_x - half_width_m * radial_x,
            centerline_y - half_width_m * radial_y,
            origin_lat,
            origin_lon,
        )
        second_lat, second_lon = local_lat_lon(
            centerline_x + half_width_m * radial_x,
            centerline_y + half_width_m * radial_y,
            origin_lat,
            origin_lon,
        )
        label_lat, label_lon = local_lat_lon(
            centerline_x + (half_width_m + 4.0) * radial_x,
            centerline_y + (half_width_m + 4.0) * radial_y,
            origin_lat,
            origin_lon,
        )
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "axis": axis,
                    "station_m": plan_station_m,
                    "geometric_station_m": geometric_station_m,
                    "bridge_width_m": half_width_m * 2.0,
                    "label_coordinates": [label_lon, label_lat],
                    "source": "plan-stations-circular-arc-r250",
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [first_lon, first_lat],
                        [second_lon, second_lat],
                    ],
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}, geometry


def load_project_axes(
    project: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any] | None]:
    axes = project.get("axes", {})
    mode = axes.get("mode")

    if mode == "geojson":
        configured_path = Path(str(axes.get("path", AXES_GEOJSON_PATH)))
        if not configured_path.is_absolute():
            configured_path = PROJECTS_CONFIG_PATH.parent / configured_path
        with configured_path.open("r", encoding="utf-8") as source_file:
            return validate_axes_geojson(json.load(source_file)), "geojson", None

    if mode == "circular_arc":
        geojson, geometry = generated_circular_axes(axes)
        return geojson, "Kreisbogen R=250 m", geometry

    raise ValueError(f"Unbekannter Achsmodus: {mode}")


INDEX_HTML = r"""
<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
:root {
    --border: #c7c7c7;
    --panel: rgba(255, 255, 255, 0.96);
    --blue: #1565c0;
    --green: #2e7d32;
}

html, body {
    height: 100%;
    margin: 0;
    font-family: Arial, sans-serif;
    background: #eee;
}

#app {
    height: 100%;
    display: grid;
    grid-template-rows: auto 1fr auto;
}

#toolbar {
    display: flex;
    flex-wrap: wrap;
    align-items: end;
    gap: 10px;
    padding: 9px 12px;
    background: white;
    border-bottom: 1px solid var(--border);
    z-index: 1000;
}

.field {
    display: flex;
    flex-direction: column;
    gap: 4px;
}

.field label {
    font-size: 12px;
    font-weight: bold;
}

input, select, button {
    height: 34px;
    box-sizing: border-box;
    border: 1px solid #aaa;
    border-radius: 4px;
    padding: 5px 8px;
    background: white;
}

button {
    cursor: pointer;
    font-weight: bold;
}

button:hover {
    background: #f0f0f0;
}

#map {
    width: 100%;
    height: 100%;
}

#status {
    min-width: 240px;
    font-size: 12px;
    color: #555;
    align-self: center;
}

#status.error {
    color: #b00020;
    font-weight: bold;
}

#project-field {
    margin-left: auto;
    min-width: 285px;
}

#project-select {
    width: 100%;
}

#timeline {
    display: grid;
    grid-template-columns: auto 1fr auto auto;
    align-items: center;
    gap: 10px;
    padding: 7px 12px;
    background: white;
    border-top: 1px solid var(--border);
}

#time-slider {
    width: 100%;
}

#legend {
    position: fixed;
    right: 26px;
    bottom: 55px;
    z-index: 9999;
    width: 330px;
    max-width: calc(100vw - 52px);
    padding: 10px 12px;
    background: var(--panel);
    border: 2px solid #777;
    border-radius: 7px;
    box-sizing: border-box;
    font-size: 14px;
    line-height: 1.35;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.15);
}

.vehicle-title {
    margin-top: 9px;
    font-weight: bold;
}

.vehicle-data {
    margin-left: 22px;
    margin-top: 3px;
}

.ref-dist {
    font-size: 1.25em;
    font-weight: bold;
    margin-top: 6px;
    color: #111;
}

.ref-extra {
    font-size: 0.86em;
    font-weight: 600;
    color: #555;
}

.ref-tools {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 5px;
    margin-top: 8px;
    font-size: 11px;
    color: #666;
}

.ref-tools[hidden] {
    display: none;
}

.mini-button {
    height: 24px;
    padding: 2px 7px;
    font-size: 11px;
    line-height: 1;
}

.icon-button {
    width: 25px;
    padding: 0;
}

.dot {
    font-size: 17px;
    vertical-align: -1px;
}

.blue { color: var(--blue); }
.green { color: var(--green); }
.muted { color: #666; }

.live-indicator {
    display: inline-block;
    width: 8px;
    height: 8px;
    margin-right: 5px;
    border-radius: 50%;
    background: #888;
}

.live-indicator.active {
    background: #27ae60;
    box-shadow: 0 0 0 4px rgba(39, 174, 96, 0.13);
}

.axis-label-icon {
    background: transparent;
    border: 0;
}

.axis-label {
    display: grid;
    place-items: center;
    width: 26px;
    height: 26px;
    color: #8a0b0b;
    background: rgba(255, 255, 255, 0.94);
    border: 2px solid #d32f2f;
    border-radius: 50%;
    box-sizing: border-box;
    font-size: 14px;
    font-weight: 800;
    line-height: 1;
    box-shadow: 0 1px 5px rgba(0, 0, 0, 0.28);
}

#axes-state {
    font-size: 11px;
    color: #666;
}

#axes-state.error {
    color: #b00020;
    font-weight: bold;
}

@media (max-width: 820px) {
    #timeline {
        grid-template-columns: auto 1fr;
    }

    #current-time, #row-info {
        grid-column: 1 / -1;
    }

    #legend {
        right: 10px;
        bottom: 82px;
        width: 285px;
    }

    #project-field {
        order: -1;
        width: 100%;
        margin-left: 0;
    }
}
</style>
</head>
<body>
<div id="app">
    <div id="toolbar">
        <div class="field">
            <label for="mode">Modus</label>
            <select id="mode">
                <option value="live">Live</option>
                <option value="history">Historie</option>
            </select>
        </div>

        <div class="field" id="live-window-field">
            <label for="live-minutes">Live-Fenster</label>
            <select id="live-minutes">
                <option value="1">1 Minute</option>
                <option value="5" selected>5 Minuten</option>
                <option value="10">10 Minuten</option>
                <option value="30">30 Minuten</option>
                <option value="60">60 Minuten</option>
            </select>
        </div>

        <div class="field history-field" hidden>
            <label for="start-time">Von</label>
            <input id="start-time" type="datetime-local" step="1">
        </div>

        <div class="field history-field" hidden>
            <label for="end-time">Bis</label>
            <input id="end-time" type="datetime-local" step="1">
        </div>

        <button id="load-button">Daten laden</button>

        <label style="display:flex;align-items:center;gap:6px;height:34px">
            <input id="follow-map" type="checkbox" checked>
            Karte folgt
        </label>

        <div class="field">
            <label style="display:flex;align-items:center;gap:6px;height:20px">
                <input id="toggle-axes" type="checkbox" {% if axes_default_visible %}checked{% endif %}>
                <span id="axes-label-text">Achsen A–L</span>
            </label>
            <span id="axes-state">wird geladen…</span>
        </div>

        <div id="status">
            <span id="live-indicator" class="live-indicator"></span>
            <span id="status-text">Initialisierung...</span>
        </div>

        <div class="field" id="project-field">
            <label for="project-select">Brücke / Projekt</label>
            <select id="project-select" aria-label="Brücke oder Projekt auswählen"></select>
        </div>
    </div>

    <div id="map"></div>

    <div id="timeline">
        <button id="play-button">▶</button>
        <input id="time-slider" type="range" min="0" max="0" value="0">
        <div id="current-time">–</div>
        <div id="row-info" class="muted">0 Punkte</div>
    </div>
</div>

<div id="legend">
    <b>GNSS-Zeitverlauf</b>
    <div class="muted" id="legend-project-name"></div>

    <div class="vehicle-title">
        <label>
            <input type="checkbox" id="toggle-f1" checked>
            <span class="dot blue">●</span> Fahrzeug 1
        </label>
    </div>
    <div class="vehicle-data">
        Geschwindigkeit (berechnet): <span id="f1-speed-calc">–</span><br>
        <div class="ref-dist">
            Referenzabstand: <span id="f1-dist">–</span><br>
            <span class="ref-extra"><span id="extra-label-f1">Extra</span>: <span id="f1-dist-extra">–</span></span>
        </div>
    </div>

    <div class="vehicle-title">
        <label>
            <input type="checkbox" id="toggle-f2" checked>
            <span class="dot green">●</span> Fahrzeug 2
        </label>
    </div>
    <div class="vehicle-data">
        Geschwindigkeit (berechnet): <span id="f2-speed-calc">–</span><br>
        <div class="ref-dist">
            Referenzabstand: <span id="f2-dist">–</span><br>
            <span class="ref-extra"><span id="extra-label-f2">Extra</span>: <span id="f2-dist-extra">–</span></span>
        </div>
    </div>

    <div class="ref-tools" id="ref-tools" title="Extra-Referenz: 51.04471185, 13.727260815">
        <span id="extra-ref-label">Ref2 fix</span>
        <button id="set-ref-f1" class="mini-button" type="button" title="Aktuelle angezeigte Position von Fahrzeug 1 als Extra-Referenz speichern">Ref2 ← F1</button>
        <button id="set-ref-f2" class="mini-button" type="button" title="Aktuelle angezeigte Position von Fahrzeug 2 als Extra-Referenz speichern">Ref2 ← F2</button>
        <button id="reset-ref" class="mini-button icon-button" type="button" title="Extra-Referenz auf 51.04471185, 13.727260815 zurücksetzen">↺</button>
    </div>

    <hr style="border: 0; border-top: 1px solid #ccc; margin: 15px 0 10px 0;">
    <div style="font-size: 1.15em;">
        <b>Abstand (F1 ↔ F2):</b> <span id="vehicles-dist">–</span>
    </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
"use strict";

const LIVE_REFRESH_MS = {{ live_refresh_ms }};
const ANIMATION_MS = Math.max(250, Math.min(900, LIVE_REFRESH_MS * 0.85));
const DEFAULT_EXTRA_REF = Object.freeze({ lat: 51.04471185, lon: 13.727260815 });
const EXTRA_REF_STORAGE_KEY = "gnss-extra-reference-v1";
const PROJECT_STORAGE_KEY = "gnss-bridge-project-v1";
const EARTH_RADIUS_M = 6371008.8;

const map = L.map("map", { preferCanvas: true }).setView([51.0362, 13.7247], 18);

L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors"
}).addTo(map);

map.createPane("axes-lines");
map.getPane("axes-lines").style.zIndex = "430";
map.createPane("axes-labels");
map.getPane("axes-labels").style.zIndex = "650";

const axesLayer = L.layerGroup();
let axesBounds = null;

const modeEl = document.getElementById("mode");
const liveMinutesEl = document.getElementById("live-minutes");
const startTimeEl = document.getElementById("start-time");
const endTimeEl = document.getElementById("end-time");
const loadButton = document.getElementById("load-button");
const followMapEl = document.getElementById("follow-map");
const slider = document.getElementById("time-slider");
const playButton = document.getElementById("play-button");
const currentTimeEl = document.getElementById("current-time");
const rowInfoEl = document.getElementById("row-info");
const statusTextEl = document.getElementById("status-text");
const statusEl = document.getElementById("status");
const liveIndicatorEl = document.getElementById("live-indicator");
const toggleF1 = document.getElementById("toggle-f1");
const toggleF2 = document.getElementById("toggle-f2");
const toggleAxes = document.getElementById("toggle-axes");
const axesStateEl = document.getElementById("axes-state");
const vehiclesDistEl = document.getElementById("vehicles-dist");
const extraRefLabelEl = document.getElementById("extra-ref-label");
const setRefF1Button = document.getElementById("set-ref-f1");
const setRefF2Button = document.getElementById("set-ref-f2");
const resetRefButton = document.getElementById("reset-ref");
const refToolsEl = document.getElementById("ref-tools");
const projectSelectEl = document.getElementById("project-select");
const axesLabelTextEl = document.getElementById("axes-label-text");
const legendProjectNameEl = document.getElementById("legend-project-name");

const COLORS = {
    Fahrzeug1: "#1565c0",
    Fahrzeug2: "#2e7d32"
};

const state = {
    projects: [],
    activeProject: null,
    axisGeometry: null,
    records: [],
    byServer: {
        Fahrzeug1: [],
        Fahrzeug2: []
    },
    times: [],
    lastServerTime: {
        Fahrzeug1: null,
        Fahrzeug2: null
    },
    lastReceivedIso: null,
    loading: false,
    liveTimer: null,
    playing: false,
    playTimer: null,
    firstFit: true,
    liveAtLatest: true,
    currentPoints: { Fahrzeug1: null, Fahrzeug2: null },
    extraReference: loadExtraReference(),
    layers: {
        Fahrzeug1: createVehicleLayers("Fahrzeug1"),
        Fahrzeug2: createVehicleLayers("Fahrzeug2")
    }
};

function createVehicleLayers(server) {
    const color = COLORS[server];

    return {
        line: L.polyline([], {
            color,
            weight: 4,
            opacity: 0.82,
            lineJoin: "round"
        }).addTo(map),
        marker: L.circleMarker([0, 0], {
            color,
            fillColor: color,
            fillOpacity: 0.96,
            radius: 7,
            weight: 2
        }),
        animationFrame: null
    };
}

function escapeHtml(value) {
    return String(value).replace(/[&<>'"]/g, character => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "'": "&#39;",
        '"': "&quot;"
    })[character]);
}

function axisLabelIcon(axis) {
    const safeAxis = escapeHtml(axis);
    return L.divIcon({
        className: "axis-label-icon",
        html: `<span class="axis-label">${safeAxis}</span>`,
        iconSize: [26, 26],
        iconAnchor: [13, 13]
    });
}

function updateAxesVisibility() {
    if (toggleAxes.checked) {
        if (!map.hasLayer(axesLayer)) {
            axesLayer.addTo(map);
        }
    } else if (map.hasLayer(axesLayer)) {
        map.removeLayer(axesLayer);
    }
}

function axisLabelPosition(latLngs, properties) {
    if (!latLngs.length) {
        return null;
    }

    const configured = properties.label_coordinates;

    if (
        Array.isArray(configured) &&
        configured.length >= 2 &&
        Number.isFinite(Number(configured[0])) &&
        Number.isFinite(Number(configured[1]))
    ) {
        return L.latLng(Number(configured[1]), Number(configured[0]));
    }

    const first = latLngs[0];
    const last = latLngs[latLngs.length - 1];
    return L.latLng(
        (first.lat + last.lat) / 2,
        (first.lng + last.lng) / 2
    );
}

async function loadAxes(forceFit = false) {
    axesStateEl.textContent = "wird geladen…";
    axesStateEl.classList.remove("error");

    try {
        const projectId = encodeURIComponent(state.activeProject?.id || "");
        const body = await fetchJson(`/api/axes?project=${projectId}`);
        const features = body.features || [];
        state.axisGeometry = body.arc_geometry || null;
        axesLayer.clearLayers();

        if (!features.length) {
            axesStateEl.textContent = "Georeferenzierung fehlt";
            axesStateEl.classList.add("error");
            toggleAxes.checked = false;
            updateAxesVisibility();
            return;
        }

        const collectedLatLngs = [];

        for (const feature of features) {
            const properties = feature.properties || {};
            const axis = String(properties.axis || properties.name || "?");
            const safeAxis = escapeHtml(axis);
            const zug = String(properties.zug || "");
            const safeZug = escapeHtml(zug);
            const coordinates = feature.geometry?.coordinates || [];
            const latLngs = coordinates.map(coordinate => [coordinate[1], coordinate[0]]);

            if (latLngs.length < 2) {
                continue;
            }

            collectedLatLngs.push(...latLngs);
            const stationText = Number.isFinite(Number(properties.station_m))
                ? `<br>Station: ${Number(properties.station_m).toFixed(1)} m`
                : "";
            const zugText = safeZug ? `<br>${safeZug}` : "";
            const popup = `<b>Achse ${safeAxis}</b>${zugText}${stationText}`;

            L.polyline(latLngs, {
                pane: "axes-lines",
                color: "#d32f2f",
                weight: 3,
                opacity: 0.9,
                dashArray: "8 6",
                interactive: true
            }).bindPopup(popup).addTo(axesLayer);

            const labelPosition = axisLabelPosition(
                latLngs.map(value => L.latLng(value)),
                properties
            );

            if (labelPosition) {
                L.marker(labelPosition, {
                    pane: "axes-labels",
                    icon: axisLabelIcon(axis),
                    interactive: true,
                    keyboard: true,
                    title: zug ? `${zug} · Achse ${axis}` : `Achse ${axis}`
                }).bindPopup(popup).addTo(axesLayer);
            }
        }

        axesBounds = L.latLngBounds(collectedLatLngs);
        axesStateEl.textContent = `${features.length} Achsen · ${body.source}`;
        updateAxesVisibility();

        if ((forceFit || !state.records.length) && axesBounds.isValid()) {
            map.fitBounds(axesBounds.pad(0.22), { maxZoom: 19 });
        }
    } catch (error) {
        console.error(error);
        axesStateEl.textContent = "Achsen konnten nicht geladen werden";
        axesStateEl.classList.add("error");
        toggleAxes.checked = false;
        updateAxesVisibility();
    }
}

function normalizeServer(server) {
    const text = String(server ?? "");

    if (text === "Fahrzeug1" || text.includes("GNSS1")) {
        return "Fahrzeug1";
    }

    if (text === "Fahrzeug2" || text.includes("GNSS2")) {
        return "Fahrzeug2";
    }

    return text;
}

function toLocalInputValue(date) {
    const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    return local.toISOString().slice(0, 19);
}

function setDefaultHistoryRange() {
    const end = new Date();
    const start = new Date(end.getTime() - 30 * 60 * 1000);

    startTimeEl.value = toLocalInputValue(start);
    endTimeEl.value = toLocalInputValue(end);
}

function formatNumber(value, digits = 2, unit = "") {
    const numberValue = Number(value);

    if (!Number.isFinite(numberValue)) {
        return "–";
    }

    const text = numberValue.toFixed(digits);
    return unit ? `${text} ${unit}` : text;
}

function formatDistance(value) {
    const numberValue = Number(value);

    if (!Number.isFinite(numberValue)) {
        return "–";
    }

    if (Math.abs(numberValue) >= 10000) {
        return `${(numberValue / 1000).toFixed(2)} km`;
    }

    return `${numberValue.toFixed(2)} m`;
}

function formatTime(iso) {
    if (!iso) {
        return "–";
    }

    const date = new Date(iso);

    if (Number.isNaN(date.getTime())) {
        return String(iso);
    }

    return new Intl.DateTimeFormat("de-DE", {
        dateStyle: "medium",
        timeStyle: "medium"
    }).format(date);
}

function loadExtraReference() {
    try {
        const saved = JSON.parse(localStorage.getItem(EXTRA_REF_STORAGE_KEY));
        const lat = Number(saved?.lat);
        const lon = Number(saved?.lon);

        if (Number.isFinite(lat) && Number.isFinite(lon)) {
            const source = saved?.source === "F2" ? "F2" : "F1";
            return { lat, lon, custom: true, source };
        }
    } catch (error) {
        console.warn("Extra-Referenz konnte nicht geladen werden", error);
    }

    return { ...DEFAULT_EXTRA_REF, custom: false, source: "fixed" };
}

function currentReferenceStorageKey() {
    return state.activeProject?.id === "bestand_a_l"
        ? EXTRA_REF_STORAGE_KEY
        : `${EXTRA_REF_STORAGE_KEY}-${state.activeProject?.id || "default"}`;
}

function projectDefaultReference() {
    const configured = state.activeProject?.calculations?.extra_reference;

    if (Array.isArray(configured) && configured.length >= 2) {
        return { lat: Number(configured[0]), lon: Number(configured[1]) };
    }

    return { ...DEFAULT_EXTRA_REF };
}

function loadProjectReference() {
    const fallback = projectDefaultReference();

    if (!state.activeProject?.calculations?.extra_reference_editable) {
        return { ...fallback, custom: false, source: "Achse 0" };
    }

    try {
        const saved = JSON.parse(localStorage.getItem(currentReferenceStorageKey()));
        const lat = Number(saved?.lat);
        const lon = Number(saved?.lon);

        if (Number.isFinite(lat) && Number.isFinite(lon)) {
            return { lat, lon, custom: true, source: saved?.source || "custom" };
        }
    } catch (error) {
        console.warn("Projekt-Referenz konnte nicht geladen werden", error);
    }

    return { ...fallback, custom: false, source: "fixed" };
}

function updateExtraReferenceLabel() {
    const ref = state.extraReference;
    extraRefLabelEl.textContent = ref.custom ? `Ref2 ${ref.source || "custom"}` : "Ref2 fix";
    extraRefLabelEl.title = `${ref.lat.toFixed(8)}, ${ref.lon.toFixed(9)}`;
}

function setExtraReference(lat, lon, custom = true, source = "custom") {
    const nextLat = Number(lat);
    const nextLon = Number(lon);

    if (!Number.isFinite(nextLat) || !Number.isFinite(nextLon)) {
        return;
    }

    state.extraReference = { lat: nextLat, lon: nextLon, custom, source };

    if (custom) {
        localStorage.setItem(
            currentReferenceStorageKey(),
            JSON.stringify({ lat: nextLat, lon: nextLon, source })
        );
    } else {
        localStorage.removeItem(currentReferenceStorageKey());
    }

    updateExtraReferenceLabel();

    if (state.times.length) {
        renderAtIndex(Number(slider.value), false);
    }
}

function extraReferenceDistanceM(point) {
    if (!point) {
        return NaN;
    }

    if (
        state.activeProject?.calculations?.extra === "arc_station_nearby" &&
        state.axisGeometry
    ) {
        const directDistanceM = haversineM(
            point.lat, point.lon,
            state.extraReference.lat, state.extraReference.lon
        );
        const maxArcDistanceM = Number(
            state.activeProject.calculations.arc_max_reference_distance_m ?? 2000
        );

        if (directDistanceM <= maxArcDistanceM) {
            return arcStationDistanceM(point, state.axisGeometry);
        }

        return directDistanceM;
    }

    return haversineM(
        point.lat, point.lon,
        state.extraReference.lat, state.extraReference.lon
    );
}

function arcStationDistanceM(point, geometry) {
    const [originLat, originLon] = geometry.origin;
    const [centerX, centerY] = geometry.center_xy_m;
    const referenceLatRad = originLat * Math.PI / 180;
    const x = (point.lon - originLon) * Math.PI / 180 *
        EARTH_RADIUS_M * Math.cos(referenceLatRad);
    const y = (point.lat - originLat) * Math.PI / 180 * EARTH_RADIUS_M;
    const pointAngle = Math.atan2(y - centerY, x - centerX);
    let delta = pointAngle - Number(geometry.start_angle_rad);
    delta = Math.atan2(Math.sin(delta), Math.cos(delta));
    return delta * Number(geometry.direction) * Number(geometry.radius_m);
}

function updateProjectUi() {
    const project = state.activeProject;
    if (!project) return;

    const calculations = project.calculations || {};
    const extraLabel = calculations.extra_label || "Extra";
    axesLabelTextEl.textContent = project.axes_label || "Achsen";
    legendProjectNameEl.textContent = project.name;
    document.getElementById("extra-label-f1").textContent = extraLabel;
    document.getElementById("extra-label-f2").textContent = extraLabel;
    refToolsEl.hidden = !calculations.extra_reference_editable;
    state.extraReference = loadProjectReference();
    updateExtraReferenceLabel();
}

async function loadProjects() {
    const body = await fetchJson("/api/projects");
    state.projects = body.projects || [];
    const savedId = localStorage.getItem(PROJECT_STORAGE_KEY);
    const selected = state.projects.find(project => project.id === savedId) ||
        state.projects.find(project => project.id === body.default_project) ||
        state.projects[0];

    if (!selected) {
        throw new Error("Keine Brückenprojekte konfiguriert.");
    }

    projectSelectEl.replaceChildren(...state.projects.map(project => {
        const option = document.createElement("option");
        option.value = project.id;
        option.textContent = project.name;
        return option;
    }));
    state.activeProject = selected;
    projectSelectEl.value = selected.id;
    updateProjectUi();
}

function haversineM(lat1, lon1, lat2, lon2) {
    const radius = 6371000;
    const toRad = value => value * Math.PI / 180;
    const p1 = toRad(lat1);
    const p2 = toRad(lat2);
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);

    const a = Math.sin(dLat / 2) ** 2 +
        Math.cos(p1) * Math.cos(p2) * Math.sin(dLon / 2) ** 2;

    return 2 * radius * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function setStatus(text, isError = false) {
    statusTextEl.textContent = text;
    statusEl.classList.toggle("error", isError);
    liveIndicatorEl.classList.toggle("active", !isError && modeEl.value === "live");
}

function clearData() {
    stopPlayback();

    state.records = [];
    state.byServer = { Fahrzeug1: [], Fahrzeug2: [] };
    state.times = [];
    state.lastServerTime = { Fahrzeug1: null, Fahrzeug2: null };
    state.lastReceivedIso = null;
    state.liveAtLatest = true;
    state.currentPoints = { Fahrzeug1: null, Fahrzeug2: null };

    slider.min = "0";
    slider.max = "0";
    slider.value = "0";

    for (const server of ["Fahrzeug1", "Fahrzeug2"]) {
        state.layers[server].line.setLatLngs([]);

        if (map.hasLayer(state.layers[server].marker)) {
            map.removeLayer(state.layers[server].marker);
        }

        resetLegend(server);
    }

    currentTimeEl.textContent = "–";
    rowInfoEl.textContent = "0 Punkte";
    vehiclesDistEl.textContent = "–";
}

function calculateDerivedPoint(raw, previous) {
    const point = {
        ...raw,
        server: normalizeServer(raw.server),
        lat: Number(raw.lat),
        lon: Number(raw.lon),
        time_ms: new Date(raw.timestamp).getTime(),
        calc_speed_kmh: 0,
        distance_from_prev_m: 0
    };

    if (
        previous &&
        Number.isFinite(previous.lat) &&
        Number.isFinite(previous.lon) &&
        Number.isFinite(previous.time_ms) &&
        point.time_ms > previous.time_ms
    ) {
        point.distance_from_prev_m = haversineM(
            previous.lat,
            previous.lon,
            point.lat,
            point.lon
        );

        point.calc_speed_kmh =
            point.distance_from_prev_m /
            ((point.time_ms - previous.time_ms) / 1000) * 3.6;
    }

    return point;
}

function appendRecords(rawRecords, replace = false) {
    if (replace) {
        clearData();
    }

    const sorted = [...rawRecords].sort(
        (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
    );

    let added = 0;

    for (const raw of sorted) {
        const server = normalizeServer(raw.server);

        if (!(server in state.byServer)) {
            continue;
        }

        const lat = Number(raw.lat);
        const lon = Number(raw.lon);
        const timeMs = new Date(raw.timestamp).getTime();

        if (!Number.isFinite(lat) || !Number.isFinite(lon) || !Number.isFinite(timeMs)) {
            continue;
        }

        const serverRecords = state.byServer[server];
        const previous = serverRecords.length
            ? serverRecords[serverRecords.length - 1]
            : null;

        if (previous && timeMs <= previous.time_ms) {
            continue;
        }

        const point = calculateDerivedPoint(raw, previous);
        serverRecords.push(point);
        state.records.push(point);
        state.lastServerTime[server] = point.timestamp;
        added += 1;
    }

    state.records.sort((a, b) => a.time_ms - b.time_ms);
    state.times = [...new Set(state.records.map(row => row.time_ms))].sort((a, b) => a - b);

    slider.min = "0";
    slider.max = String(Math.max(0, state.times.length - 1));

    return added;
}

function trimLiveWindow() {
    if (modeEl.value !== "live" || !state.times.length) {
        return;
    }

    const windowMs = Number(liveMinutesEl.value) * 60 * 1000;
    const newestMs = state.times[state.times.length - 1];
    const cutoff = newestMs - windowMs;

    for (const server of ["Fahrzeug1", "Fahrzeug2"]) {
        state.byServer[server] = state.byServer[server].filter(
            point => point.time_ms >= cutoff
        );
    }

    state.records = state.records.filter(point => point.time_ms >= cutoff);
    state.times = [...new Set(state.records.map(row => row.time_ms))].sort((a, b) => a - b);

    slider.max = String(Math.max(0, state.times.length - 1));
}

function buildInitialUrl() {
    const params = new URLSearchParams();

    if (modeEl.value === "live") {
        params.set("mode", "live");
        params.set("minutes", liveMinutesEl.value);
    } else {
        if (!startTimeEl.value || !endTimeEl.value) {
            throw new Error("Bitte Von- und Bis-Zeit angeben.");
        }

        const start = new Date(startTimeEl.value);
        const end = new Date(endTimeEl.value);

        if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
            throw new Error("Ungültiger Zeitraum.");
        }

        params.set("mode", "history");
        params.set("start", start.toISOString());
        params.set("end", end.toISOString());
    }

    return `/api/gnss?${params.toString()}`;
}

function buildIncrementalUrl() {
    const params = new URLSearchParams();
    params.set("mode", "live");
    params.set("minutes", liveMinutesEl.value);

    if (state.lastReceivedIso) {
        params.set("since", state.lastReceivedIso);
    }

    return `/api/gnss?${params.toString()}`;
}

async function fetchJson(url) {
    const response = await fetch(url, { cache: "no-store" });
    const body = await response.json();

    if (!response.ok) {
        throw new Error(body.error || `HTTP ${response.status}`);
    }

    return body;
}

async function loadInitialData() {
    if (state.loading) {
        return;
    }

    state.loading = true;
    setStatus("Daten werden geladen...");

    try {
        const body = await fetchJson(buildInitialUrl());
        appendRecords(body.records || [], true);
        state.lastReceivedIso = body.last_received_at || null;
        trimLiveWindow();

        if (state.times.length) {
            renderAtIndex(state.times.length - 1, false);
            fitMapToData();
        }

        rowInfoEl.textContent =
            `${state.records.length.toLocaleString("de-DE")} Punkte · Raster ${body.bucket_ms} ms`;

        setStatus(
            state.records.length
                ? `Aktualisiert: ${new Date().toLocaleTimeString("de-DE")}`
                : "Keine Daten im gewählten Zeitraum."
        );
    } catch (error) {
        console.error(error);
        setStatus(error.message || String(error), true);
    } finally {
        state.loading = false;
    }
}

async function fetchLiveIncrement() {
    if (state.loading || modeEl.value !== "live") {
        return;
    }

    state.loading = true;

    try {
        const body = await fetchJson(buildIncrementalUrl());
        const added = appendRecords(body.records || [], false);

        if (body.last_received_at) {
            state.lastReceivedIso = body.last_received_at;
        }

        if (added > 0) {
            trimLiveWindow();

            if (state.liveAtLatest && state.times.length) {
                renderAtIndex(state.times.length - 1, true);
            }

            rowInfoEl.textContent =
                `${state.records.length.toLocaleString("de-DE")} Punkte · +${added}`;
        }

        setStatus(`Live: ${new Date().toLocaleTimeString("de-DE")}`);
    } catch (error) {
        console.error(error);
        setStatus(error.message || String(error), true);
    } finally {
        state.loading = false;
    }
}

function fitMapToData() {
    const points = state.records.map(point => [point.lat, point.lon]);

    if (!points.length) {
        return;
    }

    const bounds = L.latLngBounds(points);

    if (bounds.isValid()) {
        map.fitBounds(bounds.pad(0.12), { maxZoom: 19 });
    }
}

function upperBound(records, targetMs) {
    let low = 0;
    let high = records.length;

    while (low < high) {
        const mid = Math.floor((low + high) / 2);

        if (records[mid].time_ms <= targetMs) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    return low;
}

function animateMarker(server, targetLatLng, popupHtml) {
    const layer = state.layers[server];
    const marker = layer.marker;
    const target = L.latLng(targetLatLng);

    if (layer.animationFrame) {
        cancelAnimationFrame(layer.animationFrame);
        layer.animationFrame = null;
    }

    if (!map.hasLayer(marker) || marker.getLatLng().lat === 0) {
        marker.setLatLng(target).bindPopup(popupHtml);
        marker.addTo(map);
        return;
    }

    const start = marker.getLatLng();
    const startTime = performance.now();

    marker.bindPopup(popupHtml);

    function step(now) {
        const progress = Math.min(1, (now - startTime) / ANIMATION_MS);
        const eased = 1 - Math.pow(1 - progress, 3);

        marker.setLatLng([
            start.lat + (target.lat - start.lat) * eased,
            start.lng + (target.lng - start.lng) * eased
        ]);

        if (progress < 1) {
            layer.animationFrame = requestAnimationFrame(step);
        } else {
            layer.animationFrame = null;
        }
    }

    layer.animationFrame = requestAnimationFrame(step);
}

function popupFor(point) {
    return `
        <b>${point.server}</b><br>
        Zeit: ${formatTime(point.timestamp)}<br>
        Geschwindigkeit: ${formatNumber(point.speed, 2)} /
        berechnet ${formatNumber(point.calc_speed_kmh, 2, "km/h")}<br>
        Referenzabstand: ${formatNumber(point.dist_ref, 2, "m")}<br>
        Extra-Referenz: ${formatDistance(extraReferenceDistanceM(point))}<br>
        Breite: ${point.lat}<br>
        Länge: ${point.lon}
    `;
}

function renderAtIndex(index, animate) {
    if (!state.times.length) {
        return;
    }

    const safeIndex = Math.max(0, Math.min(Number(index), state.times.length - 1));
    slider.value = String(safeIndex);

    const currentMs = state.times[safeIndex];
    currentTimeEl.textContent = formatTime(new Date(currentMs).toISOString());
    state.liveAtLatest = safeIndex >= state.times.length - 1;

    const latestVisiblePoints = [];
    const latestServerPoints = { Fahrzeug1: null, Fahrzeug2: null };

    for (const server of ["Fahrzeug1", "Fahrzeug2"]) {
        const records = state.byServer[server];
        const endIndex = upperBound(records, currentMs);
        const visibleRecords = records.slice(0, endIndex);
        const last = visibleRecords.length
            ? visibleRecords[visibleRecords.length - 1]
            : null;

        state.layers[server].line.setLatLngs(
            visibleRecords.map(point => [point.lat, point.lon])
        );

        if (last) {
            const enabled = server === "Fahrzeug1" ? toggleF1.checked : toggleF2.checked;

            if (enabled) {
                if (animate) {
                    animateMarker(server, [last.lat, last.lon], popupFor(last));
                } else {
                    state.layers[server].marker
                        .setLatLng([last.lat, last.lon])
                        .bindPopup(popupFor(last));

                    if (!map.hasLayer(state.layers[server].marker)) {
                        state.layers[server].marker.addTo(map);
                    }
                }
            }

            latestServerPoints[server] = last;
            state.currentPoints[server] = last;
            latestVisiblePoints.push(last);
            updateLegend(server, last);
        } else {
            if (map.hasLayer(state.layers[server].marker)) {
                map.removeLayer(state.layers[server].marker);
            }

            state.currentPoints[server] = null;
            resetLegend(server);
        }
    }

    if (latestServerPoints.Fahrzeug1 && latestServerPoints.Fahrzeug2) {
        const d = haversineM(
            latestServerPoints.Fahrzeug1.lat, latestServerPoints.Fahrzeug1.lon,
            latestServerPoints.Fahrzeug2.lat, latestServerPoints.Fahrzeug2.lon
        );
        vehiclesDistEl.textContent = formatNumber(d, 2, "m");
    } else {
        vehiclesDistEl.textContent = "–";
    }

    updateVehicleVisibility();

    if (
        modeEl.value === "live" &&
        followMapEl.checked &&
        state.liveAtLatest &&
        latestVisiblePoints.length
    ) {
        const centerLat = latestVisiblePoints.reduce((sum, p) => sum + p.lat, 0) / latestVisiblePoints.length;
        const centerLon = latestVisiblePoints.reduce((sum, p) => sum + p.lon, 0) / latestVisiblePoints.length;
        map.panTo([centerLat, centerLon], { animate: true, duration: 0.35 });
    }
}

function updateLegend(server, point) {
    const prefix = server === "Fahrzeug1" ? "f1" : "f2";

    document.getElementById(`${prefix}-speed-calc`).textContent =
        formatNumber(point.calc_speed_kmh, 2, "km/h");

    document.getElementById(`${prefix}-dist`).textContent =
        formatNumber(point.dist_ref, 2, "m");

    document.getElementById(`${prefix}-dist-extra`).textContent =
        formatDistance(extraReferenceDistanceM(point));
}

function resetLegend(server) {
    const prefix = server === "Fahrzeug1" ? "f1" : "f2";

    for (const suffix of ["speed-calc", "dist", "dist-extra"]) {
        document.getElementById(`${prefix}-${suffix}`).textContent = "–";
    }
}

function updateVehicleVisibility() {
    const visibility = {
        Fahrzeug1: toggleF1.checked,
        Fahrzeug2: toggleF2.checked
    };

    for (const server of ["Fahrzeug1", "Fahrzeug2"]) {
        const layer = state.layers[server];

        if (visibility[server]) {
            if (!map.hasLayer(layer.line)) {
                layer.line.addTo(map);
            }

            const markerPos = layer.marker.getLatLng();

            if (markerPos.lat !== 0 && !map.hasLayer(layer.marker)) {
                layer.marker.addTo(map);
            }
        } else {
            if (map.hasLayer(layer.line)) {
                map.removeLayer(layer.line);
            }

            if (map.hasLayer(layer.marker)) {
                map.removeLayer(layer.marker);
            }
        }
    }
}

function stopPlayback() {
    state.playing = false;
    playButton.textContent = "▶";

    if (state.playTimer) {
        clearInterval(state.playTimer);
        state.playTimer = null;
    }
}

function startPlayback() {
    if (state.times.length < 2) {
        return;
    }

    state.playing = true;
    playButton.textContent = "⏸";

    if (Number(slider.value) >= state.times.length - 1) {
        slider.value = "0";
        renderAtIndex(0, false);
    }

    state.playTimer = setInterval(() => {
        const next = Number(slider.value) + 1;

        if (next >= state.times.length) {
            stopPlayback();
            return;
        }

        renderAtIndex(next, false);
    }, 100);
}

function configureMode() {
    const historyMode = modeEl.value === "history";

    document.getElementById("live-window-field").hidden = historyMode;
    document.querySelectorAll(".history-field").forEach(
        element => element.hidden = !historyMode
    );

    stopPlayback();

    if (state.liveTimer) {
        clearInterval(state.liveTimer);
        state.liveTimer = null;
    }

    if (!historyMode) {
        state.liveTimer = setInterval(fetchLiveIncrement, LIVE_REFRESH_MS);
    }

    loadInitialData();
}

modeEl.addEventListener("change", configureMode);
liveMinutesEl.addEventListener("change", loadInitialData);
loadButton.addEventListener("click", loadInitialData);
playButton.addEventListener("click", () => {
    if (state.playing) {
        stopPlayback();
    } else {
        startPlayback();
    }
});

slider.addEventListener("input", () => {
    stopPlayback();
    renderAtIndex(Number(slider.value), false);
});

toggleF1.addEventListener("change", updateVehicleVisibility);
toggleF2.addEventListener("change", updateVehicleVisibility);
toggleAxes.addEventListener("change", updateAxesVisibility);

setRefF1Button.addEventListener("click", () => {
    const point = state.currentPoints.Fahrzeug1;

    if (!point) {
        setStatus("Keine aktuelle Position von Fahrzeug 1 verfügbar.", true);
        return;
    }

    setExtraReference(point.lat, point.lon, true, "F1");
    setStatus("Extra-Referenz auf aktuelle Position von Fahrzeug 1 gesetzt.");
});

setRefF2Button.addEventListener("click", () => {
    const point = state.currentPoints.Fahrzeug2;

    if (!point) {
        setStatus("Keine aktuelle Position von Fahrzeug 2 verfügbar.", true);
        return;
    }

    setExtraReference(point.lat, point.lon, true, "F2");
    setStatus("Extra-Referenz auf aktuelle Position von Fahrzeug 2 gesetzt.");
});

resetRefButton.addEventListener("click", () => {
    const reference = projectDefaultReference();
    setExtraReference(reference.lat, reference.lon, false, "fixed");
    setStatus("Extra-Referenz auf feste Koordinaten zurückgesetzt.");
});

projectSelectEl.addEventListener("change", async () => {
    const selected = state.projects.find(project => project.id === projectSelectEl.value);
    if (!selected) return;

    state.activeProject = selected;
    localStorage.setItem(PROJECT_STORAGE_KEY, selected.id);
    updateProjectUi();
    await loadAxes(true);

    if (state.times.length) {
        renderAtIndex(Number(slider.value), false);
    }
});

map.on("dragstart zoomstart", () => {
    if (modeEl.value === "live") {
        followMapEl.checked = false;
    }
});

async function initialize() {
    try {
        setDefaultHistoryRange();
        await loadProjects();
        await loadAxes();
        configureMode();
    } catch (error) {
        console.error(error);
        setStatus(error.message || "Initialisierung fehlgeschlagen.", true);
    }
}

initialize();
</script>
</body>
</html>
"""


def dsn() -> str:
    if not DB_PASSWORD:
        raise RuntimeError("GNSS_DB_PASSWORD ist nicht gesetzt.")

    return (
        f"host={DB_HOST} "
        f"port={DB_PORT} "
        f"dbname={DB_NAME} "
        f"user={DB_USER} "
        f"password={DB_PASSWORD} "
        f"connect_timeout={DB_CONNECT_TIMEOUT}"
    )


def parse_iso(value: str) -> datetime:
    if not value:
        raise ValueError("Zeitangabe fehlt.")

    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def json_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (int, float, str, bool)):
        return value

    return str(value)


def calculate_bucket_ms(start_time: datetime, end_time: datetime) -> int:
    duration_ms = max(
        1,
        int((end_time - start_time).total_seconds() * 1000),
    )

    return max(
        1,
        math.ceil(duration_ms / MAX_POINTS_PER_VEHICLE),
    )


def serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    for row in rows:
        result.append(
            {
                "timestamp": json_value(row["timestamp"]),
                "server": json_value(row["server"]),
                "east": json_value(row["east"]),
                "north": json_value(row["north"]),
                "elev": json_value(row["elev"]),
                "lat": json_value(row["lat"]),
                "lon": json_value(row["lon"]),
                "speed": json_value(row["speed"]),
                "dist_ref": json_value(row["dist_ref"]),
                "status": json_value(row["status"]),
                "msg": json_value(row["msg"]),
                "topic": json_value(row["topic"]),
                "received_at": json_value(row["received_at"]),
            }
        )

    return result


def query_initial(
    start_time: datetime,
    end_time: datetime,
) -> tuple[list[dict[str, Any]], int]:
    bucket_ms = calculate_bucket_ms(start_time, end_time)

    sql = f"""
    WITH ranked AS (
        SELECT
            "timestamp",
            server,
            east,
            north,
            elev,
            lat,
            lon,
            speed,
            dist_ref,
            status,
            msg,
            topic,
            received_at,
            ROW_NUMBER() OVER (
                PARTITION BY
                    server,
                    FLOOR(
                        EXTRACT(EPOCH FROM "timestamp") * 1000
                        / %(bucket_ms)s
                    )
                ORDER BY "timestamp" ASC
            ) AS rn
        FROM {TABLE}
        WHERE "timestamp" >= %(start_time)s
          AND "timestamp" <= %(end_time)s
          AND lat IS NOT NULL
          AND lon IS NOT NULL
    )
    SELECT
        "timestamp",
        server,
        east,
        north,
        elev,
        lat,
        lon,
        speed,
        dist_ref,
        status,
        msg,
        topic,
        received_at
    FROM ranked
    WHERE rn = 1
    ORDER BY "timestamp" ASC, server ASC;
    """

    with psycopg.connect(dsn(), row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "bucket_ms": bucket_ms,
                },
            )
            rows = cursor.fetchall()

    return rows, bucket_ms


def query_incremental(
    since_received_at: datetime,
    window_start: datetime,
) -> list[dict[str, Any]]:
    sql = f"""
    SELECT
        "timestamp",
        server,
        east,
        north,
        elev,
        lat,
        lon,
        speed,
        dist_ref,
        status,
        msg,
        topic,
        received_at
    FROM {TABLE}
    WHERE received_at > %(since_received_at)s
      AND "timestamp" >= %(window_start)s
      AND lat IS NOT NULL
      AND lon IS NOT NULL
    ORDER BY received_at ASC, "timestamp" ASC, server ASC
    LIMIT %(row_limit)s;
    """

    with psycopg.connect(dsn(), row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                {
                    "since_received_at": since_received_at,
                    "window_start": window_start,
                    "row_limit": MAX_INCREMENTAL_ROWS,
                },
            )
            return cursor.fetchall()


@app.get("/")
def index():
    return render_template_string(
        INDEX_HTML,
        title=APP_TITLE,
        live_refresh_ms=LIVE_REFRESH_MS,
        axes_default_visible=AXES_DEFAULT_VISIBLE,
    )


@app.get("/api/axes")
def api_axes():
    try:
        _, project = project_by_id(request.args.get("project"))
        geojson, source, arc_geometry = load_project_axes(project)
        return jsonify(
            **geojson,
            source=source,
            configured=bool(geojson["features"]),
            project_id=project["id"],
            arc_geometry=arc_geometry,
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        log.exception("Achsen-Georeferenzierung konnte nicht geladen werden")
        return jsonify(error=f"Achsen-Konfiguration ungueltig: {exc}"), 500


@app.get("/api/projects")
def api_projects():
    try:
        config = load_projects_config()
        projects = []

        for project in config["projects"]:
            projects.append(
                {
                    "id": project["id"],
                    "name": project["name"],
                    "axes_label": project.get("axes", {}).get("label", "Achsen"),
                    "map": project.get("map", {}),
                    "calculations": project.get("calculations", {}),
                }
            )

        return jsonify(
            default_project=config["default_project"],
            projects=projects,
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        log.exception("Brückenprojekte konnten nicht geladen werden")
        return jsonify(error=f"Projekt-Konfiguration ungueltig: {exc}"), 500


@app.get("/health")
def health():
    try:
        with psycopg.connect(dsn()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()

        return jsonify(
            status="ok",
            database=DB_NAME,
            host=DB_HOST,
        )
    except Exception as exc:
        log.exception("Health check failed")
        return jsonify(status="error", error=str(exc)), 503


@app.get("/api/gnss")
def api_gnss():
    try:
        mode = request.args.get("mode", "live").strip().lower()
        since_raw = request.args.get("since", "").strip()

        if mode == "live":
            minutes = int(
                request.args.get(
                    "minutes",
                    str(DEFAULT_LIVE_MINUTES),
                )
            )

            if minutes < 1 or minutes > 1440:
                raise ValueError(
                    "Live-Fenster muss zwischen 1 und 1440 Minuten liegen."
                )

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(minutes=minutes)

            if since_raw:
                since_received_at = parse_iso(since_raw)
                rows = query_incremental(
                    since_received_at=since_received_at,
                    window_start=start_time,
                )
                bucket_ms = 1
            else:
                rows, bucket_ms = query_initial(start_time, end_time)

        elif mode == "history":
            start_time = parse_iso(request.args.get("start", ""))
            end_time = parse_iso(request.args.get("end", ""))

            if start_time >= end_time:
                raise ValueError(
                    "Die Startzeit muss vor der Endzeit liegen."
                )

            duration_hours = (
                end_time - start_time
            ).total_seconds() / 3600.0

            if duration_hours > MAX_HISTORY_HOURS:
                raise ValueError(
                    f"Maximaler Zeitraum: {MAX_HISTORY_HOURS} Stunden."
                )

            rows, bucket_ms = query_initial(start_time, end_time)

        else:
            raise ValueError("Unbekannter Modus.")

        records = serialize_rows(rows)
        last_received_at = None

        if rows:
            last_received = max(
                row["received_at"]
                for row in rows
                if row["received_at"] is not None
            )
            last_received_at = last_received.isoformat()

        return jsonify(
            mode=mode,
            count=len(records),
            bucket_ms=bucket_ms,
            last_received_at=last_received_at,
            records=records,
        )

    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    except psycopg.Error as exc:
        log.exception("PostgreSQL query failed")
        return jsonify(error=f"PostgreSQL-Fehler: {exc}"), 503
    except Exception as exc:
        log.exception("Unexpected API error")
        return jsonify(error=f"Interner Fehler: {exc}"), 500


if __name__ == "__main__":
    app.run(
        host=APP_HOST,
        port=APP_PORT,
        threaded=True,
        debug=False,
    )
