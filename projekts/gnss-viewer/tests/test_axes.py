import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

# Unit tests for the geometry do not need Flask/PostgreSQL. Lightweight stubs
# keep these tests runnable in an offline build environment.
psycopg_stub = types.ModuleType("psycopg")
psycopg_stub.Error = Exception
psycopg_stub.connect = lambda *args, **kwargs: None
psycopg_rows_stub = types.ModuleType("psycopg.rows")
psycopg_rows_stub.dict_row = object()


class FlaskStub:
    def __init__(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        return lambda function: function


flask_stub = types.ModuleType("flask")
flask_stub.Flask = FlaskStub
flask_stub.jsonify = lambda *args, **kwargs: kwargs or args
flask_stub.render_template_string = lambda *args, **kwargs: ""
flask_stub.request = types.SimpleNamespace(args={})

sys.modules.setdefault("psycopg", psycopg_stub)
sys.modules.setdefault("psycopg.rows", psycopg_rows_stub)
sys.modules.setdefault("flask", flask_stub)

import gnss_postgres_server as server


class AxesTests(unittest.TestCase):
    def setUp(self):
        self.original_reference = (
            server.AXIS_A_LAT,
            server.AXIS_A_LON,
            server.AXIS_L_LAT,
            server.AXIS_L_LON,
        )
        self.original_path = server.AXES_GEOJSON_PATH

    def tearDown(self):
        (
            server.AXIS_A_LAT,
            server.AXIS_A_LON,
            server.AXIS_L_LAT,
            server.AXIS_L_LON,
        ) = self.original_reference
        server.AXES_GEOJSON_PATH = self.original_path

    def test_plan_stations_match_plan(self):
        self.assertEqual(
            [axis for axis, _ in server.PLAN_AXIS_STATIONS_M],
            ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L"],
        )
        self.assertEqual(server.PLAN_AXIS_STATIONS_M[-1][1], 323.5)

    def test_generates_eleven_axes_from_reference_points(self):
        server.AXIS_A_LAT = 51.0
        server.AXIS_A_LON = 13.0
        server.AXIS_L_LAT = 51.002
        server.AXIS_L_LON = 13.003
        result = server.generated_axes_geojson()

        self.assertIsNotNone(result)
        self.assertEqual(len(result["features"]), 11)
        self.assertEqual(result["features"][2]["properties"]["axis"], "C")
        self.assertEqual(result["features"][2]["properties"]["station_m"], 61.5)

    def test_geojson_file_has_priority(self):
        payload = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"axis": "C", "station_m": 61.5},
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [[13.1, 51.1], [13.2, 51.2]],
                    },
                }
            ],
        }

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "axes.geojson"
            path.write_text(json.dumps(payload), encoding="utf-8")
            server.AXES_GEOJSON_PATH = path
            result, source = server.load_axes_geojson()

        self.assertEqual(source, "geojson")
        self.assertEqual(result["features"][0]["properties"]["axis"], "C")


if __name__ == "__main__":
    unittest.main()
