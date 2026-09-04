# MultiConnect

Universal MKP MultiConnect firmware base.

- Sketch: `MultiConnect.ino`
- Firmware ID: `1.5.0`
- LoRa payload: V5, selectable fields with optional Modbus values
- SenseCAP ONE: configurable RS-485 Modbus-RTU polling with web diagnostics
- LittleFS files: `data/`
- Hardware notes: `PCB_VERSION.md`

Use this project as the common base for future MultiConnect features. The
project-specific Leer variant is stored next to it in `MultiConnect-Leer/`.

For the Heltec WiFi LoRa 32 V4 profile use EU868 and
`USE_KCT8103L_PA`. Compile, upload, and upload the `data/` filesystem from
this folder separately in Arduino IDE.

The SenseCAP Modbus integration uses `9600 8N1` and function `0x04` by default.
Select the exact SenseCAP model and slave address in the AP web interface before
connecting the sensor. The sensor requires a separate 12-24 V operating supply;
the heating wires are not part of the RS-485 connection.

The AP interface contains separate burger-menu pages for live data, LoRaWAN
settings, LoRa payload field selection, Modbus diagnostics, charts, and expert
diagnostics. The payload page can select any non-empty combination of controller
and SenseCAP fields, including a header-only status payload if no fields are
selected. Its V5 frame contains a four-byte field mask followed by the selected
values in field-id order. The complete field set is 88 bytes and
fits EU868 DR3 or higher; 51-byte selections fit DR0-2 and 242 bytes is the
EU868 maximum application payload.

The ChirpStack decoder is [data/chirpstack_decoder.js](data/chirpstack_decoder.js).
It supports V5 optional fields and remains compatible with the previous V4
controller payload.

The Node-RED/PostgreSQL mapping is
[node-red/multiconnect_to_postgres.function.js](node-red/multiconnect_to_postgres.function.js).
Run [sql/multiconnect_modbus.sql](sql/multiconnect_modbus.sql) once before
deploying that Function node. The Grafana dashboard can then be imported from
[grafana/multiconnect_sensecap_weather.json](grafana/multiconnect_sensecap_weather.json).
During import, select the PostgreSQL datasource that contains
`public.multiconnect`. The dashboard exposes a `controller_id` selector and
queries the Modbus weather columns added by the migration.

For direct paste into Grafana **Dashboard settings -> JSON model**, use
[grafana/multiconnect_sensecap_weather_json_model.json](grafana/multiconnect_sensecap_weather_json_model.json).
This version has no import metadata or datasource placeholder and is already
bound to PostgreSQL datasource UID `afmykralbnj7kf`. Change that UID in the
JSON model if the target Grafana instance uses a different datasource UID.
