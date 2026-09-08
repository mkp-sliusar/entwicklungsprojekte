# MultiConnect

Universal MKP MultiConnect firmware base.

- Sketch: `MultiConnect.ino`
- Firmware ID: `1.6.0`
- LoRa payload: V5/V6, selectable fields with optional Modbus values
- SenseCAP ONE: configurable RS-485 Modbus-RTU polling with web diagnostics
- LittleFS files: `data/`
- Hardware notes: `PCB_VERSION.md`

Use this project as the common base for future MultiConnect features. The
project-specific Leer variant is stored next to it in `MultiConnect-Leer/`.

For the Heltec WiFi LoRa 32 V4 profile use EU868 and
`USE_KCT8103L_PA`. Compile, upload, and upload the `data/` filesystem from
this folder separately in Arduino IDE.

The Modbus integration uses `9600 8N1` and function `0x04` by default. The AP
web interface supports one SenseCAP weather station and up to twelve SISGEO
devices on the shared RS-485 bus. A further twelve-slot total limit keeps the
configuration bounded. Select a vendor first and then its model preset; each
slot has its own enable flag, slave address, baud rate, and warm-up time. The
current presets are all SenseCAP ONE models, SISGEO H-LEVEL digital (`0x0202`,
32-bit float), and a Custom profile for `float32`, `uint16`, `int16`, or
`uint32` values using function `03` or `04`.

Devices are read sequentially. The shared sensor rail can use timed power
(the default, with the configured warm-up applied after wake) or an always-on
power policy for installations with permanent external supply. Warm-up is
configurable from 0 to 3600 seconds per device. The default timed behavior
still turns the MOSFET off during field sleep.

The AP starts its web server before the first measurement. Sensor reads are
queued from web handlers, and Modbus warm-up, retry, and response waits service
the AP DNS/HTTP loop in short slices. With automatic redirect disabled, Windows
captive-portal probe URLs return immediately with HTTP 204 and unknown URLs
return HTTP 404; they are not redirected into a loop.

The SenseCAP sensor requires a separate 12-24 V operating supply; its heating
wires are not part of the RS-485 connection. SISGEO wiring must follow the
vendor diagram supplied with the sensor. The existing V5 LoRa payload remains
backward compatible and uses the first valid SenseCAP slot. When SISGEO fields
are selected, the controller uses V6: it carries the base fields plus a device
mask and a field mask/float32 values for each selected SISGEO slot. All
configured slots, including generic profiles, are available with their values
and diagnostics in the AP web interface.

The AP interface contains separate burger-menu pages for live data, connection
settings, LoRa payload field selection, Modbus devices, charts, diagnostics, and
Wegsensor calibration.
The main page shows connected sensor values and the transmission interval. The
SenseCAP measurement grid and SenseCAP payload fields are shown only when an
enabled SenseCAP station is configured. With SISGEO-only configurations the
SISGEO device cards remain visible, while unrelated SenseCAP fields are hidden.
The payload page can select any combination of controller, SenseCAP, and
per-device SISGEO fields, including a header-only status payload if no fields
are selected. V5 contains a four-byte base-field mask followed by selected
values in field-id order. V6 adds a two-byte SISGEO device mask and one field
mask plus float32 values for each selected SISGEO device. The complete base
field set is 88 bytes and fits EU868 DR3 or higher; 242 bytes is the EU868
maximum application payload.

The ChirpStack decoder is [data/chirpstack_decoder.js](data/chirpstack_decoder.js).
It supports V6 SISGEO device fields, V5 optional fields, and remains compatible
with the previous V4 controller payload.

The Node-RED/PostgreSQL mapping is
[node-red/multiconnect_to_postgres.function.js](node-red/multiconnect_to_postgres.function.js).
Run [sql/multiconnect_modbus.sql](sql/multiconnect_modbus.sql) once before
deploying that Function node. The migration also adds the JSONB column for V6
SISGEO device values. The Grafana dashboard can then be imported from
[grafana/multiconnect_sensecap_weather.json](grafana/multiconnect_sensecap_weather.json).
During import, select the PostgreSQL datasource that contains
`public.multiconnect`. The dashboard exposes a `controller_id` selector and
queries the Modbus weather columns added by the migration.

For a combined view of all available sources, import
[grafana/multiconnect_all_sensors.json](grafana/multiconnect_all_sensors.json).
It includes local controller values, SenseCAP weather/wind/rain/solar values,
SISGEO values and diagnostics from `lora_sisgeo_devices`, Modbus availability,
and battery/LoRa link quality. Panels with no corresponding sensor data remain
empty. For direct paste into Grafana's native JSON model use
[grafana/multiconnect_all_sensors_json_model.json](grafana/multiconnect_all_sensors_json_model.json).

For direct paste into Grafana **Dashboard settings -> JSON model**, use
[grafana/multiconnect_sensecap_weather_json_model.json](grafana/multiconnect_sensecap_weather_json_model.json).
This version has no import metadata or datasource placeholder and is already
bound to PostgreSQL datasource UID `afmykralbnj7kf`. Change that UID in the
JSON model if the target Grafana instance uses a different datasource UID.
