# MultiConnect-Leer

Project-specific MKP MultiConnect firmware for Leer Suedringbruecke.

- Sketch: `MultiConnect-Leer.ino`
- Firmware ID: `1.0.0-Leer`
- AP SSID: `MultiConnect-Leer-<MAC suffix>`
- NVS namespace: `multileer`
- LoRa payload: V5, 12 bytes
- ChirpStack codec: `CHIRPSTACK_CODEC.js`
- LittleFS files: `data/`
- Hardware notes: `PCB_VERSION.md`

The compact V5 uplink contains only battery percentage, battery voltage,
position, Wegsensor raw value, and temperature. INA226 data is not included
in the uplink. Sentinel values represent null measurements; see the payload
comment in `MultiConnect-Leer.ino`.

For the Heltec WiFi LoRa 32 V4 profile use EU868 and
`USE_KCT8103L_PA`. Compile, upload, and upload the `data/` filesystem from
this folder separately in Arduino IDE.
