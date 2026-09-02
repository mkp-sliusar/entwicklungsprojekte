# MultiConnect

Universal MKP MultiConnect firmware base.

- Sketch: `MultiConnect.ino`
- Firmware ID: `1.4.5`
- LoRa payload: V4, universal format with optional INA226 fields
- LittleFS files: `data/`
- Hardware notes: `PCB_VERSION.md`

Use this project as the common base for future MultiConnect features. The
project-specific Leer variant is stored next to it in `MultiConnect-Leer/`.

For the Heltec WiFi LoRa 32 V4 profile use EU868 and
`USE_KCT8103L_PA`. Compile, upload, and upload the `data/` filesystem from
this folder separately in Arduino IDE.
