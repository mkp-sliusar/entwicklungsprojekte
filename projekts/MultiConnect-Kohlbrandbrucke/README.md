# MultiConnect-Kohlbrandbrucke

Project-specific MKP MultiConnect firmware for Kohlbrandbruecke.

- Release: `v1.0.1`
- Sketch: `MultiConnect-Kohlbrandbrucke.ino`
- Firmware ID: `1.0.1-Kohlbrandbrucke`
- AP SSID: `MC-Kohlbrandbruecke-<MAC suffix>`
- NVS namespace: `mckohlbruecke`
- LoRa payload: V5, 12 bytes
- ChirpStack codec: `CHIRPSTACK_CODEC.js`
- LittleFS files: `data/`
- Hardware notes: `PCB_VERSION.md`
- Runtime reference: `Laufzeitprognose_MultiConnect_Leer_v1.0.1.xlsx` (Leer measurements; not Kohlbrandbruecke validation)

The AP configuration stores DevEUI, JoinEUI, and AppKey in this project's NVS
namespace and reloads them after restart. A new device gets a chip-derived
DevEUI and empty JoinEUI/AppKey; configure them in AP mode.

The compact V5 uplink contains only battery percentage, battery voltage,
position, Wegsensor raw value, and temperature. INA226 data is not included
in the uplink. Sentinel values represent null measurements; see the payload
comment in `MultiConnect-Kohlbrandbrucke.ino`.

For the Heltec WiFi LoRa 32 V4 profile use EU868 and
`USE_KCT8103L_PA`. Compile, upload, and upload the `data/` filesystem from
this folder separately in Arduino IDE.

## Kohlbrandbruecke profile

The firmware controls the switched sensor rail with GPIO34 only:

- `GPIO34` = sensor MOSFET control.
- `GPIO39` retains the original schematic mapping to `MAX3485 DI`; RS485 is
	not implemented by this firmware.
- The as-built board is confirmed to route GPIO34 through R14 to Q4_G; GPIO39
	is not tied into the MOSFET gate. GPIO34 switches the external 24 V branch
	and the 3V3 peripheral rail together.

The switched `3V3_PERIPH` rail supplies the 10 mm / 1 kOhm potentiometer,
DS18B20 pull-up, ADS1220 digital supply, INA226, and MAX3485. The potentiometer
excitation must use `J2 pin 5 / 3V3_PERIPH`; `J2 pin 4 / 3V3_A` is not suitable
for this load because it is always on. The potentiometer wiper remains on the
configured ADS1220 analog input and its return is `AGND`.

In FIELD mode the sequence is:

```text
sensor rail ON -> settle -> initialize/read sensors -> cache data
-> sensor rail OFF -> LoRaWAN transmit/sleep
```

In AP mode the sensor rail remains on for live monitoring. DS18B20 startup
waits for the switched rail to settle and retries device discovery and
temperature conversion after a power cycle.

RS485 is intentionally unused by this profile. GPIO39, GPIO38, and GPIO40
retain their schematic MAX3485 assignments, but no RS485 communication is
implemented in firmware.

## Power measurement reference

The workbook `Laufzeitprognose_MultiConnect_Leer_v1.0.1.xlsx` uses the CSV
measurement from 2026-09-11 as its active profile. These are inherited Leer
reference measurements, not validation data for Kohlbrandbruecke:

- measured active block: approximately `4.196 s` at `92.676 mA` average;
- measured short peak: approximately `252.2 mA`;
- measured active voltage: approximately `3.518 V`;
- measured sleep value after clamping negative readings to zero: approximately
	`62 uA`, with limited accuracy below the measuring instrument's useful range;
- Heltec reference value: `40 uA` sleep current under ideal conditions.

For the longer-range scenario, the workbook also supports an active duration
of `8-10 s` at an average active current of approximately `62 mA`. These are
planning values, not production guarantees; validate the final radio profile,
battery, temperature range, and converter efficiency on the assembled unit.
