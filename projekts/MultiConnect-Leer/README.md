# MultiConnect-Leer

Project-specific MKP MultiConnect firmware for Leer Suedringbruecke.

- Release: `v1.0.1`
- Sketch: `MultiConnect-Leer.ino`
- Firmware ID: `1.0.1-Leer`
- AP SSID: `MultiConnect-Leer-<MAC suffix>`
- NVS namespace: `multileer`
- LoRa payload: V5, 12 bytes
- ChirpStack codec: `CHIRPSTACK_CODEC.js`
- LittleFS files: `data/`
- Hardware notes: `PCB_VERSION.md`
- Runtime model: `Laufzeitprognose_MultiConnect_Leer_v1.0.1.xlsx`

The compact V5 uplink contains only battery percentage, battery voltage,
position, Wegsensor raw value, and temperature. INA226 data is not included
in the uplink. Sentinel values represent null measurements; see the payload
comment in `MultiConnect-Leer.ino`.

For the Heltec WiFi LoRa 32 V4 profile use EU868 and
`USE_KCT8103L_PA`. Compile, upload, and upload the `data/` filesystem from
this folder separately in Arduino IDE.

## Release v1.0.1

The v1.0.1 firmware controls the switched sensor rail with two outputs in
parallel:

- `GPIO34` = primary sensor MOSFET control.
- `GPIO39` = auxiliary sensor MOSFET control.
- Both outputs use the same active-high state and must reach the shared gate
	driver through separate series resistors.
- `GPIO39` is dedicated to sensor power in this profile and must not remain
	connected as `MAX3485 DI`.

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

The current v1.0.1 firmware profile leaves the MAX3485 `DI` signal unused:
`GPIO40` remains `RE/DE` and `GPIO38` remains `RO`. The original schematic
mapping of `MAX3485 DI` to `GPIO39` must be removed or changed in the
as-built PCB before production.

## Power measurement reference

The workbook `Laufzeitprognose_MultiConnect_Leer_v1.0.1.xlsx` uses the CSV
measurement from 2026-09-11 as its active profile:

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
