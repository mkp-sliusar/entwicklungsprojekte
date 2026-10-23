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

## Einheitlicher Sensor-Namensstandard (Vorschlag)

Fuer neue Projekte sollen Payload- und API-Namen die Messgroesse beschreiben,
nicht Hersteller, Messprinzip oder Einsatzort. Damit heisst ein Wegaufnehmer
fuer Riss, Durchbiegung oder Setzung immer `weg_mm`. Namen sind lowercase
`snake_case`; physikalische Werte erhalten die Einheit als Suffix. Achsen
werden als `_x_`, `_y_` und `_z_` angegeben.

| Bedeutung | Reservierter Name | Einheit | Hinweis |
|---|---|---|---|
| Weg / Verschiebung | `weg_mm` | mm | Unabhaengig vom Einsatzzweck |
| Rohwert Wegaufnehmer | `weg_raw` | count | Unskalierter ADC-/Sensorwert, nicht mm |
| Weg X | `weg_x_mm` | mm | Achse im projektspezifisch dokumentierten Koordinatensystem |
| Weg Y | `weg_y_mm` | mm | Achse im projektspezifisch dokumentierten Koordinatensystem |
| Weg Z | `weg_z_mm` | mm | Achse im projektspezifisch dokumentierten Koordinatensystem |
| Temperatur | `temp_c` | deg C | Celsius |
| Batteriespannung | `batt_v` | V | |
| Batterieladezustand | `batt_percent` | % | Schaetzung, falls nicht direkt gemessen |
| Versorgungsspannung | `supply_v` | V | |
| Stromaufnahme | `current_ma` | mA | Bei mehreren Stroemen qualifizieren, z.B. `batt_current_ma` |
| Kraft | `force_n` | N | |
| Druck | `pressure_kpa` | kPa | |
| Relative Feuchte | `humidity_percent` | %RH | |
| Beschleunigung X | `accel_x_ms2` | m/s^2 | |
| Beschleunigung Y | `accel_y_ms2` | m/s^2 | |
| Beschleunigung Z | `accel_z_ms2` | m/s^2 | |
| Neigung X | `tilt_x_deg` | deg | |
| Neigung Y | `tilt_y_deg` | deg | |
| Neigung Z | `tilt_z_deg` | deg | |
| RSSI | `rssi` | dBm | Empfangspegel |
| SNR | `snr` | dB | Signal-Rausch-Verhaeltnis |
| LoRaWAN Data-Rate-Index | `lora_dr` | index | `dr` allein ist mehrdeutig |
| Datenrate | `data_rate_bps` | bit/s | Nur fuer eine physikalische Bitrate |
| Zeitstempel | `timestamp` | ISO 8601 UTC | Beispiel: `2026-10-22T12:30:00Z` |
| Breitengrad | `latitude_deg` | deg | WGS84 |
| Laengengrad | `longitude_deg` | deg | WGS84 |
| Hoehe | `altitude_m` | m | Bezugsniveau projektspezifisch dokumentieren |

`_raw` ist ausschliesslich fuer unskalierte Messwerte vorgesehen. Rohwerte
duerfen nicht mit physikalischen Werten vermischt werden. Wenn mehrere Sensoren
dieselbe Groesse liefern, wird die Instanz zusaetzlich eindeutig nummeriert
oder ueber Metadaten identifiziert; der Basisname bleibt erhalten.

### Bestehende Payload V5 Namen

Die V5-Feldnamen sind Bestandteil des bestehenden Funkprotokolls und werden
hier nicht geaendert. Bei einer kuenftigen Payload-Version sollen sie auf den
Standard abgebildet werden:

| Payload V5 heute | Kanonischer Name fuer neue Payloads |
|---|---|
| `battery_percent` | `batt_percent` |
| `battery_voltage_v` | `batt_v` |
| `position_mm` | `weg_mm` |
| `weg_raw` | `weg_raw` |
| `temperature_c` | `temp_c` |

Eine Protokollversion muss alte Decoder weiter unterstuetzen oder eindeutig
versioniert werden.

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
AP sensor reads and visible-tab state polling are scheduled once per second;
both pause during a manual LoRa test.

RS485 is intentionally unused by this profile. GPIO39, GPIO38, and GPIO40
retain their schematic MAX3485 assignments, but no RS485 communication is
implemented in firmware.

## Messwertmittelung

- Wegsensor (ADS1220): ein erster Messwert wird verworfen; der Mittelwert wird aus 10 Einzelmessungen gebildet.
- Batteriespannung: die erste ADC-Messung nach Aktivierung des Teilers wird verworfen; danach werden 24 Messungen gemittelt.
- DS18B20: einzelne 10-Bit-Konversion, keine Mittelung; Wiederholung nur bei ungültigem Messwert.
- INA226: Mittelung auf 1 Sample eingestellt; je Aktualisierung wird ein Bus- und ein Shuntwert gelesen.

## AP LoRaWAN test

In Expert diagnostics, `Send cached V5 test uplink` sends the current cached
Payload V5 on fPort 2 as a confirmed uplink. It uses the saved OTAA credentials
and does not change the production payload or schedule a recurring send. While
joining and waiting for the Class A response, AP sensor polling is paused and
the ADS1220 interface is released; sensor-power and configuration writes are
temporarily blocked.

The result distinguishes an ACK, an application downlink, a join timeout, and
a send without ACK. A send without ACK may still have reached the gateway.
RSSI/SNR in the web UI are measured by the device for the last received
gateway-to-device reply, including a MAC ACK, so the values update after the
confirmed uplink response. Gateway reception quality for the uplink is
per-gateway ChirpStack `rxInfo` metadata and is not available to the device
unless the server sends it back in a downlink.

## Power measurement reference

Reported Kohlbrandbruecke measurement: PCB v1.0.1 with the current firmware
draws approximately `0.31 mA` (`310 uA`) in sleep mode. The measurement date,
instrument, and exact operating setup were not provided; treat this as an
observed value, not a production limit.

The Leer PCB v1.0.0 baseline had an always-on 3V3 rail and no switched
`3V3_PERIPH` rail, unlike the v1.0.1 candidate. The readings come from
different hardware and undocumented test setups, so they are not a direct
firmware-only comparison.

The workbook `Laufzeitprognose_MultiConnect_Leer_v1.0.1.xlsx` uses the CSV
measurement from 2026-09-11 as its active profile. These are inherited Leer
reference measurements, not validation data for Kohlbrandbruecke:

- measured active block: approximately `4.196 s` at `92.676 mA` average;
- measured short peak: approximately `252.2 mA`;
- measured active voltage: approximately `3.518 V`;
- legacy Leer PCB v1.0.0 sleep value after clamping negative readings to zero:
	approximately `62 uA`, with limited accuracy below the measuring instrument's
	useful range;
- Heltec reference value: `40 uA` sleep current under ideal conditions.

For the longer-range scenario, the workbook also supports an active duration
of `8-10 s` at an average active current of approximately `62 mA`. These are
planning values, not production guarantees; validate the final radio profile,
battery, temperature range, and converter efficiency on the assembled unit.
