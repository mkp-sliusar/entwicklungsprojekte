# Nibelung Bridge Raspberry Pi service

For operators and end users, see the [user manual](USER_MANUAL.md).

This rebuilt version separates measurement, system monitoring and outputs into independent workers:

- `sensor-adxl345` reads ADXL345 at `sensors.adxl345.sample_rate_hz`.
- `sensor-ads1220` reads ADS at `sensors.ads1220.sample_rate_hz`.
- `sensor-x728` reads the X728 battery voltage and estimated charge level over I2C.
- `sensor-pisugar` reads PiSugar battery and power status over I2C when present.
- `sensor-system` reads OS metrics at `services.system_monitor.sample_rate_hz`.
- Each destination (`influxdb`, `mqtt_external`, `mqtt_local`, `websocket`) has its own output thread and bounded queue.
- Every enabled hardware sensor reconnects independently when it is absent or loses communication.

A slow MQTT broker, InfluxDB write, WebSocket client or system metric check cannot block sensor measurement threads.
An unavailable sensor does not stop the API, outputs or other sensor loops.

## Install on Raspberry Pi 4B

```bash
cd /opt
sudo mkdir -p nibelung_bridge
sudo chown -R $USER:$USER nibelung_bridge
# copy project files here
cd /opt/nibelung_bridge
bash scripts/install.sh
```

Enable hardware interfaces:

```bash
sudo raspi-config
# Interface Options -> I2C -> enable
# Interface Options -> SPI -> enable
sudo reboot
```

Run manually:

```bash
cd /opt/nibelung_bridge
./scripts/run.sh
```

## systemd service

Edit `systemd/nibelung-bridge.service` if your path or user is different, then:

```bash
sudo cp systemd/nibelung-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable nibelung-bridge
sudo systemctl start nibelung-bridge
journalctl -u nibelung-bridge -f
```

## Sensor reconnect

Each hardware sensor accepts `retry_interval_s`, which defaults to `5` seconds.
Connection and read failures are isolated to that sensor; the bridge keeps
running and retries in its own worker. For example:

```yaml
sensors:
  adxl345:
    retry_interval_s: 3
  ads1220:
    retry_interval_s: 5
```

The ADS startup tare also runs asynchronously after a successful connection.

## YAML output modes

Use these per destination and per sensor:

- `mode: raw` sends every measured sample.
- `mode: latest` sends only the newest sample at `rate_hz`.
- `mode: aggregate` sends a window at `rate_hz` with RMS/peak/avg fields.

Use these destination payload modes:

- `payload: per_sensor` sends separate sensor messages or Influx measurements.
- `payload: snapshot` sends one combined dashboard-style message with latest `adxl345`, `ads1220`, `system` and `status`.

Accelerometer payloads use the same common fields for ADXL345 and any retained
MPU9250 integration: `ax`, `ay`, `az`, `roll_deg`, `pitch_deg`, `acc_mag_g`,
`vibration_ms2`, `vibration_z_ms2`, `frequency_hz`, and rate metadata. The
`sensor` field, Influx measurement, and MQTT topic identify the concrete model
(`adxl345` or `mpu9250`). Temperature is optional because ADXL345 has no
temperature sensor.

To send every ADXL345 sample to WebSocket at 200 Hz:

```yaml
sensors:
  adxl345:
    sample_rate_hz: 200

destinations:
  websocket:
    enabled: true
    payload: per_sensor
    sensors:
      adxl345:
        enabled: true
        mode: raw
```

To measure ADXL345 at 200 Hz but send MQTT aggregates at 10 Hz:

```yaml
sensors:
  adxl345:
    sample_rate_hz: 200

destinations:
  mqtt_external:
    sensors:
      adxl345:
        enabled: true
        mode: aggregate
        rate_hz: 10
```

## Tare and filter API

- `POST /tare` toggles tare in a separate thread and returns `{"status":"started"}`.
- Startup tare enables corrected values automatically. The first later tare command switches to `mode: "raw"`; the next one samples a new zero and switches back to `mode: "tared"`.
- Poll `GET /status` and wait for `tare.running` to become `0`; `tare.mode`, `tare.status`, and `tare.raw_values` show which values are being published.
- A tare request returns HTTP `409` when tare is already running or ADS1220 is unavailable.
- `POST /filter` with `{"alpha": 0.05}` updates ADS filtering.
- `GET /filter` returns current ADS filter alpha.
- `GET /status` returns current service status.

WebSocket clients can send `{"command":"tare"}` to `ws://<bridge>:8765` and
receive `tare_done` and `tare_status` messages with `mode: "tared"` or
`mode: "raw"`. The supplied Grafana dashboards are telemetry-only and do not
include a tare control; a Grafana HTTP action must send
`POST http://<bridge>:8080/tare`.

The live WebSocket sensor channels are:

- `NiE+080_HSN-u-_DU.E+080DU_HSN-u-` for strain.
- `NiE+233_HSN-m-_BU.E+233BU_HSN-m-` for vertical vibration acceleration.
- `battery_percent` for the PiSugar/X728 battery charge level.

Subscribe to the battery channel with
`{"command":"subscribe","channel":"battery_percent"}`. Its value is a
rolling median of the last five WebSocket samples; change
`destinations.websocket.battery_percent_median_window` to tune the smoothing.

The local MQTT client also publishes the retained dashboard status on
`live/<device-id>/tare` with `tare_status` (`TARED` or `RAW`) and
`tare_status_code` (`1` or `0`).

Default API port: `8080`.

## Raspberry Pi ribbon cable pin map

The project uses BCM GPIO names in configuration. The physical header numbers
and ribbon colors for the active connections are:

| Project signal | Device | BCM GPIO | Physical pin | Ribbon color |
|---|---|---:|---:|---|
| I2C SDA1 | ADXL345, PiSugar | GPIO2 | 3 | orange |
| I2C SCL1 | ADXL345, PiSugar | GPIO3 | 5 | green |
| SPI MOSI | ADS1220 | GPIO10 | 19 | white |
| SPI MISO | ADS1220 | GPIO9 | 21 | brown |
| SPI SCLK | ADS1220 | GPIO11 | 23 | orange |
| SPI CE0 / chip select | ADS1220 | GPIO8 | 24 | yellow |
| ADS1220 DRDY | ADS1220 | GPIO17 | 11 | brown |
| Tare button | Button | GPIO27 | 13 | orange |

Use a 3.3 V contact and a GND contact shared by the connected boards. With the
provided color order, physical pin 1 is 3.3 V / brown and physical pin 6 is
GND / blue. Other 3.3 V and GND contacts have their own colors in the ribbon
table and are electrically equivalent. With `pull_up: true`, the tare button
must connect GPIO27 to GND when pressed.

The optional X728 uses the same I2C SDA/SCL lines at address `0x36`; it is
currently disabled in `config.yaml`. SPI CE1 / physical pin 26 / GPIO7 is
available but unused.

### ADXL345 I2C wiring

The default ADXL345 address is `0x53`, so it does not conflict with the
PiSugar at `0x57` or the X728 RTC at `0x68`:

1. Power off the Raspberry Pi.
2. Connect ADXL345 `VCC` to `3.3 V`, `GND` to GND, `SDA` to GPIO2/pin 3,
   and `SCL` to GPIO3/pin 5.
3. Connect `CS` to `3.3 V` to select I2C mode.
4. Leave `SDO`/`ALT ADDRESS` low for `0x53` and leave `EDA`/`ECL` unused.
5. Leave `sensors.adxl345.address` set to `0x53` in `config.yaml`.

Do not connect any ADXL345 pin to 5 V. With this wiring, `i2cdetect -y 1`
should show `0x53`, the PiSugar at `0x57`, and the RTC may still appear at
`0x68`. The alternate ADXL345 address is `0x1D` when `SDO` is high.

## Geekworm X728 UPS

Enable the optional X728 telemetry in `config.yaml`:

```yaml
sensors:
  x728:
    enabled: true
    i2c_bus: 1
    address: 0x36
    sample_rate_hz: 1
```

The sensor publishes `battery_voltage_v` and `battery_percent` as `x728` data.
The X728 RTC is at I2C address `0x68`. ADXL345 at `0x53` can share the bus with
the RTC and PiSugar; the old MPU9250 configuration remains disabled because
its `0x68/0x69` addresses are less convenient for this hardware mix.
GPIO-based X728 shutdown and charging control require a confirmed X728 revision
and are not enabled by this integration.

X728 and PiSugar are optional. With `auto_detect: true`, the bridge probes X728
at I2C `0x36` and PiSugar at I2C `0x57` during startup and retries at their
configured `retry_interval_s`. A missing board remains isolated without
stopping ADXL345, ADS1220, system monitoring, or outputs. Only the detected
UPS publishes its own sensor topic.

PiSugar publishes `battery_voltage_v`, `battery_percent`, `external_power`,
`charging_enabled`, and `output_enabled` as `pisugar` data.

## Grafana X728 dashboard

Import [grafana/x728-ups-dashboard.json](grafana/x728-ups-dashboard.json) in
Grafana and select the existing InfluxDB data source when prompted. The
dashboard expects bucket `MKP`, measurement `x728`, and the fields
`battery_percent` and `battery_voltage_v`.

For the combined dashboard with the system monitor panels, import
[grafana/system-monitor-x728-dashboard.json](grafana/system-monitor-x728-dashboard.json).
It is titled `1. System Monitor` and includes CPU, RAM, disk, CPU temperature,
network throughput, X728 battery charge, and X728 voltage panels.
