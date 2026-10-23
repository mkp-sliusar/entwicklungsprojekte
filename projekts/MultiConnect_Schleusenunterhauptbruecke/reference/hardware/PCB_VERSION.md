# MultiConnect PCB Version Record

## 1. Scope

This file documents the hardware only:

- Eagle schematic revisions.
- PCB components, packages, values, and electrical functions.
- Power rails, switching circuits, sensor interfaces, and connectors.
- Hardware review findings and release checks.

This file does not document the firmware in `MultiConnect-Kohlbrandbrucke.ino`. The PCB version and firmware version are independent.

The review is based on Eagle schematic files. No Eagle board file (`.brd`), Gerber output, or assembled PCB was supplied, so PCB copper routing, clearances, pad orientation, thermal performance, and manufacturing DRC are not verified by this document.

### Release status for v1.0.1

The switched `3V3_PERIPH` rail and Q3/Q4 gate-driver topology are the PCB
v1.0.1 candidate. The firmware drives the shared Q2/Q4 gate control from
`GPIO34` only, matching the source schematic. GPIO39 remains mapped to MAX3485
`DI`. The project owner confirms that the as-built board routes GPIO34 through
R14 to `Q4_G`, has no GPIO39 gate connection, and switches both 24 V and 3V3
branches from GPIO34.

## 2. Source revisions

### PCB v1.0.0 baseline

Source schematic: `LoRa+NB-IoT v17.sch`

- 49 schematic parts.
- One always-on 3V3 rail named `3V3`.
- Q1 and Q2 switch the external 24 V branch.
- ADS1220, INA226, MAX3485, DS18B20 pull-up, and the sensor connector are connected to the always-on 3V3 rail.
- No Q3, Q4, R18, C8, or C9.
- No separately switched 3V3 peripheral rail.
- C2 is `22 uF` in the baseline schematic.

### PCB v1.0.1 candidate

Source schematic: `LoRa+NB-IoTn3.sch`

- 54 schematic parts.
- Adds exactly these five new references:
	- Q3: P-MOSFET high-side switch.
	- Q4: N-MOSFET gate driver.
	- R18: 100 kOhm Q3 gate-to-source pull-up.
	- C8: 100 nF output decoupling.
	- C9: 10 uF output bulk capacitor.
- Splits the former 3V3 function into:
	- `3V3-IN_S`: unswitched source rail from the controller side.
	- `3V3_PERIPH`: switched peripheral output rail after Q3.
- Keeps the 24 V Q1/Q2 branch and makes Q4 share the same GPIO34 control node as Q2.
- Moves the existing C7 100 nF capacitor to `3V3_PERIPH`.

### Version delta exception

The intended v1.0.1 delta is only Q3, Q4, R18, C8, and C9. The actual `LoRa+NB-IoTn3.sch` also changes C2 from `22 uF` to `4.7 uF`.

This must be resolved before release:

- Restore C2 to `22 uF` if v1.0.1 is intended to contain only the five new components; or
- Record the C2 change as an intentional additional hardware change and verify that `4.7 uF` satisfies the DC/DC converter datasheet.

## 3. Hardware architecture

### 3.1 Alternative controller slots

The board contains two alternative module footprints:

- IC1: Heltec WiFi LoRa 32 V4 module.
- U$2: LilyGO T-SIM7080G-S3 module.

LoRa and T-SIM are alternative population options. They are not intended to be populated or operated simultaneously. Both alternative modules use the `3V3-IN_S` source-side rail in the schematic.

### T-SIM7080G-S3 Standard H795 compatibility check

The PCB schematic connects U$2 `3V3` to `3V3-IN_S`. The official
T-SIM7080G-S3 Standard H795 Rev1.0 schematic generates `+3V3` on-board;
LilyGO documents VBUS (4.5-5.5 V) or VBAT (3.4-4.3 V) as the board power
inputs. The MultiConnect netlist does not show `5V_IN` or VBAT connected to
U$2. Its 3V3 pin may source the external sensor rail when the H795 is powered
separately, but the 3V3 pin alone is not a documented way to power the H795.
The owner confirms power for the planned test is handled; record the actual
VBUS/VBAT path in the as-built schematic before production release.

The owner confirms that the H795 camera and SD card are not connected or used.
This leaves its camera and SD GPIOs available for the MultiConnect sensors:
ADS1220 uses GPIO8/10/11/12/13, INA226 uses GPIO47/48, DS18B20 uses GPIO1,
and the switched sensor rail uses GPIO14. GPIO8 is also the H795 battery ADC
pin, so the first sensor test must leave battery measurement disabled. GPIO48
is the modem RTS pin and is available only while its RTS/CTS pads remain open.

### T-SIM profile selection and observed hardware

Both archived sketches select `LILYGO_ESP32S3_CAM_SIM7080G`. The combined
test sketch probes for an AXP2101 at GPIO15/7; `PMU.begin()` succeeded on
COM22. This positively identifies the connected controller as the
CAM-SIM7080G profile, not the H795 Standard profile previously assumed. The
owner confirms camera and SD are not connected or used. The archives use
ADS1115 and different external sensor wiring, but their SIM7080/AXP2101
modem-power sequence applies to this detected CAM profile. The H795 Standard
pin map below remains relevant only to a future Standard module population.

### CAM-SIM7080G sensor smoke test (2026-10-22)

The combined sensor/modem sketch in `T_SIM7080G_H795_SensorTest/` was compiled
with OPI PSRAM and its runtime AXP2101 probe selected the CAM profile on COM22.
GPIO14 was switched off after the test; while on, it also enabled the external
24 V output.

- ADS1220: initialization passed (`REG0=0x10`). Repeated readings were stable
	around 5,758,000 raw counts / 2.265 V estimated.
- INA226: I2C communication passed at address `0x40` (manufacturer `0x5449`,
	die `0x2260`), but raw bus and shunt registers remained zero. The analog
	measurement path is not yet validated.
- DS18B20: intermittent. One run discovered the device and the second
	conversion returned `23.00 C`; a later run found zero devices. In that run
	GPIO1 measured 1316 mV with the rail OFF and 1353 mV with it ON (logic LOW).
	A later retest repeated two full OFF/ON cycles; both returned zero devices
	and approximately 1323 mV / LOW on GPIO1 with the rail ON.
	The sketch retries the temperature conversion once, but cannot read a device
	that fails OneWire discovery. Sensor VDD at J2 pin 5 remains unmeasured.
	Next isolate the DS18B20/J2 pin 8 branch and measure J2 pin 5 to pin 1 plus
	J2 pin 8 to pin 1 while the rail is ON; do not add more startup delay until
	DATA idles HIGH and the sensor supply is confirmed. On the MQTT test below,
	both fresh reads again found zero DS18B20 devices, so temperature was sent
	as invalid/null.

A diagnostic build waited 10 seconds after command `P`. The newly supplied
v17 netlist shows that GPIO14 drives only the external 24 V J7 MOSFET path
through R14/Q2/Q1; it does not switch sensor 3V3. J2 pin 5 and R9 connect to
the unswitched `3V3` net. Therefore this was not a sensor-rail settling test:
the sensor supply was independent of GPIO14, while J7 24 V was enabled during
the wait. GPIO1 still measured about 1316 mV / LOW and no DS18B20 was found.
Keep GPIO14 LOW during sensor tests and measure J2 pin 5 to pin 1 to confirm
the actual supply voltage.

OneWire library inspection found that `OneWire::begin()` calls `pinMode(pin,
INPUT)`, clearing a previously enabled `INPUT_PULLUP`. The sketch now reapplies
the pull-up after each `oneWire.begin()`; it compiled and was flashed to COM36.
The first retest measured GPIO1 at 1328 mV / LOW and found zero devices. The
v17 schematic confirms the old `P` command enabled J7 24 V, not sensor 3V3;
`O` disabled J7 while sensor 3V3 remained connected. The corrected sketch holds
GPIO14 LOW and treats `P/O` as acquisition controls. Its retest again found
zero devices with GPIO1 at 1331 mV / LOW; `O` confirmed acquisition OFF, sensor
3V3 ON, and J7 24 V OFF. The pull-up ordering bug was real but did not explain
the low line. The later standalone LilyGO test is recorded below; the
MultiConnect PCB's J2 supply and DATA path remain to be checked independently.

### Standalone DS18B20 test on LilyGO (2026-10-23)

The owner tested the CAM-SIM7080G controller without the MultiConnect PCB,
with only the LilyGO and DS18B20 connected. The active sketch selects GPIO9 for
DATA. After moving sensor VDD from the left-side 3V3 contact to the lower-right
3V3 contact shown in the LilyGO pinout, the serial output reported
`[DS18B20] 24.00 C`. This confirms that the sensor and OneWire/Dallas stack can
complete a reading in the standalone setup. The supply-contact change is the
key observed difference; the exact cause of the earlier low DATA readings has
not been isolated. The test sketch diagnostics now report the selected GPIO
instead of hard-coding GPIO1. The MultiConnect PCB path still needs a separate
test at J2 pin 5/pin 8 with GPIO14 held LOW.

The owner then compared the same sensor on the standalone LilyGO: GPIO9 reads
`24.00 C`, while GPIO1 does not. This points to a GPIO1-specific electrical
or camera-interface interaction, rather than a general OneWire or sensor
failure. In the v17 netlist, `TEMP_1` contains only J2 pin 8, Heltec IO47,
T-SIM GPIO1, and R9 pin 1; R9 is 4.7 kOhm from `TEMP_1` to `3V3`. There is no
relay on this net. R3 is 10 kOhm between `N$1` and `AGND`, unrelated to GPIO1.
The separate `MOSFET` net is GPIO14 and drives Q1/Q2 for J7. LilyGO's CAM
profile assigns GPIO1 to camera SIOC; the camera is unused in this test. The
test sketch now uses GPIO9, but v17 J2 pin 8 remains routed to GPIO1, so using
the sensor through J2 requires a hardware reroute or a separate jumper. The
physical PCB routing was not supplied for verification.

### CAM-SIM7080G GPIO budget for supplied PCB v17 (2026-10-23)

This map applies to the attached `LoRa_NB-IoT_v17` netlist and the detected
CAM-SIM7080G profile. Do not combine it with the separate v1.0.1 candidate
below, which has a switched `3V3_PERIPH` rail.

| GPIO | PCB v17 connection | LilyGO CAM-profile role | Decision |
|---|---|---|---|
| 1 | `TEMP_1`, J2 pin 8, R9 pull-up | Camera SIOC | Do not use for DS18B20; fails the standalone test |
| 2 | LOW/OFF/AP switch input | Camera SIOD | Reserved |
| 8 | ADS1220 DRDY | Camera XCLK | Keep for ADS1220 |
| 9 | Not connected in PCB v17 | Camera Y9 | Use for DS18B20 DATA in next PCB; standalone test returned 24.00 C |
| 10 | ADS1220 CS | Camera Y8 | Keep for ADS1220 |
| 11 | ADS1220 MISO | Camera Y7 | Keep for ADS1220 |
| 12 | ADS1220 SCLK | Camera PCLK | Keep for ADS1220 |
| 13 | ADS1220 MOSI | Camera Y6 | Keep for ADS1220 |
| 14 | Q1/Q2 MOSFET control for J7 24 V | Camera Y2 | Reserved; do not reassign |
| 15 | AXP2101 PMU SDA | PMU I2C SDA | Reserved |
| 16 | MAX3485 RE/DE | Camera VSYNC | Reserved for RS485 |
| 17 | MAX3485 DI | Camera HREF | Reserved for RS485 |
| 18 | MAX3485 RO | Camera RESET | Reserved for RS485 |
| 47 | INA226 SDA | Camera Y3 | Keep for INA226 |
| 48 | INA226 SCL | Camera Y4 | Keep for INA226 |

Other CAM-SIM7080G pins to keep reserved: GPIO3/4/5 for modem RING/RX/TX,
GPIO6 for the PMU input, GPIO7 for AXP2101 SCL, GPIO41/42 for modem PWRKEY/DTR,
and GPIO0 for the BOOT button. GPIO0/3/45/46 are ESP32-S3 strapping pins; avoid
using them for external sensors. GPIO19/20 are USB-JTAG. GPIO26-32 are used by
SPI flash, GPIO33-37 by the configured OPI PSRAM, and GPIO38-40 by the SD
interface; keep these unavailable in the current profile. GPIO22-25 are not
ESP32-S3 GPIOs. GPIO43/44 are optional SimShield signals and should only be
considered if that accessory interface is absent. GPIO21 is not connected in
PCB v17 and is an optional spare only with the camera disconnected.

For the next PCB revision, route J2 pin 8 / `TEMP_1` to T-SIM GPIO9 instead of
GPIO1. Keep R9 = 4.7 kOhm from `TEMP_1` to the same 3.3 V rail that supplies
the DS18B20, and verify the voltage at J2 pin 5 on the assembled board. The
v17 netlist shows J2 pin 5 and R9 on the unswitched `3V3` net; the v1.0.1
candidate documents a different switched-rail design. Preserve GPIO14 for J7
24 V control and update firmware profile and schematic together.

### Modem power-control and network test (2026-10-22)

The CAM-SIM7080G archives initialize the AXP2101 at I2C GPIO15/7, set DC3 to
3.0 V, enable DC3, then pulse the modem PWRKEY. The profile uses PWRKEY GPIO41,
DTR GPIO42, RI GPIO3, UART RX GPIO4, and UART TX GPIO5. The combined sketch
now auto-detects the AXP2101, selects these pins, enables DC3 before PWRKEY,
and uses that CAM profile on COM22.

The H795 Standard Rev1.0 schematic is a separate profile: it has no AXP2101
control; its documented modem controls are GPIO42 `Power Save Mode` and GPIO46
`PWRKEY`, with UART RX GPIO5/TX GPIO4. It must not use the CAM DC3/PWRKEY41
sequence.

On COM22, AXP2101 detection, modem AT initialization, and `modem.init()` pass
with the CAM DC3/PWRKEY/UART sequence. The network setup now follows the
official CAM NB-IoT order: RF off, automatic network mode, NB-IoT preferred
mode, APN configuration, RF on, then bounded registration polling. The test
reported SIM status `1` (SIM_READY), later registered to `Telekom.de` with
CSQ `31`, and established packet data using `internet.m2mportal.de`.

MQTT connected to `mqtt.marxkrontal.com:1883` with state `0`; two publishes
succeeded (293 and 299 bytes). Those payloads had invalid/null temperature
because DS18B20 discovery failed in both fresh sensor reads. MQTT
username/password fields are blank; configure them locally if the broker
requires authentication. After the test, command `O` switched off the sensor
rail/J7 24 V output and command `X` powered down CAM DC3.

### Serial monitor boot-loop recovery (2026-10-22)

The monitor was blank because the flashed partition table was invalid for the
16 MB flash: the bootloader repeatedly reported partition 3 at offset
`0xC90000` with size `0xC80000`, beyond the flash limit. Rebuilding with
`PartitionScheme=app3M_fat9M_16MB` produced a valid table (app0/app1 3 MB,
FATFS from `0x610000`, coredump at `0xFF0000`) and flashing restored startup.
COM22 now prints the CAM/AXP2101 profile and responds to `?` at 115200 baud.

The MultiConnect PCB netlist still does not show a supported VBUS (4.5-5.5 V)
or VBAT (3.4-4.3 V) input path to U$2 for autonomous operation. The module's
3V3 pin is not its documented power input; record the final power path in the
as-built schematic before production release.

### 3.2 3V3 peripheral rail

The v1.0.1 power path is:

```text
controller-side 3V3-IN_S -> Q3 source
Q3 drain                   -> 3V3_PERIPH
3V3_PERIPH                 -> external peripheral loads
```

The switched output currently supplies:

- ADS1220 digital supply, `U$1:DVDD`.
- MAX3485 supply.
- INA226 supply.
- DS18B20 pull-up resistor R9.
- 10 mm / 1 kOhm potentiometer excitation through sensor connector J2 pin 5.
- Sensor connector J2 pin 5.
- Analog filter input FB1.
- Output capacitors C7, C8, and C9.

The controller-side source rail supplies the selected Heltec or T-SIM module and is not switched by Q3.

The potentiometer must not be powered from `3V3_A` / J2 pin 4. Its excitation
uses `3V3_PERIPH` / J2 pin 5, its return is `AGND`, and its wiper remains on
the configured ADS1220 analog input. This removes the approximately 3.3 mA
continuous current that would flow through a 1 kOhm potentiometer at 3.3 V.

### 3.3 Q3/Q4 active-high switch

The final v1.0.1 netlist is:

```text
Q3 source -> 3V3-IN_S
Q3 drain  -> 3V3_PERIPH
Q3 gate   -> Q4_D

Q4 source -> AGND
Q4 drain  -> Q4_D
Q4 gate   -> Q4_G

GPIO34    -> R14 100 Ohm -> Q4_G
R15       -> 100 kOhm from Q4_G to AGND
R18       -> 100 kOhm from Q4_D/Q3 gate to 3V3-IN_S
```

Q4 is also connected to Q2 gate through `Q4_G`. Therefore GPIO34 switches both
the external 24 V branch and the 3V3 peripheral branch at the same time on this
schematic. GPIO39 is not part of the MOSFET gate network and remains available
for the original MAX3485 `DI` connection.

Truth table:

| GPIO34 state | Q4 state | Q3 gate | Q3 state | 3V3_PERIPH |
|---|---|---|---|---|
| LOW | OFF | Pulled to 3V3-IN_S by R18 | OFF | Isolated, except leakage |
| HIGH | ON | Pulled toward GND by Q4 | ON | Connected to 3V3-IN_S |

The source/drain direction of Q3 is correct for a P-MOSFET high-side switch. R18 prevents a floating Q3 gate and defines the default OFF state. R15 defines the default OFF state of Q4 and Q2.

### 3.5 GPIO34/VGNSS_CTRL deep-sleep conflict

The Heltec V4.3 board uses GPIO34 as `VGNSS_CTRL` and pulls it up to `VDD_3V3` with onboard R21 = 10 kOhm. The MultiConnect PCB reuses the same GPIO34 through R14 for the Q1/Q2 gate driver. During normal operation the ESP32 actively drives the pin and the 24 V output switches correctly. During deep sleep GPIO34 can become high impedance; R21 then pulls the gate-control node HIGH and can turn Q2/Q1 back on. This explains why AP mode works while the 24 V output may reappear during sleep.

R21 is on the Heltec board and is not removed for the current assembly. Two options are reserved for a future PCB revision:

1. **Resistor option:** replace R15 = 100 kOhm with approximately 2.2 kOhm, or add 2.2 kOhm in parallel, from `Q4_G` to AGND. This should hold the Q2/Q4 gate node below the turn-on voltage while GPIO34 is high impedance. The trade-off is approximately 0.27 mA through the Heltec 10 kOhm pull-up during sleep; the unused GNSS switch Q7 may also be enabled and must be checked.
2. **Pin-swap option:** move the MultiConnect MOSFET control net from GPIO34 to GPIO38 and move RS485 RO from GPIO38 to GPIO34. GPIO34 then remains an input with the Heltec pull-up, while GPIO38 controls Q1/Q2 without the VGNSS_CTRL pull-up. The PCB reroute, firmware pin definitions, and all GPIO conflicts must be verified together.

D1 remains important for limiting Q1 gate-source voltage at 24 V, but it does not explain the sleep-only switching symptom by itself. Its type, polarity, and voltage rating must still be recorded before production release.

### 3.4 Q3/Q4 component requirements

The schematic currently uses generic symbols:

- Q3: `PMOSFET`, package `SOT23-GSD`.
- Q4: `NMOSFET`, package `SOT23-GSD`.

The production BOM must specify real manufacturer part numbers. The selected parts must match the Eagle pin mapping:

```text
SOT23-GSD: pad 1 = G, pad 2 = S, pad 3 = D
```

Suitable candidates discussed during the review were IRLML6402 or AO3401A for Q3 and 2N7002 or BSS138 for Q4. The final manufacturer datasheet and pinout must be checked before PCB release.

## 4. Functional hardware blocks

### 4.1 Heltec controller and LoRa module

IC1 is the Heltec WiFi LoRa 32 V4 symbol with a through-hole module footprint. It provides:

- ESP32-S3 controller.
- SX1262 LoRa radio.
- Main board control and interface GPIOs.
- Controller-side 3.3 V input on `3V3-IN_S`.
- 5 V input on `5V_IN`.
- Ground on `AGND`.

The schematic symbol exposes both `2_3V3-IN` and `3_3V3-IN2`. In the current netlist, `2_3V3-IN` is connected to `3V3-IN_S`, while `3_3V3-IN2` is unconnected. Verify the actual Heltec V4.3 power pin requirements before release.

### 4.2 ADS1220 measurement front end

U$1 is the ADS1220 external module. It is used for bridge and analog measurements.

Digital interface:

```text
DRDY -> Heltec IO45
MISO -> Heltec IO5
MOSI -> Heltec IO4
SCLK -> Heltec IO6
CS   -> Heltec IO3
CLK  -> AGND in the current schematic
```

Supply and reference:

```text
DVDD  -> 3V3_PERIPH
DGND  -> AGND
AVDD  -> 3V3_A
AGND  -> AGND
REFP0 -> 3V3_A
REFN0 -> AGND
```

The ADS1220 analog inputs are connected to the external analog conditioning and bridge networks described below.

### 4.3 0-10 V analog input channels

K1 and K2 are six-pin configuration switches for two analog channels.

Channel 1 uses:

- R5 22 kOhm upper divider resistor.
- R6 10 kOhm lower divider resistor to AGND.
- R7 1 kOhm series/input resistor.
- C4 100 nF filter capacitor to AGND.
- R17 220 Ohm series connection to ADS1220 AIN2.
- K1 selection between voltage, current, and signal paths.

Channel 2 uses:

- R2 22 kOhm upper divider resistor.
- R3 10 kOhm lower divider resistor to AGND.
- R4 1 kOhm series/input resistor.
- C1 100 nF filter capacitor to AGND.
- R16 220 Ohm series connection to ADS1220 AIN3.
- K2 selection between voltage, current, and signal paths.

The 22 kOhm / 10 kOhm dividers reduce 0-10 V field signals to a range suitable for the analog front end. The exact maximum input voltage, resistor tolerance, ADS1220 gain, and protection limits must be verified in the final electrical design.

### 4.4 0-20 mA shunt paths

- R1 100 Ohm is connected from K1 current input `0-20MA` to AGND.
- R8 100 Ohm is connected from K2 current input `0-20MA2` to AGND.

At 20 mA, a 100 Ohm shunt produces approximately 2 V. Verify the power rating and input protection for the actual field current range.

### 4.5 DMS bridge and calibration network

The bridge network contains:

- R_1 = 350 Ohm.
- R_2 = 350 Ohm.
- R_3 = 350 Ohm.
- 3.8MV/V6 = 82 kOhm.
- S1, S2, S3, and S4 for bridge/configuration selection.
- ADS1220 AIN0 and AIN1 connections.

Relevant connections:

```text
3V3_A -> S3 -> R_1 -> AIN0 path
R_2 and R_3 -> S1/S2 bridge switching paths
S4 -> selects the AIN0 path or the 82 kOhm calibration path
3.8MV/V6 -> AGND
```

A 350 Ohm load directly across 3.3 V would draw approximately:

```text
I = 3.3 V / 350 Ohm = 9.4 mA
```

The actual bridge current depends on which switches are closed and the complete bridge topology.

### 4.6 INA226 current and voltage monitor

U$3 is the INA226 module:

```text
VCC -> 3V3_PERIPH
GND -> AGND
SDA -> SDA
SCL -> SCL
ALERT -> unconnected in the current schematic
```

The ALERT pin must be explicitly marked as intentionally unused or routed to a controller GPIO. Check whether the selected INA226 module already contains I2C pull-up resistors; no external SDA/SCL pull-ups are visible in the current netlist.

### 4.7 DS18B20 temperature input

The DS18B20 signal is routed to:

```text
J2 pin 8 -> TEMP_1 -> Heltec IO47 / T-SIM GPIO1
```

R9 is 4.7 kOhm from `TEMP_1` to `3V3_PERIPH`. Because the pull-up is on the switched rail, the temperature interface is disconnected when the peripheral rail is OFF.

### 4.8 RS485 interface

The MAX3485 reference uses the `MAX3088ESA+` library device and SOIC package in the current schematic. This reference/library naming mismatch must be resolved in the production BOM.

Connections:

```text
VCC        -> 3V3_PERIPH
GND        -> AGND
A          -> J7 pin 5
B          -> J7 pin 6
RO         -> Heltec IO38 / T-SIM GPIO18 (reserved)
!RE and DE -> common RE/DE net -> Heltec IO40 / T-SIM GPIO16 (reserved)
DI         -> Heltec GPIO39 (unused by the current firmware)
```

`!RE` and `DE` are tied together. This supports half-duplex direction control, but it does not provide independent receiver disable and driver enable control.
The original schematic connection `DI -> GPIO39` is retained by the
GPIO34-only firmware profile. RS485 communication is not implemented here.

### 4.9 24 V switched external branch

The high-side 24 V switch consists of Q1 and Q2:

```text
24V -> F1 -> Q1 source -> Q1 drain -> J7 pin 4
Q1 gate -> Q2_D
Q2 source -> AGND
Q2 drain -> Q1 gate network
GPIO34 -> R14 -> Q4_G -> Q2 gate
```

Q1 is a generic P-MOSFET in a TO220 package. Q2 is a generic N-MOSFET in an SOT23 package. R13 pulls the Q1 gate toward the source, R12 limits the Q2 drain-to-Q1-gate path, and D1 is a zener clamp across the Q1 gate/source network.

The external 24 V connector paths are:

- J7 pin 2: Q1 source side after F1.
- J7 pin 4: Q1 switched output.

The DC/DC converter input is not behind Q1. U1 VIN is directly connected to the raw `24V` net, so Q1/Q2 do not switch off the converter input.

### 4.10 24 V to 5 V DC/DC converter

U1 is the Wurth Elektronik `173010535` converter symbol.

```text
VIN  -> 24V
GND  -> AGND
VOUT -> N$2
```

The output path is:

```text
U1 VOUT -> N$2 -> LOW/OFF/AP COM_1
C5 100 nF -> N$2 to AGND
LOW/OFF/AP NO_1 and NC_1 -> 5V_IN
```

C2 is the converter input capacitor. Its baseline value is 22 uF; the candidate v1.0.1 schematic shows 4.7 uF and requires confirmation.

### 4.11 Operating mode switch

LOW|OFF|AP is a six-contact mode switch with the following relevant connections:

```text
COM_1 -> N$2 / DC/DC output
NO_1  -> 5V_IN
NC_1  -> 5V_IN
COM_2 -> AGND
NO_2  -> N$4 -> Heltec IO48 / T-SIM GPIO2
NC_2  -> no functional connection found in the current netlist
```

The physical switch selects the board operating/power mode. The final switch truth table must be checked against the intended panel marking.

### 4.12 External connectors

J2 is an eight-pin M12 sensor connector:

| Pin | Net | Function |
|---|---|---|
| 1 | AGND | Sensor ground |
| 2 | AIN0 | Bridge or analog input |
| 3 | N$13 / AIN1 path | ADS1220 differential input path |
| 4 | 3V3_A | Analog excitation/reference rail |
| 5 | 3V3_PERIPH | Switched peripheral supply |
| 6 | SIG_IN1 | Channel 1 signal path |
| 7 | SIG_IN2 | Channel 2 signal path |
| 8 | TEMP_1 | DS18B20 signal |

J7 is an eight-pin M12 field connector:

| Pin | Net | Function |
|---|---|---|
| 1 | AGND | Ground |
| 2 | Q1_S | Fused 24 V source-side path |
| 3 | AGND | Ground |
| 4 | Q1_D | Switched 24 V output |
| 5 | A | RS485 A |
| 6 | B | RS485 B |
| 7 | Unconnected | No current function |
| 8 | Unconnected | No current function |

J1 is a two-pin power connector:

- Pin 1: AGND.
- Pin 2: 5V_IN.

## 5. Schematic named-net review for GPIO34-only firmware

The following connections are taken directly from the `LoRa+NB-IoTn3.sch` netlist.
The firmware uses GPIO34 alone for the shared MOSFET gate control; no GPIO39
gate-driver ECO is required.

| Net | Connected endpoints |
|---|---|
| `3V3-IN_S` | Q3:S, IC1:2_3V3-IN, U$2:3V3, R18:1 |
| `3V3_PERIPH` | Q3:D, U$1:DVDD, MAX3485:VCC, U$3:VCC, FB1:1, J2:5, R9:2, R11:1, C7:1, C8:2, C9:2 |
| `Q4_D` | Q4:D, Q3:G, R18:2 |
| `Q4_G` | Q4:G, Q2:G, R14:1, R15:2 |
| `DI` | MAX3485:DI, IC1:10-IO39/MTCK |
| `MOSFET` | IC1:11-IO34/FSPICS0, U$2:GPIO14, R14:2 |
| `3V3_A` | U$1:AVDD, U$1:REFP0, FB1:2, J2:4, S3:1, R_3:2 |
| `24V` | U1:VIN, F1:2, C2:2 |
| `Q1_S` | Q1:S, F1:1, J7:2, C3:1, R13:1, D1:C |
| `Q1_D` | Q1:D, J7:4 |
| `Q2_D` | Q1:G, R13:2, D1:A, R12:1 |
| `5V_IN` | IC1:2-5V-IN, J1:2, LOW/OFF/AP:NO_1, LOW/OFF/AP:NC_1 |
| `MISO` | IC1:16_IO5/ADC1-4, U$1:MISO, U$2:GPIO11 |
| `MOSI` | IC1:15_IO4/ADC1-3, U$1:MOSI, U$2:GPIO13 |
| `SCLK` | IC1:17_IO6/ADC1-5, U$1:SCLK, U$2:GPIO12 |
| `CS` | IC1:14_IO3/ADC1-2, U$1:CS, U$2:GPIO10 |
| `DRDY` | IC1:6_IO45, U$1:DRDY, U$2:GPIO8 |
| `SDA` | IC1:8_IO41/MTDI, U$3:SDA, U$2:GPIO47 |
| `SCL` | IC1:7_IO42/MTMS, U$3:SCL, U$2:GPIO48 |
| `TEMP_1` | IC1:13-IO47, U$2:GPIO1, J2:8, R9:1 |
| `RE/DE` | MAX3485:!RE, MAX3485:DE, IC1:9_IO40/MTDO, U$2:GPIO16 |
| `DI` | Original schematic: MAX3485:DI to IC1:10_IO39/MTCK; superseded and not populated for the v1.0.1 firmware profile |
| `RO` | MAX3485:R0, IC1:11_IO38/FSPIWP, U$2:GPIO18; reserved in the v1.0.1 firmware profile |
| `A` | MAX3485:A, R11:2, J7:5 |
| `B` | MAX3485:B, R10:1, J7:6 |
| `AGND` | Q3/Q4 gate network ground, Q1/Q2 ground network, controller ground, module grounds, converter ground, connector grounds, and all analog return paths |

## 6. Complete component register for PCB v1.0.1

The package names below are the package/device names present in the Eagle schematic. Generic symbols without a manufacturer part number are not production BOM entries until resolved.

| Ref | Value or device | Package in schematic | Hardware function |
|---|---|---|---|
| U$1 | ADS1220 module | ADS_MODULE_PRINT | External precision ADC for bridge and analog measurements; DVDD on switched rail, AVDD/reference on 3V3_A |
| IC1 | Heltec WiFi LoRa 32 V4 | HELTEC_LORA_32, TH | Alternative LoRa controller module and board controller |
| R5 | 22 kOhm | CHIP-1206 | Channel 1 high-side voltage divider resistor |
| R6 | 10 kOhm | CHIP-1206 | Channel 1 divider return to AGND |
| R7 | 1 kOhm | CHIP-1206 | Channel 1 input series/filter resistor |
| C4 | 100 nF | CHIP-1206 | Channel 1 analog filter capacitor to AGND |
| R1 | 100 Ohm | CHIP-1206 | Channel 1 0-20 mA shunt |
| K1 | DS03 switch | DS-03 | Selects channel 1 voltage, current, and signal paths |
| R2 | 22 kOhm | CHIP-1206 | Channel 2 high-side voltage divider resistor |
| R3 | 10 kOhm | CHIP-1206 | Channel 2 divider return to AGND |
| R4 | 1 kOhm | CHIP-1206 | Channel 2 input series/filter resistor |
| C1 | 100 nF | CHIP-1206 | Channel 2 analog filter capacitor to AGND |
| R8 | 100 Ohm | CHIP-1206 | Channel 2 0-20 mA shunt |
| K2 | DS03 switch | DS-03 | Selects channel 2 voltage, current, and signal paths |
| R_1 | 350 Ohm | CHIP-1206 | DMS bridge resistor |
| R_2 | 350 Ohm | CHIP-1206 | DMS bridge resistor |
| R_3 | 350 Ohm | CHIP-1206 | DMS bridge resistor |
| 3.8MV/V6 | 82 kOhm | CHIP-1206 | Bridge/calibration reference resistor selected by S4 |
| S1 | DS01 switch | DS-01 | Bridge return/configuration switch |
| S2 | DS01 switch | DS-01 | Bridge interconnection switch |
| S3 | DS01 switch | DS-01 | Bridge excitation switch from 3V3_A |
| S4 | DS01 switch | DS-01 | Selects bridge AIN0 path or calibration path |
| MAX3485 | MAX3088ESA+ library device | SOIC127P600X175-8N, SOIC | 3.3 V RS485 transceiver; DI is unavailable in the v1.0.1 firmware profile |
| R10 | 10 kOhm | CHIP-1206 | RS485 B-side bias resistor to AGND |
| R11 | 10 kOhm | CHIP-1206 | RS485 A-side bias resistor to 3V3_PERIPH |
| C3 | 10 uF | CHIP-1206 | Bulk capacitor on the fused Q1 source-side external 24 V branch |
| R13 | 470 kOhm | CHIP-1206 | Q1 gate-to-source pull-up, default Q1 OFF |
| Q1 | Generic PMOSFET | TO220 | High-side switch for the external 24 V output |
| D1 | Generic zener diode | DO-215-AD | Q1 gate/source voltage clamp |
| Q2 | Generic NMOSFET | SOT23 | Low-side driver for Q1 gate |
| R14 | 100 Ohm | CHIP-1206 | Series resistor from GPIO34 control net to Q2/Q4 gate net |
| R15 | 100 kOhm | CHIP-1206 | Pull-down on Q2/Q4 gate net |
| R12 | 47 kOhm | CHIP-1206 | Limits Q2 drain current into Q1 gate network |
| U1 | Wurth 173010535 DC/DC converter | 173010535 | Converts raw 24 V input to the board 5 V path |
| F1 | Fuse, value unspecified | CHIP-1206 | Input fuse between raw 24 V and Q1 source-side external branch |
| C2 | 4.7 uF in n3; 22 uF in v1.0.0 | CHIP-1206 | DC/DC input capacitor; value change requires decision |
| C5 | 100 nF | CHIP-1206 | DC/DC output capacitor on N$2 |
| U$2 | LilyGO T-SIM7080G-S3 | T-SIM7080G_PRINT | Alternative NB-IoT module slot; do not populate with LoRa slot simultaneously |
| J2 | M12A-08PFFR-SF7003 | M12A-08PFFR-SF7003 | Eight-pin external sensor connector |
| J7 | M12A-08PFFR-SF7003 | M12A-08PFFR-SF7003 | Eight-pin 24 V and RS485 field connector |
| U$3 | INA226 module | INA226 | Switched-rail current/voltage monitor |
| FB1 | Ferrite bead, value unspecified | CHIP-1206 | Separates switched digital 3V3_PERIPH from analog 3V3_A |
| C6 | 100 nF | CHIP-1206 | Capacitor between the AIN0/AIN1 analog path nodes |
| C7 | 100 nF | CHIP-1206 | Existing decoupling capacitor moved to 3V3_PERIPH in v1.0.1 |
| J1 | B2B-XH-A | B2B-XH-A | Two-pin 5 V input connector |
| R9 | 4.7 kOhm | CHIP-1206 | DS18B20 pull-up to 3V3_PERIPH |
| LOW/OFF/AP | Three-position switch | 100DP3T1B1M2QEH | Selects power/operating mode and controller mode input |
| R16 | 220 Ohm | CHIP-1206 | ADS1220 AIN3 series resistor |
| R17 | 220 Ohm | CHIP-1206 | ADS1220 AIN2 series resistor |
| Q4 | Generic NMOSFET | SOT23-GSD | Pulls Q3 gate low when GPIO34 is HIGH |
| Q3 | Generic PMOSFET | SOT23-GSD | Switched high-side supply for 3V3_PERIPH |
| C8 | 100 nF | CHIP-1206 | High-frequency output decoupling after Q3 |
| C9 | 10 uF | CHIP-1206 | Bulk output capacitor after Q3 |
| R18 | 100 kOhm | CHIP-1206 | Q3 gate-to-source pull-up and default-OFF resistor |

No GPIO39 gate resistor or auxiliary control is required for this firmware
profile. Keep or verify the schematic's GPIO39-to-MAX3485-DI route on the
as-built board; the PCB files were not supplied for copper-level verification.

## 7. Power and current comparison

This section compares the reported Kohlbrandbruecke reading with legacy Leer
PCB v1.0.0 CSV measurements and calculations. None are guaranteed limits.

- Reported Kohlbrandbruecke reading: PCB v1.0.1 with the current firmware draws
	approximately 0.31 mA (310 uA) in sleep mode. The measurement date, instrument,
	and exact operating setup were not provided; treat this as an observation,
	not a production limit.
- CSV capture duration: approximately 94.882 s at a 2 ms sample interval.
- Active measurement/transmission block: approximately 4.196 s.
- Active current: approximately 92.676 mA average.
- Active peak: approximately 252.2 mA.
- Active voltage: approximately 3.518 V average; active power approximately 324.875 mW.

- Legacy Leer PCB v1.0.0 CSV sleep current after clamping negative instrument readings
	to zero: approximately 62 uA average. This is an estimate; the instrument is
	not sufficiently accurate below approximately 1 mA and has analog zero offset/noise.
- Heltec's stated sleep value is approximately 40 uA under the relevant ideal
	conditions; this is a reference value, not the measured Kohlbrandbruecke
	system current.
- Long-range planning scenario: active duration 8-10 s at approximately 62 mA
	average. This scenario must be measured on the final radio configuration.
- USB-connected sleep can be around 2 mA and is not representative of battery-only sleep.
- A 100 kOhm Q3 gate pull-up at 3.3 V corresponds to approximately 33 uA while Q4 is ON.
- MOSFET gate networks normally contribute tens of uA, not mA.
- A 350 Ohm bridge element at 3.3 V can consume approximately 9.4 mA when directly energized.
- U1 is directly connected to raw 24 V, so its quiescent current remains present even when Q1 is OFF.
- The workbook `Laufzeitprognose_MultiConnect_Leer_v1.0.1.xlsx`
	contains inherited Leer battery and runtime calculations. It is reference
	material, not Kohlbrandbrücke runtime validation.

## 8. Hardware review findings and release checklist

### Required before PCB release

- [ ] Decide whether C2 is 22 uF or 4.7 uF and record the decision in the BOM.
- [ ] Assign manufacturer part numbers to Q3 and Q4.
- [ ] Verify Q3 and Q4 manufacturer pinouts against the `SOT23-GSD` Eagle symbol.
- [x] As-built board routes GPIO34 through R14 to `Q4_G`; GPIO39 is not tied into the MOSFET gate (owner confirmation).
- [x] As-built gate logic switches `3V3_PERIPH` and the 24 V branch from GPIO34 LOW/HIGH (owner confirmation).
- [x] RS485 is intentionally unused by this firmware profile.
- [ ] Verify IC1 `3_3V3-IN2`; it is currently unconnected in the schematic.
- [ ] Resolve the Heltec V4.3 GPIO5 conflict: ADS1220 MISO uses IO5, while the KCT8103L FEM uses IO5 as PA_CTX.
- [ ] Verify whether GPIO34 may be reused with the Heltec internal GNSS power-control circuit for the selected population option.
- [ ] Verify that the potentiometer excitation is on J2 pin 5 (`3V3_PERIPH`), not J2 pin 4 (`3V3_A`), and that the wiper/AGND wiring is correct.
- [ ] Verify that the switched digital loads, DS18B20 pull-up, INA226, MAX3485, ADS1220 DVDD, and potentiometer excitation are intentionally on `3V3_PERIPH`.
- [ ] Mark INA226 ALERT as intentional no-connect or route it to a defined GPIO.
- [ ] Verify T-SIM power pads and the alternative-slot population rule. The small switched sensor rail must not be used for a high-current modem design without a separate power analysis.
- [ ] Resolve the `MAX3485` reference versus `MAX3088ESA+` library-device naming mismatch.
- [ ] Specify values and ratings for F1, D1, and the Q1/Q2 MOSFETs.
- [ ] Verify DC/DC input and output capacitor requirements against the U1 datasheet.
- [ ] Add or verify local analog decoupling for ADS1220 AVDD and the 3V3_A/reference node.
- [ ] Run Eagle electrical-rule check and board DRC on the actual `.brd` file.
- [ ] Verify SMD polarity/orientation, especially D1, Q3, Q4, FB1, and any polarized C9 implementation.

### Confirmed in `LoRa+NB-IoTn3.sch`

- [x] Q3 source is on `3V3-IN_S`.
- [x] Q3 drain is on `3V3_PERIPH`.
- [x] Q4 source is on AGND.
- [x] Q4 drain is connected to Q3 gate.
- [x] GPIO34 reaches Q4/Q2 through R14.
- [x] The source schematic maps GPIO39 to MAX3485 `DI`, not the Q4/Q2 gate network; the owner confirms GPIO39 is not tied to the as-built MOSFET gate.
- [x] R15 is the Q4/Q2 gate pull-down.
- [x] R18 is 100 kOhm from Q3 gate to Q3 source.
- [x] C8 is 100 nF from `3V3_PERIPH` to AGND.
- [x] C9 is 10 uF from `3V3_PERIPH` to AGND.
- [x] Q3/Q4 active-high switching logic is electrically consistent.

## 9. Release conclusion

The Q3/Q4/R18/C8/C9 topology in `LoRa+NB-IoTn3.sch` is electrically correct
for a GPIO34-controlled switched 3V3 peripheral rail. Before production:

1. The selected MOSFETs match the Eagle symbol pinout.
2. The Heltec controller remains on `3V3-IN_S`.
3. Only the intended external peripheral loads remain on `3V3_PERIPH`.
4. RS485 is intentionally unused; GPIO39 is not part of MOSFET gate control.
5. The C2 value change is explicitly resolved.
6. The GPIO5, GPIO34, GPIO39, Heltec supply-pin, BOM, and PCB-layout checks
	are completed.

The inherited v1.0.1 firmware was validated on the Leer test unit, including
the switched potentiometer/DS18B20 rail and measured temperature uplink. The
Kohlbrandbrücke profile has not yet been validated on its target hardware.
The GPIO34-only gate behavior is confirmed, but the PCB remains a schematic
candidate, not a released manufacturing revision, until the remaining
checklist items are verified on the assembled board.
