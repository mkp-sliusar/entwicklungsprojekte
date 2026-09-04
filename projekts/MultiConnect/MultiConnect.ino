/*
 * =====================================================================
 *  MKP MultiConnect V1.4.5
 *  Universal LoRaWAN / NB-IoT-ready sensor controller
 *  Current firmware profile: LoRaWAN + KCT8103L FEM + optional DS18B20 + ADS1220 Wegsensor + INA226 + SenseCAP Modbus
 *
 *  RELEASE NOTES - V1.5.0
 *    - KCT8103L FEM is selected in Arduino IDE and controlled only by the
 *      Heltec radio driver during TX and RX windows.
 *    - External sensor power and ADS1220 measurements are kept separate from
 *      LoRaWAN radio activity.
 *    - Wegsensor calibration is applied to the live cache immediately after
 *      it is saved from the AP interface.
 *    - The battery profile can be selected for LiPo/18650 or SAFT 3.6 V
 *      Li-SOCl2 cells.
 *    - INA226 is optional. A physically disconnected INA226 is reported as
 *      unavailable and its payload values are intentionally null.
 *    - AP mode provides captive-portal DNS. Automatic browser redirect is
 *      disabled by default and can be enabled in Expert mode.
 *
 *  REQUIRED ARDUINO IDE PROFILE
 *    Board: WiFi LoRa 32 (V4)
 *    LoRaWan Region: REGION_EU868
 *    LoRa FEM Type: USE_KCT8103L_PA
 *    LoRa External FEM Receive Gain: choose as required for the installation
 * ---------------------------------------------------------------------
 *  Board: Heltec WiFi LoRa 32 V4.3 (ESP32-S3, SX1262)
 *  Region: EU868, OTAA, Class A
 *
 *  Operating modes:
 *    FIELD - Wi-Fi/Bluetooth/OLED off, LoRaWAN Class A, low-power sleep.
 *    AP    - configuration access point, web UI, OLED and live monitoring.
 *
 *  AP selection at startup:
 *    GPIO48 connected to GND  -> FIELD low-power mode.
 *    GPIO48 open / HIGH       -> AP configuration mode.
 *    The mode is sampled once during startup.
 *
 *  Hardware:
 *    ADS1220: SCLK=6, MISO=5, MOSI=4, CS=3, DRDY=45 (HSPI)
 *    WARNING: MISO GPIO5 is shared with Heltec V4.3 PA_CTX GPIO5;
 *    rewire ADS1220 MISO before using the external FEM reliably.
 *    Wegsensor: AIN2 - AIN3
 *    DS18B20: GPIO47, external 4.7 kOhm pull-up to 3.3 V
 *    INA226 (optional): SDA=41, SCL=42, address 0x40 (second I2C controller)
 *    Internal battery measurement:
 *      VBAT ADC=GPIO1, divider enable=GPIO37, divider ratio=4.9
 *    V4.3 board controls: Vext GPIO36, VFEM GPIO7, PA_CSD GPIO2,
 *    PA_CTX GPIO5, reserved sensor MOSFET GPIO34, white LED GPIO35,
 *    battery divider GPIO37
 *
 *  Important OneWire patch for GPIO47:
 *    OneWire 2.3.8 / util / OneWire_direct_gpio.h
 *    replace every "pin < 46" with "pin < 49".
 *
 *  Important LoRa / ADS1220 behavior:
 *    ADS1220 measurements are cached. LoRaWAN SEND uses only cached data.
 *    Do not read ADS1220 immediately before LoRaWAN.send().
 *    GPIO5 is shared by ADS1220 MISO and KCT8103L PA_CTX. The firmware uses
 *    these pins in separate phases; do not use them simultaneously.
 * =====================================================================
 */

#include <Arduino.h>

#ifdef LORAWAN_DEVEUI_AUTO
#undef LORAWAN_DEVEUI_AUTO
#endif
#define LORAWAN_DEVEUI_AUTO 0

#define USE_KCT8103L_PA
#include "LoRaWan_APP.h"
#include "radio/radio.h"

#if defined(USE_NONE_PA) || defined(USE_GC1109_PA)
#error "Select Tools > LoRa FEM Type > USE_KCT8103L_PA for this firmware"
#endif

#if !defined(LORA_PA_POWER) || !defined(LORA_PA_CSD) || !defined(LORA_PA_CTX)
#error "Install a KCT8103L-capable Heltec ESP32 Dev-Boards library"
#endif

#include "HT_SSD1306Wire.h"
#include <ArduinoJson.h>
#include <DallasTemperature.h>
#include <DNSServer.h>
#include <HardwareSerial.h>
#include <LittleFS.h>
#include <OneWire.h>
#include <Preferences.h>
#include <SPI.h>
#include <WebServer.h>
#include <WiFi.h>
#include <Wire.h>
#include "driver/rtc_io.h"
#include <math.h>

// ============================= Firmware =============================
static constexpr const char* FW_VERSION = "1.5.0";
static constexpr const char* DEVICE_NAME = "MKP MultiConnect";

// ============================== Modes ===============================
static constexpr uint8_t PIN_AP_MODE = 48;  // HIGH/open at startup selects AP mode
static constexpr char AP_PASSWORD[] = "12345678";

bool apMode = false;
bool littleFsReady = false;

// ============================== Pins ================================
static constexpr uint8_t PIN_ADS_SCLK = 6;
static constexpr uint8_t PIN_ADS_MISO = 5;
static constexpr uint8_t PIN_ADS_MOSI = 4;
static constexpr uint8_t PIN_ADS_CS = 3;
static constexpr uint8_t PIN_ADS_DRDY = 45;

static constexpr uint8_t PIN_DS18B20 = 47;
static constexpr uint8_t PIN_INA_SDA = 41;
static constexpr uint8_t PIN_INA_SCL = 42;

// Heltec WiFi LoRa 32 V4.3 onboard power-control pins.
static constexpr uint8_t PIN_LORA_FEM_POWER = LORA_PA_POWER;
static constexpr uint8_t PIN_LORA_FEM_CSD = LORA_PA_CSD;
static constexpr uint8_t PIN_LORA_FEM_CTX = LORA_PA_CTX;
// GPIO34 also drives the unpopulated V4.3 GNSS power switch. With no GNSS
// module fitted it is available for the external sensor MOSFET; LOW is off.
static constexpr uint8_t PIN_SENSOR_MOSFET = 34;
// Latest schematic: GPIO34 controls the shared Q1/Q2 gate driver.
static constexpr uint8_t SENSOR_MOSFET_ON_LEVEL = HIGH;
static constexpr uint8_t SENSOR_MOSFET_OFF_LEVEL = LOW;
static constexpr uint8_t PIN_BOARD_LED = 35;      // White LED, LOW is off

// Heltec WiFi LoRa 32 V4.x internal Li-ion battery monitor.
static constexpr uint8_t PIN_BATTERY_ADC = 1;    // ADC1_CH0 / VBAT_Read
static constexpr uint8_t PIN_BATTERY_CTRL = 37; // HIGH enables divider
static constexpr float BATTERY_DIVIDER_RATIO = 4.90f;
static constexpr float BATTERY_CALIBRATION = 1.0128f;
static constexpr uint8_t BATTERY_SAMPLES = 24;
static constexpr uint8_t BATTERY_TYPE_LIPO_18650 = 0;
static constexpr uint8_t BATTERY_TYPE_SAFT_36V = 1;

// Reserved for future firmware profiles. GPIO40..38 are shared with the V4.3
// GNSS header and may be used for an external RS485 transceiver.
static constexpr uint8_t PIN_RS485_RE_DE = 40;
static constexpr uint8_t PIN_RS485_DI = 39;
static constexpr uint8_t PIN_RS485_RO = 38;

// SenseCAP ONE model identifiers and factory Modbus addresses.
enum SenseCapModel : uint8_t {
  SENSECAP_S200 = 0,
  SENSECAP_S500,
  SENSECAP_S600_A,
  SENSECAP_S700_A,
  SENSECAP_S700_BC,
  SENSECAP_S800,
  SENSECAP_S1000,
  SENSECAP_S1000_C
};

struct SenseCapModelInfo {
  const char* id;
  const char* label;
  uint8_t defaultAddress;
};

static constexpr SenseCapModelInfo SENSECAP_MODELS[] = {
  { "s200", "S200", 44 },
  { "s500", "S500", 10 },
  { "s600-a", "S600-A", 69 },
  { "s700-a", "S700-A", 20 },
  { "s700-bc", "S700-B/C", 60 },
  { "s800", "S800", 46 },
  { "s1000", "S1000", 43 },
  { "s1000-c", "S1000-C", 61 }
};

static constexpr size_t SENSECAP_MODEL_COUNT =
  sizeof(SENSECAP_MODELS) / sizeof(SENSECAP_MODELS[0]);

const SenseCapModelInfo& senseCapModelInfo(uint8_t model) {
  if (model >= SENSECAP_MODEL_COUNT) model = SENSECAP_S700_BC;
  return SENSECAP_MODELS[model];
}

int parseSenseCapModel(const String& value) {
  for (size_t i = 0; i < SENSECAP_MODEL_COUNT; ++i) {
    if (value == SENSECAP_MODELS[i].id) return static_cast<int>(i);
  }
  return -1;
}

bool isSupportedModbusBaud(uint32_t baudRate) {
  switch (baudRate) {
    case 1200:
    case 2400:
    case 4800:
    case 9600:
    case 19200:
    case 38400:
    case 57600:
    case 115200:
      return true;
    default:
      return false;
  }
}

enum LoraFieldId : uint8_t {
  LORA_FIELD_TEMPERATURE = 0,
  LORA_FIELD_POSITION,
  LORA_FIELD_WEG_RAW,
  LORA_FIELD_INA_BUS,
  LORA_FIELD_INA_SHUNT,
  LORA_FIELD_INA_CURRENT,
  LORA_FIELD_INA_POWER,
  LORA_FIELD_BATTERY_VOLTAGE,
  LORA_FIELD_BATTERY_PERCENT,
  LORA_FIELD_AIR_TEMPERATURE,
  LORA_FIELD_HUMIDITY,
  LORA_FIELD_PRESSURE,
  LORA_FIELD_LIGHT,
  LORA_FIELD_MIN_WIND_DIRECTION,
  LORA_FIELD_MAX_WIND_DIRECTION,
  LORA_FIELD_AVERAGE_WIND_DIRECTION,
  LORA_FIELD_MIN_WIND_SPEED,
  LORA_FIELD_MAX_WIND_SPEED,
  LORA_FIELD_AVERAGE_WIND_SPEED,
  LORA_FIELD_ACCUMULATED_RAINFALL,
  LORA_FIELD_RAIN_DURATION,
  LORA_FIELD_RAIN_INTENSITY,
  LORA_FIELD_MAX_RAIN_INTENSITY,
  LORA_FIELD_HEATING_TEMPERATURE,
  LORA_FIELD_TILT,
  LORA_FIELD_PM25,
  LORA_FIELD_PM10,
  LORA_FIELD_CO2,
  LORA_FIELD_NOISE,
  LORA_FIELD_SOLAR_RADIATION,
  LORA_FIELD_SUNSHINE_DURATION,
  LORA_FIELD_COUNT
};

struct LoraFieldInfo {
  const char* id;
  const char* label;
  const char* unit;
  const char* source;
  uint8_t bytes;
};

static constexpr uint32_t LORA_FIELD_MASK_ALL =
  (1UL << LORA_FIELD_COUNT) - 1UL;

static constexpr LoraFieldInfo LORA_FIELDS[LORA_FIELD_COUNT] = {
  { "temperature_c", "Temperature", "C", "local", 2 },
  { "position_mm", "Position", "mm", "local", 2 },
  { "weg_raw", "Wegsensor raw", "raw", "local", 4 },
  { "ina_bus_v", "INA226 bus voltage", "V", "local", 2 },
  { "ina_shunt_mv", "INA226 shunt voltage", "mV", "local", 2 },
  { "ina_current_ma", "INA226 current", "mA", "local", 4 },
  { "ina_power_w", "INA226 power", "W", "local", 4 },
  { "battery_v", "Battery voltage", "V", "local", 2 },
  { "battery_percent", "Battery level", "%", "local", 1 },
  { "air_temperature_c", "Air temperature", "C", "modbus", 2 },
  { "humidity_pct", "Humidity", "%RH", "modbus", 2 },
  { "pressure_pa", "Pressure", "Pa", "modbus", 4 },
  { "light_lux", "Light intensity", "Lux", "modbus", 4 },
  { "min_wind_direction_deg", "Minimum wind direction", "deg", "modbus", 2 },
  { "max_wind_direction_deg", "Maximum wind direction", "deg", "modbus", 2 },
  { "average_wind_direction_deg", "Average wind direction", "deg", "modbus", 2 },
  { "min_wind_speed_ms", "Minimum wind speed", "m/s", "modbus", 2 },
  { "max_wind_speed_ms", "Maximum wind speed", "m/s", "modbus", 2 },
  { "average_wind_speed_ms", "Average wind speed", "m/s", "modbus", 2 },
  { "accumulated_rainfall_mm", "Accumulated rainfall", "mm", "modbus", 4 },
  { "accumulated_rainfall_duration_s", "Rainfall duration", "s", "modbus", 4 },
  { "rain_intensity_mm_h", "Rain intensity", "mm/h", "modbus", 2 },
  { "max_rain_intensity_mm_h", "Maximum rain intensity", "mm/h", "modbus", 2 },
  { "heating_temperature_c", "Heating temperature", "C", "modbus", 2 },
  { "tilt_state", "Tilt state", "", "modbus", 1 },
  { "pm25_ug_m3", "PM2.5", "ug/m3", "modbus", 4 },
  { "pm10_ug_m3", "PM10", "ug/m3", "modbus", 4 },
  { "co2_ppm", "CO2", "ppm", "modbus", 2 },
  { "noise_db", "Noise intensity", "dB", "modbus", 2 },
  { "solar_radiation_wm2", "Global solar radiation", "W/m2", "modbus", 4 },
  { "sunshine_duration_h", "Sunshine duration", "h", "modbus", 4 }
};

uint32_t loraPayloadSize(uint32_t fieldMask) {
  uint32_t size = 6;
  for (uint8_t id = 0; id < LORA_FIELD_COUNT; ++id) {
    if ((fieldMask & (1UL << id)) != 0) size += LORA_FIELDS[id].bytes;
  }
  return size;
}

// ============================ LoRaWAN ================================
// Defaults are overwritten from NVS during setup.
uint8_t devEui[8] = {
  0x63, 0x1E, 0x56, 0x18, 0xDF, 0x8E, 0xB3, 0xD4
};
uint8_t appEui[8] = {
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};
uint8_t appKey[16] = {
  0x63, 0x6A, 0xC0, 0x9B, 0x24, 0x82, 0x4A, 0x47,
  0x30, 0x32, 0x80, 0x58, 0xCE, 0x63, 0x6A, 0xDF
};

uint8_t nwkSKey[16] = { 0 };
uint8_t appSKey[16] = { 0 };
uint32_t devAddr = 0;

uint32_t appTxDutyCycle = 15UL * 60000UL;
bool overTheAirActivation = true;
bool loraWanAdr = true;
bool isTxConfirmed = false;
uint8_t appPort = 2;
uint8_t confirmedNbTrials = 1;

uint16_t userChannelsMask[6] = { 0x00FF, 0, 0, 0, 0, 0 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t loraWanClass = CLASS_A;

extern uint8_t appData[];
extern uint8_t appDataSize;

// ============================ Configuration =========================
struct Config {
  uint8_t devEui[8] = {
    0x63, 0x1E, 0x56, 0x18, 0xDF, 0x8E, 0xB3, 0xD4
  };
  uint8_t appEui[8] = { 0 };
  uint8_t appKey[16] = {
    0x63, 0x6A, 0xC0, 0x9B, 0x24, 0x82, 0x4A, 0x47,
    0x30, 0x32, 0x80, 0x58, 0xCE, 0x63, 0x6A, 0xDF
  };

  uint32_t intervalMinutes = 15;
  bool adr = true;
  uint8_t dataRate = 3;
  bool lowPowerEnabled = true;
  uint8_t batteryType = BATTERY_TYPE_LIPO_18650;
  bool autoRedirectEnabled = false;

  bool modbusEnabled = true;
  uint8_t modbusModel = SENSECAP_S700_BC;
  uint8_t modbusAddress = 60;
  uint32_t modbusBaudRate = 9600;
  uint16_t modbusPollSeconds = 2;
  uint32_t loraFieldMask = LORA_FIELD_MASK_ALL;

  int32_t wegRaw0 = 0;
  int32_t wegRaw1 = 8388607;
  int32_t wegMm0_x1000 = 0;
  int32_t wegMm1_x1000 = 10000;
};

Config cfg;
Preferences preferences;

// ============================ Measurements =========================
struct ModbusMeasurements {
  bool valid = false;
  bool complete = false;

  float airTemperatureC = NAN;
  float humidityPct = NAN;
  float pressurePa = NAN;
  float lightLux = NAN;

  float minWindDirectionDeg = NAN;
  float maxWindDirectionDeg = NAN;
  float averageWindDirectionDeg = NAN;
  float minWindSpeedMs = NAN;
  float maxWindSpeedMs = NAN;
  float averageWindSpeedMs = NAN;

  float accumulatedRainfallMm = NAN;
  float accumulatedRainfallDurationS = NAN;
  float rainIntensityMmH = NAN;
  float maxRainIntensityMmH = NAN;
  float heatingTemperatureC = NAN;
  float tiltState = NAN;

  float pm25UgM3 = NAN;
  float pm10UgM3 = NAN;
  float co2Ppm = NAN;
  float noiseDb = NAN;
  float solarRadiationWm2 = NAN;
  float sunshineDurationH = NAN;
};

void decodeModbusCommon(const uint32_t* values, ModbusMeasurements& output);
void decodeModbusLight(const uint32_t* values, ModbusMeasurements& output);
void decodeModbusWind(const uint32_t* values,
                      ModbusMeasurements& output,
                      uint8_t offset);
void decodeModbusRain(const uint32_t* values, ModbusMeasurements& output);
void decodeModbusParticles(const uint32_t* values, ModbusMeasurements& output);
void decodeModbusSolar(const uint32_t* values, ModbusMeasurements& output);
bool readSenseCapMeasurements(ModbusMeasurements& output);

struct Measurements {
  bool temperatureValid = false;
  float temperatureC = NAN;

  bool wegValid = false;
  int32_t wegRaw = 0;
  float wegVoltageV = NAN;
  float wegPositionMm = NAN;

  bool inaValid = false;
  float inaBusVoltageV = NAN;
  float inaShuntVoltageMv = NAN;
  float inaCurrentMa = NAN;
  float inaPowerW = NAN;

  bool batteryValid = false;
  uint16_t batteryMillivolts = 0;
  float batteryVoltageV = NAN;
  uint8_t batteryPercent = 0;

  ModbusMeasurements modbus;
  uint32_t measuredAtMs = 0;
};

Measurements cached;

// ============================== Objects =============================
SPIClass adsSPI(HSPI);
SPISettings adsSPISettings(500000, MSBFIRST, SPI_MODE1);
HardwareSerial rs485Serial(1);

OneWire oneWire(PIN_DS18B20);
DallasTemperature temperatureSensors(&oneWire);

TwoWire sensorI2C = TwoWire(1);

static constexpr uint8_t OLED_ADDRESS = 0x3C;
SSD1306Wire oled(
  OLED_ADDRESS,
  400000,
  SDA_OLED,
  SCL_OLED,
  GEOMETRY_128_64,
  RST_OLED
);

WebServer server(80);
DNSServer apDnsServer;
bool apDnsReady = false;

bool adsAvailable = false;
bool dsAvailable = false;
bool inaAvailable = false;
bool oledAvailable = false;
bool sensorPowerEnabled = false;
bool sensorInterfacesReady = false;
bool fieldMeasurementPending = false;
bool modbusInterfaceReady = false;
uint32_t lastModbusPollMs = 0;
uint32_t lastModbusSuccessMs = 0;
uint8_t modbusLastException = 0;
String modbusLastRequestHex;
String modbusLastResponseHex;
String modbusLastError;

void powerExternalSensorsOn();
void powerExternalSensorsOff();
void setExternalSensorMosfet(bool enabled);
void prepareLoRaFemForSensorAccess();

// ============================== Helpers =============================
static inline uint32_t intervalMs() {
  const uint32_t minutes = constrain(cfg.intervalMinutes, 1UL, 1440UL);
  return minutes * 60000UL;
}

String bytesToHex(const uint8_t* bytes, size_t count) {
  static constexpr char HEX_DIGITS[] = "0123456789ABCDEF";
  String result;
  result.reserve(count * 2);
  for (size_t i = 0; i < count; ++i) {
    result += HEX_DIGITS[(bytes[i] >> 4) & 0x0F];
    result += HEX_DIGITS[bytes[i] & 0x0F];
  }
  return result;
}

bool parseHex(const String& value, uint8_t* output, size_t outputSize) {
  String clean = value;
  clean.trim();
  clean.replace(" ", "");
  clean.toUpperCase();

  if (clean.length() != outputSize * 2) return false;

  for (size_t i = 0; i < outputSize; ++i) {
    const char high = clean[i * 2];
    const char low = clean[i * 2 + 1];

    auto hexValue = [](char c) -> int {
      if (c >= '0' && c <= '9') return c - '0';
      if (c >= 'A' && c <= 'F') return c - 'A' + 10;
      return -1;
    };

    const int hi = hexValue(high);
    const int lo = hexValue(low);
    if (hi < 0 || lo < 0) return false;
    output[i] = static_cast<uint8_t>((hi << 4) | lo);
  }
  return true;
}

void chipDevEui(uint8_t output[8]) {
  const uint64_t mac = ESP.getEfuseMac();
  uint8_t m[6];
  for (int i = 0; i < 6; ++i) {
    m[i] = static_cast<uint8_t>((mac >> (8 * (5 - i))) & 0xFF);
  }
  output[0] = m[0];
  output[1] = m[1];
  output[2] = m[2];
  output[3] = 0xFF;
  output[4] = 0xFE;
  output[5] = m[3];
  output[6] = m[4];
  output[7] = m[5];
}

String apSsid() {
  const uint64_t mac = ESP.getEfuseMac();
  char suffix[7];
  snprintf(suffix, sizeof(suffix), "%06lX", static_cast<unsigned long>(mac & 0xFFFFFFUL));
  return String("MultiConnect-") + suffix;
}

void copyConfigToLoRaGlobals() {
  memcpy(devEui, cfg.devEui, sizeof(devEui));
  memcpy(appEui, cfg.appEui, sizeof(appEui));
  memcpy(appKey, cfg.appKey, sizeof(appKey));
  loraWanAdr = cfg.adr;
  appTxDutyCycle = intervalMs();
}

// ================================ NVS ===============================
void loadConfig() {
  preferences.begin("multiconnect", true);

  if (preferences.getBytesLength("devEui") == sizeof(cfg.devEui)) {
    preferences.getBytes("devEui", cfg.devEui, sizeof(cfg.devEui));
  }
  if (preferences.getBytesLength("appEui") == sizeof(cfg.appEui)) {
    preferences.getBytes("appEui", cfg.appEui, sizeof(cfg.appEui));
  }
  if (preferences.getBytesLength("appKey") == sizeof(cfg.appKey)) {
    preferences.getBytes("appKey", cfg.appKey, sizeof(cfg.appKey));
  }

  cfg.intervalMinutes = preferences.getUInt("interval", cfg.intervalMinutes);
  cfg.adr = preferences.getBool("adr", cfg.adr);
  cfg.dataRate = preferences.getUChar("dr", cfg.dataRate);
  cfg.lowPowerEnabled = preferences.getBool("lowPower", cfg.lowPowerEnabled);
  cfg.batteryType = preferences.getUChar("batteryType", cfg.batteryType);
  cfg.autoRedirectEnabled = preferences.getBool("autoRedirect", cfg.autoRedirectEnabled);
  cfg.modbusEnabled = preferences.getBool("mbEnabled", cfg.modbusEnabled);
  cfg.modbusModel = preferences.getUChar("mbModel", cfg.modbusModel);
  cfg.modbusAddress = preferences.getUChar("mbAddress", cfg.modbusAddress);
  cfg.modbusBaudRate = preferences.getUInt("mbBaud", cfg.modbusBaudRate);
  cfg.modbusPollSeconds = static_cast<uint16_t>(
    preferences.getUInt("mbPoll", cfg.modbusPollSeconds)
  );
  cfg.loraFieldMask = preferences.getUInt("loraMask", cfg.loraFieldMask);
  cfg.wegRaw0 = preferences.getInt("wegRaw0", cfg.wegRaw0);
  cfg.wegRaw1 = preferences.getInt("wegRaw1", cfg.wegRaw1);
  cfg.wegMm0_x1000 = preferences.getInt("wegMm0", cfg.wegMm0_x1000);
  cfg.wegMm1_x1000 = preferences.getInt("wegMm1", cfg.wegMm1_x1000);

  preferences.end();

  cfg.intervalMinutes = constrain(cfg.intervalMinutes, 1UL, 1440UL);
  cfg.dataRate = constrain(cfg.dataRate, static_cast<uint8_t>(0), static_cast<uint8_t>(5));
  cfg.batteryType = constrain(cfg.batteryType,
                              BATTERY_TYPE_LIPO_18650,
                              BATTERY_TYPE_SAFT_36V);
  if (cfg.modbusModel >= SENSECAP_MODEL_COUNT) cfg.modbusModel = SENSECAP_S700_BC;
  cfg.modbusAddress = constrain(cfg.modbusAddress,
                                static_cast<uint8_t>(1),
                                static_cast<uint8_t>(247));
  if (!isSupportedModbusBaud(cfg.modbusBaudRate)) cfg.modbusBaudRate = 9600;
  cfg.modbusPollSeconds = constrain(cfg.modbusPollSeconds,
                                    static_cast<uint16_t>(2),
                                    static_cast<uint16_t>(3600));
  cfg.loraFieldMask &= LORA_FIELD_MASK_ALL;

  if (cfg.wegRaw0 == cfg.wegRaw1) {
    cfg.wegRaw0 = 0;
    cfg.wegRaw1 = 8388607;
  }
}

void saveConfig() {
  preferences.begin("multiconnect", false);
  preferences.putBytes("devEui", cfg.devEui, sizeof(cfg.devEui));
  preferences.putBytes("appEui", cfg.appEui, sizeof(cfg.appEui));
  preferences.putBytes("appKey", cfg.appKey, sizeof(cfg.appKey));
  preferences.putUInt("interval", cfg.intervalMinutes);
  preferences.putBool("adr", cfg.adr);
  preferences.putUChar("dr", cfg.dataRate);
  preferences.putBool("lowPower", cfg.lowPowerEnabled);
  preferences.putUChar("batteryType", cfg.batteryType);
  preferences.putBool("autoRedirect", cfg.autoRedirectEnabled);
  preferences.putBool("mbEnabled", cfg.modbusEnabled);
  preferences.putUChar("mbModel", cfg.modbusModel);
  preferences.putUChar("mbAddress", cfg.modbusAddress);
  preferences.putUInt("mbBaud", cfg.modbusBaudRate);
  preferences.putUInt("mbPoll", cfg.modbusPollSeconds);
  preferences.putUInt("loraMask", cfg.loraFieldMask);
  preferences.putInt("wegRaw0", cfg.wegRaw0);
  preferences.putInt("wegRaw1", cfg.wegRaw1);
  preferences.putInt("wegMm0", cfg.wegMm0_x1000);
  preferences.putInt("wegMm1", cfg.wegMm1_x1000);
  preferences.end();
}

// ============================== ADS1220 =============================
static constexpr uint8_t ADS_CMD_POWERDOWN = 0x02;
static constexpr uint8_t ADS_CMD_RESET = 0x06;
static constexpr uint8_t ADS_CMD_START = 0x08;
static constexpr uint8_t ADS_CMD_RDATA = 0x10;
static constexpr uint8_t ADS_CMD_RREG = 0x20;
static constexpr uint8_t ADS_CMD_WREG = 0x40;
static constexpr uint8_t ADS_MUX_AIN2_AIN3 = 0x50;
static constexpr uint8_t ADS_AVERAGE_SAMPLES = 10;
static constexpr uint8_t ADS_DISCARD_SAMPLES = 1;

void adsSelect() {
  adsSPI.beginTransaction(adsSPISettings);
  digitalWrite(PIN_ADS_CS, LOW);
  delayMicroseconds(2);
}

void adsDeselect() {
  delayMicroseconds(2);
  digitalWrite(PIN_ADS_CS, HIGH);
  adsSPI.endTransaction();
}

void adsCommand(uint8_t command) {
  adsSelect();
  adsSPI.transfer(command);
  adsDeselect();
}

void adsPowerDown() {
  adsCommand(ADS_CMD_POWERDOWN);
}

void disableAdsInterface() {
  adsSPI.end();

  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);
  pinMode(PIN_ADS_SCLK, OUTPUT);
  digitalWrite(PIN_ADS_SCLK, LOW);
  pinMode(PIN_ADS_MOSI, OUTPUT);
  digitalWrite(PIN_ADS_MOSI, LOW);
  // GPIO5 is also the V4.3 FEM PA_CTX line.
  if (PIN_ADS_MISO != PIN_LORA_FEM_CTX) {
    pinMode(PIN_ADS_MISO, INPUT);
  }
  pinMode(PIN_ADS_DRDY, INPUT);
}

void adsWriteRegister(uint8_t address, uint8_t value) {
  adsSelect();
  adsSPI.transfer(ADS_CMD_WREG | ((address & 0x03) << 2));
  adsSPI.transfer(value);
  adsDeselect();
}

uint8_t adsReadRegister(uint8_t address) {
  adsSelect();
  adsSPI.transfer(ADS_CMD_RREG | ((address & 0x03) << 2));
  const uint8_t value = adsSPI.transfer(0xFF);
  adsDeselect();
  return value;
}

bool waitAdsReady(uint32_t timeoutMs) {
  const uint32_t started = millis();
  while (digitalRead(PIN_ADS_DRDY) == HIGH) {
    if (millis() - started >= timeoutMs) return false;
    delay(1);
  }
  return true;
}

int32_t adsReadRaw() {
  adsSelect();
  adsSPI.transfer(ADS_CMD_RDATA);

  uint32_t raw = 0;
  raw |= static_cast<uint32_t>(adsSPI.transfer(0xFF)) << 16;
  raw |= static_cast<uint32_t>(adsSPI.transfer(0xFF)) << 8;
  raw |= static_cast<uint32_t>(adsSPI.transfer(0xFF));
  adsDeselect();

  if (raw & 0x00800000UL) raw |= 0xFF000000UL;
  return static_cast<int32_t>(raw);
}

bool adsSingleConversion(int32_t& raw) {
  adsCommand(ADS_CMD_START);
  if (!waitAdsReady(500)) return false;
  raw = adsReadRaw();
  return true;
}

void configureWegChannel() {
  // REG0: AIN2-AIN3, gain 1, PGA bypass.
  adsWriteRegister(0, ADS_MUX_AIN2_AIN3 | 0x01);
  // REG1: 20 SPS, normal mode, single-shot.
  adsWriteRegister(1, 0x00);
  // REG2: AVDD-AVSS reference; IDAC disabled.
  adsWriteRegister(2, 0xC0);
  // REG3: IDAC routing disabled.
  adsWriteRegister(3, 0x00);
  delayMicroseconds(100);
}

bool initializeADS1220() {
  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);
  pinMode(PIN_ADS_DRDY, INPUT_PULLUP);

  adsSPI.begin(PIN_ADS_SCLK, PIN_ADS_MISO, PIN_ADS_MOSI, PIN_ADS_CS);
  adsCommand(ADS_CMD_RESET);
  delay(10);

  adsWriteRegister(0, 0x10);
  delay(2);
  const uint8_t readback = adsReadRegister(0);
  Serial.printf("[ADS1220] REG0 readback: 0x%02X\n", readback);

  if (readback != 0x10) {
    disableAdsInterface();
    Serial.println("[ADS1220] not available - continuing without Wegsensor");
    return false;
  }
  configureWegChannel();
  return true;
}

bool readWegsensor(int32_t& raw, float& voltageV, float& positionMm) {
  if (!adsAvailable) return false;

  configureWegChannel();

  for (uint8_t i = 0; i < ADS_DISCARD_SAMPLES; ++i) {
    int32_t dummy = 0;
    if (!adsSingleConversion(dummy)) {
      adsPowerDown();
      adsAvailable = false;
      disableAdsInterface();
      Serial.println("[ADS1220] read timeout - disabling Wegsensor");
      return false;
    }
  }

  int64_t sum = 0;
  for (uint8_t i = 0; i < ADS_AVERAGE_SAMPLES; ++i) {
    int32_t sample = 0;
    if (!adsSingleConversion(sample)) {
      adsPowerDown();
      adsAvailable = false;
      disableAdsInterface();
      Serial.println("[ADS1220] read timeout - disabling Wegsensor");
      return false;
    }
    sum += sample;
  }

  raw = static_cast<int32_t>(sum / ADS_AVERAGE_SAMPLES);
  voltageV = static_cast<float>(raw) * 3.300f / 8388608.0f;

  const int32_t rawDelta = cfg.wegRaw1 - cfg.wegRaw0;
  if (rawDelta == 0) {
    adsPowerDown();
    return false;
  }

  const float mm0 = static_cast<float>(cfg.wegMm0_x1000) / 1000.0f;
  const float mm1 = static_cast<float>(cfg.wegMm1_x1000) / 1000.0f;
  const float ratio = static_cast<float>(raw - cfg.wegRaw0) / static_cast<float>(rawDelta);
  positionMm = mm0 + ratio * (mm1 - mm0);

  const float lower = min(mm0, mm1);
  const float upper = max(mm0, mm1);
  positionMm = constrain(positionMm, lower, upper);
  const bool valid = isfinite(positionMm);
  adsPowerDown();
  return valid;
}

// ============================== DS18B20 =============================
bool initializeDS18B20() {
  // GPIO47 is put into a passive state when the switched sensor rail is off.
  // Reinitialize OneWire after every power cycle before searching for devices.
  oneWire.begin(PIN_DS18B20);
  temperatureSensors.begin();
  const uint8_t count = temperatureSensors.getDeviceCount();
  Serial.printf("[DS18B20] devices: %u\n", count);
  if (count == 0) return false;

  // 10-bit conversion is faster and reduces active time.
  temperatureSensors.setResolution(10);
  return true;
}

bool readTemperature(float& temperatureC) {
  if (!dsAvailable) return false;
  temperatureSensors.requestTemperatures();
  temperatureC = temperatureSensors.getTempCByIndex(0);
  return temperatureC != DEVICE_DISCONNECTED_C && isfinite(temperatureC);
}

// =============================== INA226 =============================
static constexpr uint8_t INA226_ADDRESS = 0x40;
static constexpr uint8_t INA226_REG_CONFIG = 0x00;
static constexpr uint8_t INA226_REG_SHUNT = 0x01;
static constexpr uint8_t INA226_REG_BUS = 0x02;
static constexpr uint8_t INA226_REG_MANUFACTURER = 0xFE;
static constexpr uint8_t INA226_REG_DIE_ID = 0xFF;
static constexpr float INA226_SHUNT_OHMS = 0.010f;  // R010 = 10 mOhm
static constexpr uint32_t INA226_REPROBE_INTERVAL_MS = 5000;

uint32_t lastInaProbeMs = 0;

bool inaReadRegister(uint8_t reg, uint16_t& value) {
  sensorI2C.beginTransmission(INA226_ADDRESS);
  sensorI2C.write(reg);
  if (sensorI2C.endTransmission(false) != 0) return false;

  if (sensorI2C.requestFrom(INA226_ADDRESS, static_cast<uint8_t>(2)) != 2) return false;
  value = static_cast<uint16_t>(sensorI2C.read()) << 8;
  value |= static_cast<uint16_t>(sensorI2C.read());
  return true;
}

bool inaWriteRegister(uint8_t reg, uint16_t value) {
  sensorI2C.beginTransmission(INA226_ADDRESS);
  sensorI2C.write(reg);
  sensorI2C.write(static_cast<uint8_t>(value >> 8));
  sensorI2C.write(static_cast<uint8_t>(value & 0xFF));
  return sensorI2C.endTransmission() == 0;
}

bool probeINA226(bool printStatus = true) {
  lastInaProbeMs = millis();

  uint16_t manufacturer = 0;
  uint16_t dieId = 0;
  const bool present =
    inaReadRegister(INA226_REG_MANUFACTURER, manufacturer) &&
    inaReadRegister(INA226_REG_DIE_ID, dieId) &&
    manufacturer == 0x5449;

  if (present) {
    // Continuous shunt + bus conversion. The module is optional; this is
    // applied whenever it appears on the bus, including hot-plug in AP mode.
    inaWriteRegister(INA226_REG_CONFIG, 0x4127);
    if (printStatus) {
      Serial.printf("[INA226] connected manufacturer=0x%04X die=0x%04X\n",
                    manufacturer, dieId);
    }
  } else if (printStatus) {
    Serial.println("[INA226] not connected - continuing without current monitor");
  }

  inaAvailable = present;
  return present;
}

bool initializeINA226() {
  sensorI2C.begin(PIN_INA_SDA, PIN_INA_SCL, 100000);
  sensorI2C.setTimeOut(30);
  return probeINA226(true);
}

bool ensureINA226Available() {
  if (inaAvailable) return true;
  if (millis() - lastInaProbeMs < INA226_REPROBE_INTERVAL_MS) return false;
  return probeINA226(false);
}

bool readINA226(float& busVoltageV, float& shuntVoltageMv, float& currentMa, float& powerW) {
  if (!ensureINA226Available()) return false;

  uint16_t rawBus = 0;
  uint16_t rawShunt = 0;
  if (!inaReadRegister(INA226_REG_BUS, rawBus) ||
      !inaReadRegister(INA226_REG_SHUNT, rawShunt)) {
    inaAvailable = false;
    Serial.println("[INA226] disconnected during read");
    return false;
  }

  busVoltageV = static_cast<float>(rawBus) * 0.00125f;
  shuntVoltageMv = static_cast<float>(static_cast<int16_t>(rawShunt)) * 0.0025f;
  const float shuntVoltageV = shuntVoltageMv / 1000.0f;
  const float currentA = shuntVoltageV / INA226_SHUNT_OHMS;
  currentMa = currentA * 1000.0f;
  powerW = busVoltageV * currentA;
  return true;
}

// ======================== Internal battery ADC ======================
uint8_t batteryPercentFromMillivolts(uint16_t millivolts) {
  struct Point {
    uint16_t mv;
    uint8_t percent;
  };

  static constexpr Point lipo18650Curve[] = {
    { 3200,   0 },
    { 3300,   5 },
    { 3500,  10 },
    { 3600,  20 },
    { 3700,  35 },
    { 3750,  50 },
    { 3800,  60 },
    { 3900,  75 },
    { 4000,  85 },
    { 4100,  95 },
    { 4200, 100 }
  };

  static constexpr Point saft36vCurve[] = {
    { 2800,   0 },
    { 3000,   5 },
    { 3200,  15 },
    { 3300,  25 },
    { 3400,  40 },
    { 3500,  60 },
    { 3550,  75 },
    { 3600,  95 },
    { 3650, 100 }
  };

  const Point* curve = cfg.batteryType == BATTERY_TYPE_SAFT_36V
    ? saft36vCurve
    : lipo18650Curve;
  const size_t count = cfg.batteryType == BATTERY_TYPE_SAFT_36V
    ? sizeof(saft36vCurve) / sizeof(saft36vCurve[0])
    : sizeof(lipo18650Curve) / sizeof(lipo18650Curve[0]);

  if (millivolts <= curve[0].mv) return curve[0].percent;
  if (millivolts >= curve[count - 1].mv) return curve[count - 1].percent;

  for (size_t i = 1; i < count; ++i) {
    if (millivolts <= curve[i].mv) {
      const uint16_t x0 = curve[i - 1].mv;
      const uint16_t x1 = curve[i].mv;
      const uint8_t y0 = curve[i - 1].percent;
      const uint8_t y1 = curve[i].percent;
      return static_cast<uint8_t>(
        y0 + ((static_cast<uint32_t>(millivolts - x0) * (y1 - y0)) / (x1 - x0))
      );
    }
  }

  return 0;
}

void initializeBatteryMeasurement() {
  analogReadResolution(12);
  analogSetPinAttenuation(PIN_BATTERY_ADC, ADC_11db);

  pinMode(PIN_BATTERY_CTRL, OUTPUT);
  digitalWrite(PIN_BATTERY_CTRL, LOW); // divider disabled between readings
}

bool readInternalBattery(uint16_t& batteryMillivolts,
                         float& batteryVoltageV,
                         uint8_t& batteryPercent) {
  digitalWrite(PIN_BATTERY_CTRL, HIGH);
  delay(8);

  // Discard the first reading after enabling the divider.
  (void)analogReadMilliVolts(PIN_BATTERY_ADC);
  delayMicroseconds(250);

  uint32_t adcMillivoltSum = 0;
  for (uint8_t i = 0; i < BATTERY_SAMPLES; ++i) {
    adcMillivoltSum += static_cast<uint32_t>(
      analogReadMilliVolts(PIN_BATTERY_ADC)
    );
    delayMicroseconds(250);
  }

  digitalWrite(PIN_BATTERY_CTRL, LOW);

  const float adcMillivolts =
    static_cast<float>(adcMillivoltSum) /
    static_cast<float>(BATTERY_SAMPLES);

  const float calculatedBatteryMv =
    adcMillivolts *
    BATTERY_DIVIDER_RATIO *
    BATTERY_CALIBRATION;

  if (!isfinite(calculatedBatteryMv) ||
      calculatedBatteryMv < 500.0f ||
      calculatedBatteryMv > 6000.0f) {
    return false;
  }

  batteryMillivolts = static_cast<uint16_t>(
    constrain(lroundf(calculatedBatteryMv), 0L, 65534L)
  );
  batteryVoltageV = static_cast<float>(batteryMillivolts) / 1000.0f;
  batteryPercent = batteryPercentFromMillivolts(batteryMillivolts);
  return true;
}

// ============================== Modbus =============================
static constexpr uint8_t MODBUS_READ_INPUT_REGISTERS = 0x04;
static constexpr uint16_t MODBUS_MAX_REGISTERS = 32;
static constexpr size_t MODBUS_RX_BUFFER_SIZE = 128;
static constexpr uint32_t MODBUS_POWERUP_SETTLE_MS = 10000UL;
static constexpr uint8_t MODBUS_DATA_READY_RETRIES = 3;
static constexpr uint32_t MODBUS_DATA_READY_RETRY_DELAY_MS = 1000UL;
static constexpr size_t MODBUS_FRAME_MAX_SIZE =
  5 + MODBUS_MAX_REGISTERS * 2;

uint16_t modbusCrc16(const uint8_t* data, size_t length) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < length; ++i) {
    crc ^= data[i];
    for (uint8_t bit = 0; bit < 8; ++bit) {
      crc = (crc & 0x0001U) != 0
        ? static_cast<uint16_t>((crc >> 1) ^ 0xA001U)
        : static_cast<uint16_t>(crc >> 1);
    }
  }
  return crc;
}

void setModbusError(const char* message) {
  modbusLastError = message;
}

uint32_t modbusResponseTimeoutMs(uint16_t quantity) {
  (void)quantity;
  return 2000UL;
}

bool initializeModbusInterface() {
  rs485Serial.end();

  pinMode(PIN_RS485_RE_DE, OUTPUT);
  digitalWrite(PIN_RS485_RE_DE, LOW);
  pinMode(PIN_RS485_DI, OUTPUT);
  digitalWrite(PIN_RS485_DI, LOW);
  pinMode(PIN_RS485_RO, INPUT);

  if (cfg.modbusEnabled) {
    rs485Serial.begin(cfg.modbusBaudRate,
                      SERIAL_8N1,
                      PIN_RS485_RO,
                      PIN_RS485_DI);
    rs485Serial.setTimeout(20);
    while (rs485Serial.available() > 0) (void)rs485Serial.read();
  }

  modbusInterfaceReady = true;
  return true;
}

void disableModbusInterface() {
  digitalWrite(PIN_RS485_RE_DE, LOW);
  rs485Serial.end();
  modbusInterfaceReady = false;

  pinMode(PIN_RS485_DI, OUTPUT);
  digitalWrite(PIN_RS485_DI, LOW);
  pinMode(PIN_RS485_RO, INPUT);
}

bool modbusReadInputRegisters(uint16_t startAddress,
                              uint16_t quantity,
                              uint16_t* values) {
  if (!cfg.modbusEnabled) {
    setModbusError("disabled");
    return false;
  }
  if (!modbusInterfaceReady) {
    setModbusError("interface not ready");
    return false;
  }
  if (quantity == 0 || quantity > MODBUS_MAX_REGISTERS) {
    setModbusError("invalid register quantity");
    return false;
  }

  uint8_t request[8] = {
    cfg.modbusAddress,
    MODBUS_READ_INPUT_REGISTERS,
    static_cast<uint8_t>(startAddress >> 8),
    static_cast<uint8_t>(startAddress & 0xFF),
    static_cast<uint8_t>(quantity >> 8),
    static_cast<uint8_t>(quantity & 0xFF),
    0,
    0
  };
  const uint16_t requestCrc = modbusCrc16(request, 6);
  request[6] = static_cast<uint8_t>(requestCrc & 0xFF);
  request[7] = static_cast<uint8_t>(requestCrc >> 8);

  modbusLastRequestHex = bytesToHex(request, sizeof(request));
  modbusLastResponseHex = "";
  modbusLastException = 0;
  Serial.printf("[MODBUS] TX slave=%u start=0x%04X qty=%u: %s\n",
                cfg.modbusAddress,
                startAddress,
                quantity,
                modbusLastRequestHex.c_str());

  while (rs485Serial.available() > 0) (void)rs485Serial.read();

  digitalWrite(PIN_RS485_RE_DE, HIGH);
  rs485Serial.write(request, sizeof(request));
  rs485Serial.flush();
  delayMicroseconds(20);
  digitalWrite(PIN_RS485_RE_DE, LOW);

  uint8_t receiveBuffer[MODBUS_RX_BUFFER_SIZE] = { 0 };
  size_t receiveLength = 0;
  uint8_t frame[MODBUS_FRAME_MAX_SIZE] = { 0 };
  size_t frameLength = 0;
  bool validFrameFound = false;
  const uint32_t started = millis();
  const uint32_t timeoutMs = modbusResponseTimeoutMs(quantity);

  while (millis() - started < timeoutMs) {
    while (rs485Serial.available() > 0) {
      if (receiveLength >= sizeof(receiveBuffer)) {
        const size_t keep = sizeof(receiveBuffer) / 2;
        for (size_t i = 0; i < receiveLength - keep; ++i) {
          receiveBuffer[i] = receiveBuffer[i + keep];
        }
        receiveLength -= keep;
      }
      receiveBuffer[receiveLength++] = static_cast<uint8_t>(rs485Serial.read());

      // Search the complete buffer so an echoed request or leading noise does
      // not prevent recognition of the real Modbus response.
      for (size_t position = 0; position + 2 < receiveLength; ++position) {
        if (receiveBuffer[position] != cfg.modbusAddress) continue;

        const uint8_t function = receiveBuffer[position + 1];
        size_t candidateLength = 0;
        if (function == (MODBUS_READ_INPUT_REGISTERS | 0x80U)) {
          candidateLength = 5;
        } else if (function == MODBUS_READ_INPUT_REGISTERS) {
          const uint8_t byteCount = receiveBuffer[position + 2];
          if (byteCount > MODBUS_MAX_REGISTERS * 2) continue;
          candidateLength = 5 + byteCount;
        } else {
          continue;
        }

        if (position + candidateLength > receiveLength) continue;
        if (modbusCrc16(receiveBuffer + position, candidateLength - 2) !=
            (static_cast<uint16_t>(receiveBuffer[position + candidateLength - 2]) |
             static_cast<uint16_t>(receiveBuffer[position + candidateLength - 1] << 8))) {
          continue;
        }

        for (size_t i = 0; i < candidateLength; ++i) {
          frame[i] = receiveBuffer[position + i];
        }
        frameLength = candidateLength;
        validFrameFound = true;
        break;
      }

      if (validFrameFound) break;
    }

    if (validFrameFound) break;
    delay(1);
  }

  if (validFrameFound) {
    modbusLastResponseHex = bytesToHex(frame, frameLength);
  } else {
    modbusLastResponseHex = bytesToHex(receiveBuffer, receiveLength);
  }
  Serial.printf("[MODBUS] RX: %s\n",
                modbusLastResponseHex.length() > 0
                  ? modbusLastResponseHex.c_str()
                  : "<none>");

  if (!validFrameFound) {
    setModbusError("no response or timeout");
    return false;
  }
  const bool exceptionResponse = (frame[1] & 0x80U) != 0;
  if (exceptionResponse) {
    modbusLastException = frame[2];
    char error[32];
    snprintf(error, sizeof(error), "Modbus exception 0x%02X", modbusLastException);
    setModbusError(error);
    return false;
  }
  if (frame[1] != MODBUS_READ_INPUT_REGISTERS) {
    setModbusError("unexpected function code");
    return false;
  }
  if (frame[2] != quantity * 2) {
    setModbusError("unexpected byte count");
    return false;
  }

  for (uint16_t i = 0; i < quantity; ++i) {
    values[i] = static_cast<uint16_t>(frame[3 + i * 2]) << 8;
    values[i] |= static_cast<uint16_t>(frame[4 + i * 2]);
  }
  return true;
}

bool readModbusBlock(uint16_t startAddress,
                     uint16_t quantity,
                     uint32_t* values) {
  if ((quantity & 1U) != 0 || quantity > MODBUS_MAX_REGISTERS) {
    setModbusError("invalid 32-bit register block");
    return false;
  }

  uint16_t registers[MODBUS_MAX_REGISTERS] = { 0 };
  if (!modbusReadInputRegisters(startAddress, quantity, registers)) return false;

  for (uint16_t i = 0; i < quantity / 2; ++i) {
    values[i] = static_cast<uint32_t>(registers[i * 2]) << 16;
    values[i] |= static_cast<uint32_t>(registers[i * 2 + 1]);
  }
  return true;
}

bool senseCapCommonDataIsPlausible(const uint32_t* values) {
  const int32_t temperatureRaw = static_cast<int32_t>(values[0]);
  const uint32_t humidityRaw = values[1];
  const uint32_t pressureRaw = values[2];

  return temperatureRaw >= -40000L && temperatureRaw <= 85000L &&
         humidityRaw <= 100000UL &&
         pressureRaw >= 30000000UL && pressureRaw <= 125000000UL;
}

bool readSenseCapCommonBlock(uint16_t quantity, uint32_t* values) {
  for (uint8_t attempt = 0; attempt < MODBUS_DATA_READY_RETRIES; ++attempt) {
    if (!readModbusBlock(0x0000, quantity, values)) {
      Serial.printf("[MODBUS] common read failed, retry %u/%u\n",
                    static_cast<unsigned>(attempt + 1),
                    static_cast<unsigned>(MODBUS_DATA_READY_RETRIES));
      if (attempt + 1 < MODBUS_DATA_READY_RETRIES) {
        delay(MODBUS_DATA_READY_RETRY_DELAY_MS);
        continue;
      }
      return false;
    }

    if (senseCapCommonDataIsPlausible(values)) return true;

    Serial.printf("[MODBUS] common data not ready: temp_raw=%ld humidity_raw=%lu pressure_raw=%lu attempt=%u/%u\n",
                  static_cast<long>(static_cast<int32_t>(values[0])),
                  static_cast<unsigned long>(values[1]),
                  static_cast<unsigned long>(values[2]),
                  static_cast<unsigned>(attempt + 1),
                  static_cast<unsigned>(MODBUS_DATA_READY_RETRIES));
    if (attempt + 1 < MODBUS_DATA_READY_RETRIES) {
      delay(MODBUS_DATA_READY_RETRY_DELAY_MS);
    }
  }

  setModbusError("SenseCAP data not ready or invalid");
  return false;
}

void decodeModbusCommon(const uint32_t* values, ModbusMeasurements& output) {
  output.airTemperatureC = static_cast<int32_t>(values[0]) / 1000.0f;
  output.humidityPct = static_cast<float>(values[1]) / 1000.0f;
  output.pressurePa = static_cast<float>(values[2]) / 1000.0f;
}

void decodeModbusLight(const uint32_t* values, ModbusMeasurements& output) {
  output.lightLux = static_cast<float>(values[0]) / 1000.0f;
}

void decodeModbusWind(const uint32_t* values,
                      ModbusMeasurements& output,
                      uint8_t offset = 0) {
  output.minWindDirectionDeg = static_cast<float>(values[offset]) / 1000.0f;
  output.maxWindDirectionDeg = static_cast<float>(values[offset + 1]) / 1000.0f;
  output.averageWindDirectionDeg = static_cast<float>(values[offset + 2]) / 1000.0f;
  output.minWindSpeedMs = static_cast<float>(values[offset + 3]) / 1000.0f;
  output.maxWindSpeedMs = static_cast<float>(values[offset + 4]) / 1000.0f;
  output.averageWindSpeedMs = static_cast<float>(values[offset + 5]) / 1000.0f;
}

void decodeModbusRain(const uint32_t* values, ModbusMeasurements& output) {
  output.accumulatedRainfallMm = static_cast<float>(values[0]) / 1000.0f;
  output.accumulatedRainfallDurationS = static_cast<float>(values[1]) / 1000.0f;
  output.rainIntensityMmH = static_cast<float>(values[2]) / 1000.0f;
  output.maxRainIntensityMmH = static_cast<float>(values[3]) / 1000.0f;
  output.heatingTemperatureC = static_cast<int32_t>(values[4]) / 1000.0f;
  output.tiltState = static_cast<float>(values[5]) / 1000.0f;
}

void decodeModbusParticles(const uint32_t* values, ModbusMeasurements& output) {
  output.pm25UgM3 = static_cast<float>(values[0]) / 1000.0f;
  output.pm10UgM3 = static_cast<float>(values[1]) / 1000.0f;
}

void decodeModbusSolar(const uint32_t* values, ModbusMeasurements& output) {
  output.solarRadiationWm2 = static_cast<float>(values[0]) / 1000.0f;
  output.sunshineDurationH = static_cast<float>(values[1]) / 1000.0f;
}

bool readSenseCapMeasurements(ModbusMeasurements& output) {
  output = ModbusMeasurements();
  lastModbusPollMs = millis();
  modbusLastError = "";

  if (!cfg.modbusEnabled) {
    setModbusError("disabled");
    return false;
  }
  if (!modbusInterfaceReady) {
    setModbusError("interface not ready");
    return false;
  }

  uint32_t values[MODBUS_MAX_REGISTERS] = { 0 };
  bool anyBlockValid = false;
  bool primaryBlockValid = false;
  bool allBlocksValid = true;

  switch (cfg.modbusModel) {
    case SENSECAP_S200:
      if (readModbusBlock(0x0008, 12, values)) {
        decodeModbusWind(values, output);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    case SENSECAP_S500:
      if (readSenseCapCommonBlock(6, values)) {
        decodeModbusCommon(values, output);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      if (readModbusBlock(0x0008, 12, values)) {
        decodeModbusWind(values, output);
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    case SENSECAP_S600_A:
      if (readSenseCapCommonBlock(20, values)) {
        decodeModbusCommon(values, output);
        decodeModbusLight(values + 3, output);
        decodeModbusWind(values, output, 4);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    case SENSECAP_S700_A:
      if (readSenseCapCommonBlock(32, values)) {
        decodeModbusCommon(values, output);
        decodeModbusLight(values + 3, output);
        decodeModbusWind(values, output, 4);
        decodeModbusRain(values + 10, output);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    case SENSECAP_S700_BC:
      if (readSenseCapCommonBlock(32, values)) {
        decodeModbusCommon(values, output);
        decodeModbusLight(values + 3, output);
        decodeModbusWind(values, output, 4);
        decodeModbusRain(values + 10, output);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      delay(50);
      if (readModbusBlock(0x004A, 4, values)) {
        decodeModbusSolar(values, output);
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    case SENSECAP_S800:
      if (readSenseCapCommonBlock(6, values)) {
        decodeModbusCommon(values, output);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      if (readModbusBlock(0x0008, 12, values)) {
        decodeModbusWind(values, output);
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      if (readModbusBlock(0x0030, 4, values)) {
        decodeModbusParticles(values, output);
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      if (readModbusBlock(0x0048, 2, values)) {
        output.noiseDb = static_cast<float>(values[0]) / 1000.0f;
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    case SENSECAP_S1000:
    case SENSECAP_S1000_C:
      if (readSenseCapCommonBlock(32, values)) {
        decodeModbusCommon(values, output);
        decodeModbusLight(values + 3, output);
        decodeModbusWind(values, output, 4);
        decodeModbusRain(values + 10, output);
        anyBlockValid = true;
        primaryBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      if (readModbusBlock(0x0030, 4, values)) {
        decodeModbusParticles(values, output);
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      if (readModbusBlock(0x0040, 2, values)) {
        output.co2Ppm = static_cast<float>(values[0]) / 1000.0f;
        anyBlockValid = true;
      } else {
        allBlocksValid = false;
      }
      break;

    default:
      setModbusError("invalid model");
      allBlocksValid = false;
      break;
  }

  output.valid = primaryBlockValid;
  output.complete = primaryBlockValid && allBlocksValid;
  if (primaryBlockValid) lastModbusSuccessMs = millis();
  if (!primaryBlockValid && modbusLastError.length() == 0) {
    setModbusError("no valid register block");
  }
  return primaryBlockValid;
}

bool modbusPollDue() {
  if (!cfg.modbusEnabled) return false;
  if (lastModbusPollMs == 0) return true;
  return millis() - lastModbusPollMs >=
    static_cast<uint32_t>(cfg.modbusPollSeconds) * 1000UL;
}

void initializePoweredSensorInterfaces() {
  dsAvailable = initializeDS18B20();
  inaAvailable = initializeINA226();
  adsAvailable = initializeADS1220();
  modbusInterfaceReady = initializeModbusInterface();
  sensorInterfacesReady = true;
}

void setExternalSensorMosfet(bool enabled) {
  pinMode(PIN_SENSOR_MOSFET, OUTPUT);
  digitalWrite(PIN_SENSOR_MOSFET,
               enabled ? SENSOR_MOSFET_ON_LEVEL : SENSOR_MOSFET_OFF_LEVEL);
  sensorPowerEnabled = enabled;
}

void powerExternalSensorsOn() {
  if (!sensorPowerEnabled) {
    prepareLoRaFemForSensorAccess();
    setExternalSensorMosfet(true);
    delay(50);
    sensorInterfacesReady = false;
    if (cfg.modbusEnabled) {
      Serial.printf("[MODBUS] sensor startup settle: %lu ms\n",
                    static_cast<unsigned long>(MODBUS_POWERUP_SETTLE_MS));
      delay(MODBUS_POWERUP_SETTLE_MS);
    }
    Serial.println("[SENSORS] external power ON");
  }

  if (!sensorInterfacesReady) initializePoweredSensorInterfaces();
}

// =========================== Sensor cache ===========================
void readAllSensors() {
  powerExternalSensorsOn();

  Measurements next;

  next.temperatureValid = readTemperature(next.temperatureC);
  next.wegValid = readWegsensor(next.wegRaw, next.wegVoltageV, next.wegPositionMm);
  next.inaValid = readINA226(next.inaBusVoltageV, next.inaShuntVoltageMv,
                                next.inaCurrentMa, next.inaPowerW);
  if (!cfg.modbusEnabled) {
    next.modbus = ModbusMeasurements();
  } else if (modbusPollDue()) {
    (void)readSenseCapMeasurements(next.modbus);
  } else {
    next.modbus = cached.modbus;
  }
  next.batteryValid = readInternalBattery(next.batteryMillivolts,
                                           next.batteryVoltageV,
                                           next.batteryPercent);
  next.measuredAtMs = millis();

  cached = next;

  Serial.println("[SENSORS] cache updated");
  if (cached.temperatureValid) Serial.printf("  temperature: %.2f C\n", cached.temperatureC);
  if (cached.wegValid) Serial.printf("  position: %.3f mm raw=%ld voltage=%.6f V\n",
                                     cached.wegPositionMm,
                                     static_cast<long>(cached.wegRaw),
                                     cached.wegVoltageV);
  if (cached.inaValid) Serial.printf("  INA226: %.4f V / %.4f mV / %.2f mA / %.4f W\n",
                                     cached.inaBusVoltageV,
                                     cached.inaShuntVoltageMv,
                                     cached.inaCurrentMa,
                                     cached.inaPowerW);
  if (cached.modbus.valid) {
    Serial.printf("  Modbus %s: %.2f C / %.2f %%RH / %.2f Pa / %.2f m/s\n",
                  senseCapModelInfo(cfg.modbusModel).label,
                  cached.modbus.airTemperatureC,
                  cached.modbus.humidityPct,
                  cached.modbus.pressurePa,
                  cached.modbus.averageWindSpeedMs);
  } else if (cfg.modbusEnabled) {
    Serial.printf("  Modbus: %s\n", modbusLastError.c_str());
  }
  if (cached.batteryValid) Serial.printf("  battery: %u mV / %.3f V / %u %%\n",
                                         cached.batteryMillivolts,
                                         cached.batteryVoltageV,
                                         cached.batteryPercent);
}

// ================================ OLED ==============================
void oledPowerOn() {
  pinMode(Vext, OUTPUT);
  digitalWrite(Vext, LOW);  // Heltec Vext active LOW
  delay(50);

  oled.init();
  oled.clear();
  oled.setColor(WHITE);
  oled.setFont(ArialMT_Plain_10);
  oledAvailable = true;
}

void oledPowerOff() {
  if (oledAvailable) {
    oled.clear();
    oled.display();
    oled.displayOff();
  }
  pinMode(Vext, OUTPUT);
  digitalWrite(Vext, HIGH);
  oledAvailable = false;
}

void oledSplash() {
  if (!oledAvailable) return;
  oled.clear();
  oled.setTextAlignment(TEXT_ALIGN_CENTER);
  oled.setFont(ArialMT_Plain_16);
  oled.drawString(64, 6, "MKP");
  oled.setFont(ArialMT_Plain_10);
  oled.drawString(64, 27, "MultiConnect");
  oled.drawString(64, 43, String("FW ") + FW_VERSION);
  oled.display();
}

void oledShowBootMode(bool selectedApMode) {
  if (!oledAvailable) return;
  oled.clear();
  oled.setTextAlignment(TEXT_ALIGN_CENTER);
  oled.setFont(ArialMT_Plain_10);
  oled.drawString(64, 5, "MKP MultiConnect");
  oled.setFont(ArialMT_Plain_16);
  oled.drawString(64, 23, selectedApMode ? "AP MODE" : "FIELD MODE");
  oled.setFont(ArialMT_Plain_10);
  oled.drawString(64, 47, selectedApMode ? "GPIO48 = HIGH" : "GPIO48 = GND");
  oled.display();
}

void oledShowApInfo(const String& ssid) {
  if (!oledAvailable) return;
  oled.clear();
  oled.setTextAlignment(TEXT_ALIGN_LEFT);
  oled.setFont(ArialMT_Plain_10);
  oled.drawString(0, 0, "AP CONFIG MODE");
  oled.drawString(0, 16, "SSID:");
  oled.drawString(0, 28, ssid);
  oled.drawString(0, 44, "IP: " + WiFi.softAPIP().toString());
  oled.display();
}

void oledShowSensors() {
  if (!oledAvailable) return;
  oled.clear();
  oled.setTextAlignment(TEXT_ALIGN_LEFT);
  oled.setFont(ArialMT_Plain_10);
  oled.drawString(0, 0, "MKP MultiConnect");
  oled.drawString(0, 15, cached.temperatureValid
    ? String("Temp  ") + String(cached.temperatureC, 1) + " C"
    : "Temp  N/C");
  oled.drawString(0, 29, cached.wegValid
    ? String("Weg   ") + String(cached.wegPositionMm, 3) + " mm"
    : "Weg   N/C");
  oled.drawString(0, 43, cached.batteryValid
    ? String("Bat   ") + String(cached.batteryVoltageV, 2) + " V  " + String(cached.batteryPercent) + "%"
    : "Bat   N/C");
  oled.display();
}

// ============================== Web API =============================
bool streamLittleFsFile(const char* path, const char* contentType) {
  if (!littleFsReady || !LittleFS.exists(path)) return false;
  File file = LittleFS.open(path, "r");
  if (!file) return false;
  server.streamFile(file, contentType);
  file.close();
  return true;
}

void sendJson(const JsonDocument& doc, int status = 200) {
  String output;
  serializeJson(doc, output);
  server.send(status, "application/json", output);
}

void apiState() {
  JsonDocument doc;
  doc["name"] = DEVICE_NAME;
  doc["fw"] = FW_VERSION;
  doc["mode"] = apMode ? "AP" : "FIELD";
  doc["mode_pin"] = PIN_AP_MODE;
  doc["mode_pin_high"] = apMode;
  doc["sensor_power_enabled"] = sensorPowerEnabled;
  doc["sensor_power_pin"] = PIN_SENSOR_MOSFET;
  doc["ip"] = apMode ? WiFi.softAPIP().toString() : String("-");
  doc["uptime_s"] = millis() / 1000UL;
  doc["heap_free"] = ESP.getFreeHeap();
  doc["measurement_age_s"] = cached.measuredAtMs > 0 ? (millis() - cached.measuredAtMs) / 1000UL : 0;

  doc["temperature_valid"] = cached.temperatureValid;
  if (cached.temperatureValid) doc["temperature_c"] = cached.temperatureC;

  doc["weg_valid"] = cached.wegValid;
  doc["weg_raw"] = cached.wegRaw;
  if (cached.wegValid) {
    doc["weg_voltage_v"] = cached.wegVoltageV;
    doc["position_mm"] = cached.wegPositionMm;
  }

  doc["ina_connected"] = inaAvailable;
  doc["ina_valid"] = cached.inaValid;
  if (cached.inaValid) {
    doc["ina_bus_v"] = cached.inaBusVoltageV;
    doc["ina_shunt_mv"] = cached.inaShuntVoltageMv;
    doc["ina_current_ma"] = cached.inaCurrentMa;
    doc["ina_power_w"] = cached.inaPowerW;
  }

  doc["battery_valid"] = cached.batteryValid;
  if (cached.batteryValid) {
    doc["battery_mv"] = cached.batteryMillivolts;
    doc["battery_v"] = cached.batteryVoltageV;
    doc["battery_percent"] = cached.batteryPercent;
  }

  JsonObject modbusState = doc["modbus"].to<JsonObject>();
  modbusState["enabled"] = cfg.modbusEnabled;
  modbusState["interface_ready"] = modbusInterfaceReady;
  modbusState["connected"] = sensorPowerEnabled && cached.modbus.valid;
  modbusState["complete"] = cached.modbus.complete;
  modbusState["model"] = senseCapModelInfo(cfg.modbusModel).id;
  modbusState["model_label"] = senseCapModelInfo(cfg.modbusModel).label;
  modbusState["factory_address"] = senseCapModelInfo(cfg.modbusModel).defaultAddress;
  modbusState["address"] = cfg.modbusAddress;
  modbusState["baud"] = cfg.modbusBaudRate;
  modbusState["poll_seconds"] = cfg.modbusPollSeconds;
  modbusState["protocol"] = "RS-485 Modbus RTU";
  modbusState["function_code"] = MODBUS_READ_INPUT_REGISTERS;
  modbusState["format"] = "8N1";
  modbusState["re_de_pin"] = PIN_RS485_RE_DE;
  modbusState["tx_pin"] = PIN_RS485_DI;
  modbusState["rx_pin"] = PIN_RS485_RO;
  modbusState["cable"] = "A pin 1 white | B pin 7 blue | +12-24 V pin 8 red | 0 V pin 2 brown";
  modbusState["last_error"] = modbusLastError;
  modbusState["last_exception"] = modbusLastException;
  modbusState["last_request"] = modbusLastRequestHex;
  modbusState["last_response"] = modbusLastResponseHex;
  modbusState["last_attempt_age_s"] = lastModbusPollMs > 0
    ? (millis() - lastModbusPollMs) / 1000UL
    : 0;
  modbusState["last_success_age_s"] = lastModbusSuccessMs > 0
    ? (millis() - lastModbusSuccessMs) / 1000UL
    : 0;

  JsonObject modbusData = modbusState["data"].to<JsonObject>();
  if (isfinite(cached.modbus.airTemperatureC)) {
    modbusData["air_temperature_c"] = cached.modbus.airTemperatureC;
  }
  if (isfinite(cached.modbus.humidityPct)) {
    modbusData["humidity_pct"] = cached.modbus.humidityPct;
  }
  if (isfinite(cached.modbus.pressurePa)) {
    modbusData["pressure_pa"] = cached.modbus.pressurePa;
  }
  if (isfinite(cached.modbus.lightLux)) {
    modbusData["light_lux"] = cached.modbus.lightLux;
  }
  if (isfinite(cached.modbus.minWindDirectionDeg)) {
    modbusData["min_wind_direction_deg"] = cached.modbus.minWindDirectionDeg;
  }
  if (isfinite(cached.modbus.maxWindDirectionDeg)) {
    modbusData["max_wind_direction_deg"] = cached.modbus.maxWindDirectionDeg;
  }
  if (isfinite(cached.modbus.averageWindDirectionDeg)) {
    modbusData["average_wind_direction_deg"] = cached.modbus.averageWindDirectionDeg;
  }
  if (isfinite(cached.modbus.minWindSpeedMs)) {
    modbusData["min_wind_speed_ms"] = cached.modbus.minWindSpeedMs;
  }
  if (isfinite(cached.modbus.maxWindSpeedMs)) {
    modbusData["max_wind_speed_ms"] = cached.modbus.maxWindSpeedMs;
  }
  if (isfinite(cached.modbus.averageWindSpeedMs)) {
    modbusData["average_wind_speed_ms"] = cached.modbus.averageWindSpeedMs;
  }
  if (isfinite(cached.modbus.accumulatedRainfallMm)) {
    modbusData["accumulated_rainfall_mm"] = cached.modbus.accumulatedRainfallMm;
  }
  if (isfinite(cached.modbus.accumulatedRainfallDurationS)) {
    modbusData["accumulated_rainfall_duration_s"] =
      cached.modbus.accumulatedRainfallDurationS;
  }
  if (isfinite(cached.modbus.rainIntensityMmH)) {
    modbusData["rain_intensity_mm_h"] = cached.modbus.rainIntensityMmH;
  }
  if (isfinite(cached.modbus.maxRainIntensityMmH)) {
    modbusData["max_rain_intensity_mm_h"] = cached.modbus.maxRainIntensityMmH;
  }
  if (isfinite(cached.modbus.heatingTemperatureC)) {
    modbusData["heating_temperature_c"] = cached.modbus.heatingTemperatureC;
  }
  if (isfinite(cached.modbus.tiltState)) {
    modbusData["tilt_state"] = cached.modbus.tiltState;
  }
  if (isfinite(cached.modbus.pm25UgM3)) {
    modbusData["pm25_ug_m3"] = cached.modbus.pm25UgM3;
  }
  if (isfinite(cached.modbus.pm10UgM3)) {
    modbusData["pm10_ug_m3"] = cached.modbus.pm10UgM3;
  }
  if (isfinite(cached.modbus.co2Ppm)) {
    modbusData["co2_ppm"] = cached.modbus.co2Ppm;
  }
  if (isfinite(cached.modbus.noiseDb)) {
    modbusData["noise_db"] = cached.modbus.noiseDb;
  }
  if (isfinite(cached.modbus.solarRadiationWm2)) {
    modbusData["solar_radiation_wm2"] = cached.modbus.solarRadiationWm2;
  }
  if (isfinite(cached.modbus.sunshineDurationH)) {
    modbusData["sunshine_duration_h"] = cached.modbus.sunshineDurationH;
  }

  JsonObject config = doc["cfg"].to<JsonObject>();
  config["devEui_msb"] = bytesToHex(cfg.devEui, sizeof(cfg.devEui));
  config["appEui_msb"] = bytesToHex(cfg.appEui, sizeof(cfg.appEui));
  config["appKey_msb"] = bytesToHex(cfg.appKey, sizeof(cfg.appKey));
  config["minutes"] = cfg.intervalMinutes;
  config["adr"] = cfg.adr;
  config["dr"] = cfg.dataRate;
  config["low_power"] = cfg.lowPowerEnabled;
  config["auto_redirect"] = cfg.autoRedirectEnabled;
  config["battery_type"] = cfg.batteryType == BATTERY_TYPE_SAFT_36V
    ? "saft-36v"
    : "lipo-18650";

  JsonObject modbusConfig = config["modbus"].to<JsonObject>();
  modbusConfig["enabled"] = cfg.modbusEnabled;
  modbusConfig["model"] = senseCapModelInfo(cfg.modbusModel).id;
  modbusConfig["address"] = cfg.modbusAddress;
  modbusConfig["baud"] = cfg.modbusBaudRate;
  modbusConfig["poll_seconds"] = cfg.modbusPollSeconds;

  JsonObject loraConfig = config["lora"].to<JsonObject>();
  loraConfig["version"] = 5;
  loraConfig["field_mask"] = cfg.loraFieldMask;
  loraConfig["payload_bytes"] = loraPayloadSize(cfg.loraFieldMask);

  JsonArray loraFields = doc["lora_fields"].to<JsonArray>();
  for (uint8_t id = 0; id < LORA_FIELD_COUNT; ++id) {
    JsonObject field = loraFields.add<JsonObject>();
    field["id"] = LORA_FIELDS[id].id;
    field["label"] = LORA_FIELDS[id].label;
    field["unit"] = LORA_FIELDS[id].unit;
    field["source"] = LORA_FIELDS[id].source;
    field["bytes"] = LORA_FIELDS[id].bytes;
    field["selected"] = (cfg.loraFieldMask & (1UL << id)) != 0;
  }

  JsonObject calibration = config["weg_cal"].to<JsonObject>();
  calibration["raw0"] = cfg.wegRaw0;
  calibration["mm0"] = static_cast<float>(cfg.wegMm0_x1000) / 1000.0f;
  calibration["raw1"] = cfg.wegRaw1;
  calibration["mm1"] = static_cast<float>(cfg.wegMm1_x1000) / 1000.0f;

  sendJson(doc);
}

void apiChipDevEui() {
  uint8_t chip[8];
  chipDevEui(chip);

  JsonDocument doc;
  doc["chip_devEui_msb"] = bytesToHex(chip, sizeof(chip));
  doc["stored_devEui_msb"] = bytesToHex(cfg.devEui, sizeof(cfg.devEui));
  sendJson(doc);
}

void apiUseChipDevEui() {
  uint8_t chip[8];
  chipDevEui(chip);
  memcpy(cfg.devEui, chip, sizeof(cfg.devEui));
  saveConfig();
  server.send(200, "text/plain", "saved");
}

void apiSaveConfig() {
  if (!server.hasArg("plain")) {
    server.send(400, "text/plain", "missing body");
    return;
  }

  JsonDocument request;
  const DeserializationError error = deserializeJson(request, server.arg("plain"));
  if (error) {
    server.send(400, "text/plain", "invalid json");
    return;
  }

  Config updated = cfg;

  if (request["devEui_msb"].is<const char*>()) {
    if (!parseHex(request["devEui_msb"].as<String>(), updated.devEui, sizeof(updated.devEui))) {
      server.send(400, "text/plain", "invalid DevEUI");
      return;
    }
  }
  if (request["appEui_msb"].is<const char*>()) {
    if (!parseHex(request["appEui_msb"].as<String>(), updated.appEui, sizeof(updated.appEui))) {
      server.send(400, "text/plain", "invalid JoinEUI");
      return;
    }
  }
  if (request["appKey_msb"].is<const char*>()) {
    if (!parseHex(request["appKey_msb"].as<String>(), updated.appKey, sizeof(updated.appKey))) {
      server.send(400, "text/plain", "invalid AppKey");
      return;
    }
  }

  if (request["minutes"].is<uint32_t>()) {
    const uint32_t value = request["minutes"].as<uint32_t>();
    if (value < 1 || value > 1440) {
      server.send(400, "text/plain", "interval range 1..1440");
      return;
    }
    updated.intervalMinutes = value;
  }

  if (request["adr"].is<bool>()) updated.adr = request["adr"].as<bool>();
  if (request["low_power"].is<bool>()) updated.lowPowerEnabled = request["low_power"].as<bool>();
  if (request["auto_redirect"].is<bool>()) updated.autoRedirectEnabled = request["auto_redirect"].as<bool>();
  if (request["battery_type"].is<const char*>()) {
    const String batteryType = request["battery_type"].as<String>();
    if (batteryType == "lipo-18650") {
      updated.batteryType = BATTERY_TYPE_LIPO_18650;
    } else if (batteryType == "saft-36v" || batteryType == "saft-ls26500") {
      updated.batteryType = BATTERY_TYPE_SAFT_36V;
    } else {
      server.send(400, "text/plain", "invalid battery type");
      return;
    }
  }
  if (request["dr"].is<int>()) {
    const int value = request["dr"].as<int>();
    if (value < 0 || value > 5) {
      server.send(400, "text/plain", "DR range 0..5");
      return;
    }
    updated.dataRate = static_cast<uint8_t>(value);
  }

  if (request["modbus"].is<JsonObjectConst>()) {
    JsonObjectConst modbus = request["modbus"].as<JsonObjectConst>();

    if (modbus["enabled"].is<bool>()) {
      updated.modbusEnabled = modbus["enabled"].as<bool>();
    }
    if (modbus["model"].is<const char*>()) {
      const int model = parseSenseCapModel(modbus["model"].as<String>());
      if (model < 0) {
        server.send(400, "text/plain", "invalid Modbus model");
        return;
      }
      updated.modbusModel = static_cast<uint8_t>(model);
    }
    if (modbus["address"].is<int>()) {
      const int value = modbus["address"].as<int>();
      if (value < 1 || value > 247) {
        server.send(400, "text/plain", "Modbus address range 1..247");
        return;
      }
      updated.modbusAddress = static_cast<uint8_t>(value);
    }
    if (modbus["baud"].is<uint32_t>()) {
      const uint32_t value = modbus["baud"].as<uint32_t>();
      if (!isSupportedModbusBaud(value)) {
        server.send(400, "text/plain", "unsupported Modbus baud rate");
        return;
      }
      updated.modbusBaudRate = value;
    }
    if (modbus["poll_seconds"].is<uint32_t>()) {
      const uint32_t value = modbus["poll_seconds"].as<uint32_t>();
      if (value < 2 || value > 3600) {
        server.send(400, "text/plain", "Modbus poll range 2..3600 seconds");
        return;
      }
      updated.modbusPollSeconds = static_cast<uint16_t>(value);
    }
  }

  if (request["lora"].is<JsonObjectConst>()) {
    JsonObjectConst lora = request["lora"].as<JsonObjectConst>();
    if (lora["field_mask"].is<uint32_t>()) {
      const uint32_t value = lora["field_mask"].as<uint32_t>();
      if ((value & ~LORA_FIELD_MASK_ALL) != 0) {
        server.send(400, "text/plain", "LoRa field selection is invalid");
        return;
      }
      if (loraPayloadSize(value) > 242) {
        server.send(400, "text/plain", "LoRa payload is too large");
        return;
      }
      updated.loraFieldMask = value;
    }
  }

  if (request["weg_cal"].is<JsonObjectConst>()) {
    JsonObjectConst cal = request["weg_cal"].as<JsonObjectConst>();

    if (!cal["raw0"].isNull()) updated.wegRaw0 = cal["raw0"].as<int32_t>();
    if (!cal["raw1"].isNull()) updated.wegRaw1 = cal["raw1"].as<int32_t>();
    if (!cal["mm0"].isNull()) {
      const float value = cal["mm0"].as<float>();
      if (!isfinite(value)) {
        server.send(400, "text/plain", "invalid calibration point 0");
        return;
      }
      updated.wegMm0_x1000 = static_cast<int32_t>(lroundf(value * 1000.0f));
    }
    if (!cal["mm1"].isNull()) {
      const float value = cal["mm1"].as<float>();
      if (!isfinite(value)) {
        server.send(400, "text/plain", "invalid calibration point 1");
        return;
      }
      updated.wegMm1_x1000 = static_cast<int32_t>(lroundf(value * 1000.0f));
    }

    if (updated.wegRaw0 == updated.wegRaw1) {
      server.send(400, "text/plain", "calibration raw values must differ");
      return;
    }
  }

  const bool modbusConfigChanged =
    cfg.modbusEnabled != updated.modbusEnabled ||
    cfg.modbusModel != updated.modbusModel ||
    cfg.modbusAddress != updated.modbusAddress ||
    cfg.modbusBaudRate != updated.modbusBaudRate ||
    cfg.modbusPollSeconds != updated.modbusPollSeconds;

  cfg = updated;
  saveConfig();
  copyConfigToLoRaGlobals();
  if (modbusConfigChanged) {
    lastModbusPollMs = 0;
    lastModbusSuccessMs = 0;
    modbusLastError = "";
    cached.modbus = ModbusMeasurements();
    modbusInterfaceReady = false;
    sensorInterfacesReady = false;
  }
  if (cached.batteryValid) {
    cached.batteryPercent = batteryPercentFromMillivolts(cached.batteryMillivolts);
  }
  if (sensorPowerEnabled) readAllSensors();

  // AP loop refreshes the sensor cache independently. Keep this response fast
  // so the browser does not lose the connection while saving settings.
  server.send(200, "text/plain", "saved");
}

void apiSensorPower() {
  if (!server.hasArg("plain")) {
    server.send(400, "text/plain", "missing body");
    return;
  }

  JsonDocument request;
  if (deserializeJson(request, server.arg("plain")) ||
      !request["enabled"].is<bool>()) {
    server.send(400, "text/plain", "invalid enabled value");
    return;
  }

  if (request["enabled"].as<bool>()) {
    readAllSensors();
  } else {
    powerExternalSensorsOff();
  }

  JsonDocument response;
  response["enabled"] = sensorPowerEnabled;
  sendJson(response);
}

void apiReboot() {
  server.send(200, "text/plain", "restarting");
  delay(200);
  ESP.restart();
}

void redirectToCaptivePortal() {
  if (server.uri().startsWith("/api/") || !cfg.autoRedirectEnabled) {
    server.send(404, "text/plain", "Not found");
    return;
  }

  server.sendHeader("Cache-Control", "no-store");
  server.sendHeader("Location", String("http://") + WiFi.softAPIP().toString() + "/", true);
  server.send(302, "text/plain", "Redirecting to MultiConnect");
}

void attachWebRoutes() {
  server.on("/", HTTP_GET, []() {
    server.sendHeader("Cache-Control", "no-store");
    if (!streamLittleFsFile("/index.html", "text/html")) {
      server.send(404, "text/plain", "index.html missing in LittleFS");
    }
  });

  server.on("/i18n.json", HTTP_GET, []() {
    server.sendHeader("Cache-Control", "no-store");
    if (!streamLittleFsFile("/i18n.json", "application/json")) {
      server.send(404, "text/plain", "i18n.json missing in LittleFS");
    }
  });

  server.on("/logo.svg", HTTP_GET, []() {
    server.sendHeader("Cache-Control", "public, max-age=86400");
    if (!streamLittleFsFile("/logo.svg", "image/svg+xml")) {
      server.send(404, "text/plain", "logo.svg missing in LittleFS");
    }
  });

  server.on("/chirpstack_decoder.js", HTTP_GET, []() {
    server.sendHeader("Cache-Control", "no-store");
    if (!streamLittleFsFile("/chirpstack_decoder.js", "application/javascript")) {
      server.send(404, "text/plain", "chirpstack_decoder.js missing in LittleFS");
    }
  });

  server.on("/api/state", HTTP_GET, apiState);
  server.on("/api/config", HTTP_POST, apiSaveConfig);
  server.on("/api/sensor-power", HTTP_POST, apiSensorPower);
  server.on("/api/deveui", HTTP_GET, apiChipDevEui);
  server.on("/api/deveui/use-chip", HTTP_POST, apiUseChipDevEui);
  server.on("/api/reboot", HTTP_POST, apiReboot);

  server.on("/generate_204", HTTP_ANY, redirectToCaptivePortal);
  server.on("/gen_204", HTTP_ANY, redirectToCaptivePortal);
  server.on("/hotspot-detect.html", HTTP_ANY, redirectToCaptivePortal);
  server.on("/connecttest.txt", HTTP_ANY, redirectToCaptivePortal);
  server.on("/ncsi.txt", HTTP_ANY, redirectToCaptivePortal);
  server.on("/redirect", HTTP_ANY, redirectToCaptivePortal);
  server.on("/canonical.html", HTTP_ANY, redirectToCaptivePortal);
  server.on("/success.txt", HTTP_ANY, redirectToCaptivePortal);
  server.on("/fwlink", HTTP_ANY, redirectToCaptivePortal);
  server.onNotFound(redirectToCaptivePortal);

  server.begin();
}

void startAccessPoint() {
  WiFi.mode(WIFI_AP);
  WiFi.setSleep(false);
  WiFi.setTxPower(WIFI_POWER_7dBm);

  const String ssid = apSsid();
  WiFi.softAP(ssid.c_str(), AP_PASSWORD);
  apDnsReady = apDnsServer.start(53, "*", WiFi.softAPIP());
  Serial.printf("[AP] captive portal DNS=%s\n", apDnsReady ? "READY" : "FAILED");

  littleFsReady = LittleFS.begin(true);
  if (littleFsReady) {
    attachWebRoutes();
  } else {
    Serial.println("[LittleFS] mount failed");
  }

  Serial.printf("[AP] SSID=%s IP=%s\n", ssid.c_str(), WiFi.softAPIP().toString().c_str());
  oledShowApInfo(ssid);
}

void disableUnusedRadiosForFieldMode() {
  WiFi.persistent(false);
  WiFi.disconnect(true, true);
  WiFi.mode(WIFI_OFF);
#if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
  btStop();
#endif
}

// ============================= Payload ==============================
// Payload V5, big-endian:
// 0 version=5
// 1 status: bit0 temp, bit1 position, bit2 Weg, bit3 INA, bit4 battery,
//           bit5 Modbus valid, bit6 Modbus complete
// 2..5 selected field mask, bits 0..30
// Remaining bytes contain selected fields in ascending field-id order.
static constexpr uint8_t LORA_PAYLOAD_VERSION = 5;
static constexpr uint8_t LORA_PAYLOAD_HEADER_SIZE = 6;
static constexpr uint8_t LORA_STATUS_TEMP = 1U << 0;
static constexpr uint8_t LORA_STATUS_POSITION = 1U << 1;
static constexpr uint8_t LORA_STATUS_WEG = 1U << 2;
static constexpr uint8_t LORA_STATUS_INA = 1U << 3;
static constexpr uint8_t LORA_STATUS_BATTERY = 1U << 4;
static constexpr uint8_t LORA_STATUS_MODBUS = 1U << 5;
static constexpr uint8_t LORA_STATUS_MODBUS_COMPLETE = 1U << 6;

void putInt16BE(uint8_t& index, int16_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

void putUInt16BE(uint8_t& index, uint16_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

void putInt32BE(uint8_t& index, int32_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 24) & 0xFF);
  appData[index++] = static_cast<uint8_t>((value >> 16) & 0xFF);
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

void putUInt32BE(uint8_t& index, uint32_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 24) & 0xFF);
  appData[index++] = static_cast<uint8_t>((value >> 16) & 0xFF);
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

void putLoraInt16(uint8_t& index,
                  bool valid,
                  float value,
                  float multiplier) {
  const long encoded = valid
    ? constrain(lroundf(value * multiplier),
                static_cast<long>(INT16_MIN),
                static_cast<long>(INT16_MAX - 1))
    : static_cast<long>(INT16_MAX);
  putInt16BE(index, static_cast<int16_t>(encoded));
}

void putLoraUInt16(uint8_t& index,
                   bool valid,
                   float value,
                   float multiplier) {
  const long encoded = valid
    ? constrain(lroundf(value * multiplier),
                0L,
                static_cast<long>(UINT16_MAX - 1))
    : static_cast<long>(UINT16_MAX);
  putUInt16BE(index, static_cast<uint16_t>(encoded));
}

void putLoraInt32(uint8_t& index, bool valid, int32_t value) {
  putInt32BE(index, valid ? value : INT32_MAX);
}

void putLoraUInt32(uint8_t& index,
                   bool valid,
                   float value,
                   float multiplier) {
  uint32_t encoded = UINT32_MAX;
  if (valid) {
    const float scaled = value * multiplier;
    if (!isfinite(scaled) || scaled <= 0.0f) {
      encoded = 0;
    } else if (scaled >= 4294967294.0f) {
      encoded = UINT32_MAX - 1UL;
    } else {
      encoded = static_cast<uint32_t>(lroundf(scaled));
    }
  }
  putUInt32BE(index, encoded);
}

void putLoraUInt8(uint8_t& index, bool valid, uint8_t value) {
  appData[index++] = valid ? value : UINT8_MAX;
}

static void prepareTxFrame(uint8_t port) {
  (void)port;

  uint8_t index = 0;
  appData[index++] = LORA_PAYLOAD_VERSION;

  uint8_t status = 0;
  if (cached.temperatureValid) status |= LORA_STATUS_TEMP;
  if (cached.wegValid) status |= LORA_STATUS_POSITION | LORA_STATUS_WEG;
  if (cached.inaValid) status |= LORA_STATUS_INA;
  if (cached.batteryValid) status |= LORA_STATUS_BATTERY;
  if (cached.modbus.valid) status |= LORA_STATUS_MODBUS;
  if (cached.modbus.complete) status |= LORA_STATUS_MODBUS_COMPLETE;
  appData[index++] = status;

  const uint32_t fieldMask = cfg.loraFieldMask & LORA_FIELD_MASK_ALL;
  putUInt32BE(index, fieldMask);

  for (uint8_t id = 0; id < LORA_FIELD_COUNT; ++id) {
    if ((fieldMask & (1UL << id)) == 0) continue;

    switch (id) {
      case LORA_FIELD_TEMPERATURE:
        putLoraInt16(index, cached.temperatureValid, cached.temperatureC, 100.0f);
        break;
      case LORA_FIELD_POSITION:
        putLoraUInt16(index, cached.wegValid, cached.wegPositionMm, 1000.0f);
        break;
      case LORA_FIELD_WEG_RAW:
        putLoraInt32(index, cached.wegValid, cached.wegRaw);
        break;
      case LORA_FIELD_INA_BUS:
        putLoraUInt16(index, cached.inaValid, cached.inaBusVoltageV, 1000.0f);
        break;
      case LORA_FIELD_INA_SHUNT:
        putLoraInt16(index, cached.inaValid, cached.inaShuntVoltageMv, 1000.0f);
        break;
      case LORA_FIELD_INA_CURRENT:
        putLoraInt32(index, cached.inaValid,
                     cached.inaValid
                       ? static_cast<int32_t>(lroundf(cached.inaCurrentMa))
                       : 0);
        break;
      case LORA_FIELD_INA_POWER:
        putLoraInt32(index, cached.inaValid,
                     cached.inaValid
                       ? static_cast<int32_t>(lroundf(cached.inaPowerW * 1000.0f))
                       : 0);
        break;
      case LORA_FIELD_BATTERY_VOLTAGE:
        putLoraUInt16(index, cached.batteryValid, cached.batteryVoltageV, 1000.0f);
        break;
      case LORA_FIELD_BATTERY_PERCENT:
        putLoraUInt8(index, cached.batteryValid, cached.batteryPercent);
        break;
      case LORA_FIELD_AIR_TEMPERATURE:
        putLoraInt16(index, isfinite(cached.modbus.airTemperatureC),
                     cached.modbus.airTemperatureC, 100.0f);
        break;
      case LORA_FIELD_HUMIDITY:
        putLoraUInt16(index, isfinite(cached.modbus.humidityPct),
                      cached.modbus.humidityPct, 100.0f);
        break;
      case LORA_FIELD_PRESSURE:
        putLoraUInt32(index, isfinite(cached.modbus.pressurePa),
                      cached.modbus.pressurePa, 1.0f);
        break;
      case LORA_FIELD_LIGHT:
        putLoraUInt32(index, isfinite(cached.modbus.lightLux),
                      cached.modbus.lightLux, 1.0f);
        break;
      case LORA_FIELD_MIN_WIND_DIRECTION:
        putLoraUInt16(index, isfinite(cached.modbus.minWindDirectionDeg),
                      cached.modbus.minWindDirectionDeg, 10.0f);
        break;
      case LORA_FIELD_MAX_WIND_DIRECTION:
        putLoraUInt16(index, isfinite(cached.modbus.maxWindDirectionDeg),
                      cached.modbus.maxWindDirectionDeg, 10.0f);
        break;
      case LORA_FIELD_AVERAGE_WIND_DIRECTION:
        putLoraUInt16(index, isfinite(cached.modbus.averageWindDirectionDeg),
                      cached.modbus.averageWindDirectionDeg, 10.0f);
        break;
      case LORA_FIELD_MIN_WIND_SPEED:
        putLoraUInt16(index, isfinite(cached.modbus.minWindSpeedMs),
                      cached.modbus.minWindSpeedMs, 100.0f);
        break;
      case LORA_FIELD_MAX_WIND_SPEED:
        putLoraUInt16(index, isfinite(cached.modbus.maxWindSpeedMs),
                      cached.modbus.maxWindSpeedMs, 100.0f);
        break;
      case LORA_FIELD_AVERAGE_WIND_SPEED:
        putLoraUInt16(index, isfinite(cached.modbus.averageWindSpeedMs),
                      cached.modbus.averageWindSpeedMs, 100.0f);
        break;
      case LORA_FIELD_ACCUMULATED_RAINFALL:
        putLoraUInt32(index, isfinite(cached.modbus.accumulatedRainfallMm),
                      cached.modbus.accumulatedRainfallMm, 100.0f);
        break;
      case LORA_FIELD_RAIN_DURATION:
        putLoraUInt32(index, isfinite(cached.modbus.accumulatedRainfallDurationS),
                      cached.modbus.accumulatedRainfallDurationS, 1.0f);
        break;
      case LORA_FIELD_RAIN_INTENSITY:
        putLoraUInt16(index, isfinite(cached.modbus.rainIntensityMmH),
                      cached.modbus.rainIntensityMmH, 100.0f);
        break;
      case LORA_FIELD_MAX_RAIN_INTENSITY:
        putLoraUInt16(index, isfinite(cached.modbus.maxRainIntensityMmH),
                      cached.modbus.maxRainIntensityMmH, 100.0f);
        break;
      case LORA_FIELD_HEATING_TEMPERATURE:
        putLoraInt16(index, isfinite(cached.modbus.heatingTemperatureC),
                     cached.modbus.heatingTemperatureC, 100.0f);
        break;
      case LORA_FIELD_TILT:
        putLoraUInt8(index, isfinite(cached.modbus.tiltState),
                     isfinite(cached.modbus.tiltState)
                       ? static_cast<uint8_t>(lroundf(cached.modbus.tiltState))
                       : 0);
        break;
      case LORA_FIELD_PM25:
        putLoraUInt32(index, isfinite(cached.modbus.pm25UgM3),
                      cached.modbus.pm25UgM3, 1.0f);
        break;
      case LORA_FIELD_PM10:
        putLoraUInt32(index, isfinite(cached.modbus.pm10UgM3),
                      cached.modbus.pm10UgM3, 1.0f);
        break;
      case LORA_FIELD_CO2:
        putLoraUInt16(index, isfinite(cached.modbus.co2Ppm),
                      cached.modbus.co2Ppm, 1.0f);
        break;
      case LORA_FIELD_NOISE:
        putLoraUInt16(index, isfinite(cached.modbus.noiseDb),
                      cached.modbus.noiseDb, 10.0f);
        break;
      case LORA_FIELD_SOLAR_RADIATION:
        putLoraUInt32(index, isfinite(cached.modbus.solarRadiationWm2),
                      cached.modbus.solarRadiationWm2, 1.0f);
        break;
      case LORA_FIELD_SUNSHINE_DURATION:
        putLoraUInt32(index, isfinite(cached.modbus.sunshineDurationH),
                      cached.modbus.sunshineDurationH, 10.0f);
        break;
      default:
        break;
    }
  }

  appDataSize = index;

  Serial.print("[LORA] cached payload: ");
  for (uint8_t i = 0; i < appDataSize; ++i) Serial.printf("%02X ", appData[i]);
  Serial.println();
}

// ========================= Field power policy =======================
void releaseLoRaFemHolds() {
#if defined(USE_KCT8103L_PA)
  rtc_gpio_hold_dis(static_cast<gpio_num_t>(PIN_LORA_FEM_POWER));
  rtc_gpio_hold_dis(static_cast<gpio_num_t>(PIN_LORA_FEM_CSD));
#endif
}

void disableLoRaFem() {
#if defined(USE_KCT8103L_PA)
  releaseLoRaFemHolds();

  pinMode(PIN_LORA_FEM_CSD, OUTPUT);
  digitalWrite(PIN_LORA_FEM_CSD, LOW);
  pinMode(PIN_LORA_FEM_CTX, OUTPUT);
  digitalWrite(PIN_LORA_FEM_CTX, LOW);
  pinMode(PIN_LORA_FEM_POWER, OUTPUT);
  digitalWrite(PIN_LORA_FEM_POWER, LOW);

  rtc_gpio_hold_en(static_cast<gpio_num_t>(PIN_LORA_FEM_CSD));
  rtc_gpio_hold_en(static_cast<gpio_num_t>(PIN_LORA_FEM_POWER));
#endif
}

void prepareLoRaFemForSensorAccess() {
#if defined(USE_KCT8103L_PA)
  disableLoRaFem();
  pinMode(PIN_LORA_FEM_CTX, INPUT);
#endif
}

void disableSensorInterfacesForSleep() {
  disableModbusInterface();

  if (inaAvailable) {
    // INA226 mode 0 is power-down; wake-up reinitializes the sensor.
    inaWriteRegister(INA226_REG_CONFIG, 0x4120);
  }
  if (adsAvailable) adsPowerDown();
  disableAdsInterface();

  sensorI2C.end();
  pinMode(PIN_INA_SDA, INPUT);
  pinMode(PIN_INA_SCL, INPUT);
  pinMode(PIN_DS18B20, INPUT);
}

void powerExternalSensorsOff() {
  if (sensorPowerEnabled) {
    disableSensorInterfacesForSleep();
    delay(2);
  }

  setExternalSensorMosfet(false);
  sensorInterfacesReady = false;
  dsAvailable = false;
  inaAvailable = false;
  adsAvailable = false;
  modbusInterfaceReady = false;
  lastModbusPollMs = 0;
  lastModbusSuccessMs = 0;
  modbusLastError = "sensor power off";
  Serial.println("[SENSORS] external power OFF");
}

void preparePeripheralsForLowPower() {
  // OLED and Wi-Fi/Bluetooth are already disabled in FIELD mode.
  digitalWrite(PIN_BATTERY_CTRL, LOW);
  digitalWrite(PIN_RS485_RE_DE, LOW);
  // Q1/Q2 are disabled during the sleep phase and enabled immediately
  // before the next measurement.
  powerExternalSensorsOff();
  digitalWrite(PIN_BOARD_LED, LOW);

  // The Heltec radio driver disables the V4.3 FEM after TX/RX windows.
  // Do not touch FEM pins here while the current LoRaWAN exchange may run.
  // Keep only the LoRaWAN stack state needed by LoRaWAN.sleep().
  // Deep-sleep would reboot the ESP32-S3 and force a new OTAA join unless
  // the complete LoRaMAC session were persisted. LoRaWAN.sleep() therefore
  // provides the most reliable low-energy mode for the current firmware.
  Serial.flush();
}

// =============================== Setup ==============================
bool selectApMode() {
  pinMode(PIN_AP_MODE, INPUT_PULLUP);
  delay(50);  // allow the external selector and internal pull-up to settle

  const bool selected = digitalRead(PIN_AP_MODE) == HIGH;
  Serial.printf("[MODE] GPIO48=%s -> %s mode\n",
                selected ? "HIGH/open" : "LOW/GND",
                selected ? "AP" : "FIELD");
  return selected;
}

void setup() {
  Serial.begin(115200);
  const uint32_t serialStarted = millis();
  while (!Serial && millis() - serialStarted < 5000) delay(10);

  releaseLoRaFemHolds();
  pinMode(PIN_SENSOR_MOSFET, OUTPUT);
  digitalWrite(PIN_SENSOR_MOSFET, SENSOR_MOSFET_OFF_LEVEL);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  // Keep the OLED/Vext rail physically off immediately after board init.
  // The display is initialized only when AP mode is selected.
  pinMode(Vext, OUTPUT);
  digitalWrite(Vext, HIGH);  // Heltec Vext active LOW -> HIGH means OFF

  Serial.println();
  Serial.printf("%s FW %s (%s %s)\n", DEVICE_NAME, FW_VERSION, __DATE__, __TIME__);

  // Select the operating mode once at startup.
  apMode = selectApMode();

  // Keep reserved outputs in safe states.
  pinMode(PIN_RS485_RE_DE, OUTPUT);
  digitalWrite(PIN_RS485_RE_DE, LOW);
  digitalWrite(PIN_SENSOR_MOSFET, SENSOR_MOSFET_OFF_LEVEL);
  pinMode(PIN_BOARD_LED, OUTPUT);
  digitalWrite(PIN_BOARD_LED, LOW);
  disableLoRaFem();

#if defined(USE_KCT8103L_PA)
  Serial.println("[BOARD] V4.3 KCT8103L FEM control enabled");
#else
  Serial.println("[BOARD] WARNING: select LoRa FEM Type = USE_KCT8103L_PA for V4.3");
#endif

  loadConfig();
  copyConfigToLoRaGlobals();

  if (apMode) {
    oledPowerOn();
    oledSplash();
    delay(650);
    oledShowBootMode(true);
    delay(850);
  } else {
    // FIELD mode: never initialize the SSD1306 and never enable Vext.
    oledAvailable = false;
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, HIGH);
  }

  initializeBatteryMeasurement();
  powerExternalSensorsOn();

  Serial.printf("[INIT] ADS1220=%s DS18B20=%s INA226=%s LOWPOWER=%s\n",
                adsAvailable ? "OK" : "NOT CONNECTED",
                dsAvailable ? "OK" : "NOT CONNECTED",
                inaAvailable ? "CONNECTED" : "NOT CONNECTED",
                cfg.lowPowerEnabled ? "ON" : "OFF");

  // Initial cached measurement is prepared before LoRaWAN initialization.
  readAllSensors();

  if (apMode) {
    startAccessPoint();
  } else {
    // FIELD mode: OLED remains physically powered off from boot onward and
    // Q1/Q2 are disabled until the next measurement. Force a fresh SenseCAP
    // warm-up after LoRaWAN has joined before the first uplink.
    powerExternalSensorsOff();
    fieldMeasurementPending = true;
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, HIGH);
    disableUnusedRadiosForFieldMode();
    Serial.println("[MODE] FIELD low-power mode; OLED/Vext OFF");
  }
}

// ================================ Loop ==============================
void loop() {
  if (apMode) {
    if (apDnsReady) apDnsServer.processNextRequest();
    server.handleClient();

    static uint32_t lastSensorRead = 0;
    static uint32_t lastPageChange = 0;
    static bool sensorPage = false;

    if (millis() - lastSensorRead >= 2000) {
      lastSensorRead = millis();
      if (sensorPowerEnabled) readAllSensors();
    }

    if (millis() - lastPageChange >= 5000) {
      lastPageChange = millis();
      sensorPage = !sensorPage;
      if (sensorPage) oledShowSensors();
      else oledShowApInfo(apSsid());
    }

    delay(2);
    return;
  }

  switch (deviceState) {
    case DEVICE_STATE_INIT:
      Serial.println("[LORA] INIT");
      copyConfigToLoRaGlobals();
      LoRaWAN.init(loraWanClass, loraWanRegion);
      LoRaWAN.setDefaultDR(cfg.adr ? 3 : cfg.dataRate);
      deviceState = DEVICE_STATE_JOIN;
      break;

    case DEVICE_STATE_JOIN:
      Serial.println("[LORA] JOIN");
      LoRaWAN.join();
      break;

    case DEVICE_STATE_SEND:
      if (fieldMeasurementPending) {
        Serial.println("[LORA] MEASURE AFTER WAKE");
        readAllSensors();
        if (cfg.modbusEnabled && !cached.modbus.valid) {
          Serial.println("[LORA] MODBUS UNAVAILABLE - SEND LOCAL SENSOR DATA");
        }
        fieldMeasurementPending = false;
        // Keep the sensor rail on for the next loop pass so the freshly read
        // SenseCAP values remain available while the uplink is prepared.
        delay(5);
        return;
      }

      if (cfg.modbusEnabled && !cached.modbus.valid) {
        Serial.println("[LORA] MODBUS UNAVAILABLE - LOCAL DATA ONLY");
      } else if (cfg.modbusEnabled) {
        Serial.println("[LORA] SENSECAP DATA READY");
      }

      Serial.println("[LORA] SEND CACHED");
      prepareTxFrame(appPort);
      LoRaWAN.send();
      deviceState = DEVICE_STATE_CYCLE;
      break;

    case DEVICE_STATE_CYCLE:
      // The cache was measured before SEND. Do not access sensors here:
      // LoRaWAN still owns the radio for its TX/RX windows.
      appTxDutyCycle = intervalMs();
      if (cfg.lowPowerEnabled) preparePeripheralsForLowPower();
      else powerExternalSensorsOff();
      LoRaWAN.cycle(appTxDutyCycle);
      fieldMeasurementPending = true;
      deviceState = DEVICE_STATE_SLEEP;
      break;

    case DEVICE_STATE_SLEEP:
      // Keep the Q1/Q2 gate low throughout the entire sleep interval.
      setExternalSensorMosfet(false);
      if (cfg.lowPowerEnabled) {
        LoRaWAN.sleep(loraWanClass);
      } else {
        Mcu.timerhandler();
        Radio.IrqProcess();
        delay(10);
      }
      break;

    default:
      deviceState = DEVICE_STATE_INIT;
      break;
  }
}
