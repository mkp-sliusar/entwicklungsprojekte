/*
 * =====================================================================
 *  MKP MultiConnect V1.6.0
 *  Universal LoRaWAN / NB-IoT-ready sensor controller
 *  Current firmware profile: LoRaWAN + KCT8103L FEM + optional DS18B20 + ADS1220 Wegsensor + INA226 + multi-device Modbus RTU
 *
 *  RELEASE NOTES - V1.6.0
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
 *    - Up to four Modbus devices can be configured and polled sequentially
 *      with per-device presets, addresses, baud rates, and warm-up times.
 *    - AP mode provides captive-portal DNS. Automatic browser redirect is
 *      disabled by default.
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

struct Config;

// ============================= Firmware =============================
static constexpr const char* FW_VERSION = "1.6.0";
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

// Modbus vendors, presets, and device slots.
static constexpr uint8_t MODBUS_MAX_SISGEO_DEVICES = 12;
static constexpr uint8_t MODBUS_MAX_DEVICES = MODBUS_MAX_SISGEO_DEVICES;
static constexpr uint16_t MODBUS_MAX_REGISTERS = 32;
static constexpr uint8_t MODBUS_LEGACY_MAX_DEVICES = 4;
static constexpr uint8_t MODBUS_MAX_SENSECAP_DEVICES = 1;
static constexpr uint8_t SISGEO_LORA_FIELD_COUNT = 5;
static constexpr uint8_t SISGEO_LORA_FIELD_VALUE1 = 0;
static constexpr uint8_t SISGEO_LORA_FIELD_VALUE2 = 1;
static constexpr uint8_t SISGEO_LORA_FIELD_TEMPERATURE = 2;
static constexpr uint8_t SISGEO_LORA_FIELD_HUMIDITY = 3;
static constexpr uint8_t SISGEO_LORA_FIELD_SUPPLY_VOLTAGE = 4;
static constexpr uint8_t SISGEO_LORA_FIELD_MASK_ALL =
  (1U << SISGEO_LORA_FIELD_COUNT) - 1U;
static constexpr uint8_t LORA_PAYLOAD_VERSION_V5 = 5;
static constexpr uint8_t LORA_PAYLOAD_VERSION_V6 = 6;

struct ModbusMeasurements;
struct ModbusDeviceRuntime;

enum ModbusVendor : uint8_t {
  MODBUS_VENDOR_SENSECAP = 0,
  MODBUS_VENDOR_SISGEO,
  MODBUS_VENDOR_CUSTOM,
  MODBUS_VENDOR_COUNT
};

enum ModbusValueFormat : uint8_t {
  MODBUS_VALUE_FLOAT32 = 0,
  MODBUS_VALUE_UINT16,
  MODBUS_VALUE_INT16,
  MODBUS_VALUE_UINT32
};

struct ModbusDeviceConfig {
  uint8_t enabled;
  uint8_t vendor;
  uint8_t model;
  uint8_t address;
  uint32_t baudRate;
  uint16_t warmupSeconds;
  uint16_t valueRegister;
  uint8_t functionCode;
  uint8_t registerCount;
  uint8_t valueFormat;
};

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

static constexpr uint8_t SISGEO_MODEL_H_LEVEL = 0;
static constexpr uint8_t CUSTOM_MODEL_FLOAT32 = 0;

struct ModbusPresetInfo {
  uint8_t vendor;
  uint8_t model;
  const char* vendorId;
  const char* vendorLabel;
  const char* modelId;
  const char* modelLabel;
  uint8_t defaultAddress;
  uint32_t defaultBaudRate;
  uint16_t defaultWarmupSeconds;
  uint16_t valueRegister;
  uint8_t functionCode;
  uint8_t registerCount;
  uint8_t valueFormat;
};

static constexpr ModbusPresetInfo MODBUS_PRESETS[] = {
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S200,
    "sensecap", "SenseCAP", "s200", "S200", 44, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S500,
    "sensecap", "SenseCAP", "s500", "S500", 10, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S600_A,
    "sensecap", "SenseCAP", "s600-a", "S600-A", 69, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S700_A,
    "sensecap", "SenseCAP", "s700-a", "S700-A", 20, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S700_BC,
    "sensecap", "SenseCAP", "s700-bc", "S700-B/C", 60, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S800,
    "sensecap", "SenseCAP", "s800", "S800", 46, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S1000,
    "sensecap", "SenseCAP", "s1000", "S1000", 43, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SENSECAP, SENSECAP_S1000_C,
    "sensecap", "SenseCAP", "s1000-c", "S1000-C", 61, 9600, 10, 0, 4, 0, 0 },
  { MODBUS_VENDOR_SISGEO, SISGEO_MODEL_H_LEVEL,
    "sisgeo", "SISGEO", "h-level", "H-LEVEL digital", 1, 9600, 3, 0x0202, 4, 2, MODBUS_VALUE_FLOAT32 },
  { MODBUS_VENDOR_CUSTOM, CUSTOM_MODEL_FLOAT32,
    "custom", "Custom", "float32", "Generic 32-bit float", 1, 9600, 3, 0, 4, 2, MODBUS_VALUE_FLOAT32 }
};

static constexpr size_t MODBUS_PRESET_COUNT =
  sizeof(MODBUS_PRESETS) / sizeof(MODBUS_PRESETS[0]);

const ModbusPresetInfo* findModbusPreset(uint8_t vendor, uint8_t model) {
  for (size_t i = 0; i < MODBUS_PRESET_COUNT; ++i) {
    if (MODBUS_PRESETS[i].vendor == vendor && MODBUS_PRESETS[i].model == model) {
      return &MODBUS_PRESETS[i];
    }
  }
  return nullptr;
}

const ModbusPresetInfo* findModbusPreset(const String& vendorId,
                                         const String& modelId) {
  for (size_t i = 0; i < MODBUS_PRESET_COUNT; ++i) {
    if (vendorId == MODBUS_PRESETS[i].vendorId &&
        modelId == MODBUS_PRESETS[i].modelId) {
      return &MODBUS_PRESETS[i];
    }
  }
  return nullptr;
}

const char* modbusVendorId(uint8_t vendor) {
  const ModbusPresetInfo* preset = findModbusPreset(vendor, 0);
  return preset != nullptr ? preset->vendorId : "custom";
}

const char* modbusVendorLabel(uint8_t vendor) {
  const ModbusPresetInfo* preset = findModbusPreset(vendor, 0);
  return preset != nullptr ? preset->vendorLabel : "Custom";
}

const char* modbusModelId(uint8_t vendor, uint8_t model) {
  const ModbusPresetInfo* preset = findModbusPreset(vendor, model);
  return preset != nullptr ? preset->modelId : "float32";
}

const char* modbusModelLabel(uint8_t vendor, uint8_t model) {
  const ModbusPresetInfo* preset = findModbusPreset(vendor, model);
  return preset != nullptr ? preset->modelLabel : "Generic 32-bit float";
}

int parseModbusVendor(const String& value) {
  for (size_t i = 0; i < MODBUS_PRESET_COUNT; ++i) {
    if (value == MODBUS_PRESETS[i].vendorId) {
      return MODBUS_PRESETS[i].vendor;
    }
  }
  return -1;
}

int parseModbusModel(uint8_t vendor, const String& value) {
  for (size_t i = 0; i < MODBUS_PRESET_COUNT; ++i) {
    if (MODBUS_PRESETS[i].vendor == vendor && value == MODBUS_PRESETS[i].modelId) {
      return MODBUS_PRESETS[i].model;
    }
  }
  return -1;
}

void applyModbusPreset(ModbusDeviceConfig& device,
                       uint8_t vendor,
                       uint8_t model) {
  const ModbusPresetInfo* preset = findModbusPreset(vendor, model);
  if (preset == nullptr) return;

  device.vendor = preset->vendor;
  device.model = preset->model;
  device.address = preset->defaultAddress;
  device.baudRate = preset->defaultBaudRate;
  device.warmupSeconds = preset->defaultWarmupSeconds;
  device.valueRegister = preset->valueRegister;
  device.functionCode = preset->functionCode;
  device.registerCount = preset->registerCount;
  device.valueFormat = preset->valueFormat;
}

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

struct SisgeoLoraFieldInfo {
  const char* id;
  const char* label;
  const char* unit;
  uint8_t bytes;
};

static constexpr SisgeoLoraFieldInfo SISGEO_LORA_FIELDS[SISGEO_LORA_FIELD_COUNT] = {
  { "value1", "Value 1", "", 4 },
  { "value2", "Value 2", "", 4 },
  { "temperature_c", "Sensor temperature", "C", 4 },
  { "humidity_pct", "Sensor humidity", "%RH", 4 },
  { "supply_voltage_v", "Supply voltage", "V", 4 }
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

uint32_t loraPayloadSizeV6(uint32_t fieldMask,
                           const uint8_t* sisgeoFieldMasks,
                           uint16_t sisgeoDeviceMask) {
  uint32_t size = 8;
  for (uint8_t id = 0; id < LORA_FIELD_COUNT; ++id) {
    if ((fieldMask & (1UL << id)) != 0) size += LORA_FIELDS[id].bytes;
  }
  for (uint8_t device = 0; device < MODBUS_MAX_DEVICES; ++device) {
    if ((sisgeoDeviceMask & (1U << device)) == 0) continue;
    size += 1;
    const uint8_t fieldMaskForDevice =
      sisgeoFieldMasks[device] & SISGEO_LORA_FIELD_MASK_ALL;
    for (uint8_t field = 0; field < SISGEO_LORA_FIELD_COUNT; ++field) {
      if ((fieldMaskForDevice & (1U << field)) != 0) {
        size += SISGEO_LORA_FIELDS[field].bytes;
      }
    }
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
  bool modbusAlwaysOn = false;
  uint16_t modbusPollSeconds = 2;
  uint8_t modbusDeviceCount = 1;
  ModbusDeviceConfig modbusDevices[MODBUS_MAX_DEVICES] = {
    { 1, MODBUS_VENDOR_SENSECAP, SENSECAP_S700_BC, 60, 9600, 10, 0, 4, 0, 0 },
    { 0, MODBUS_VENDOR_SENSECAP, SENSECAP_S700_BC, 60, 9600, 10, 0, 4, 0, 0 },
    { 0, MODBUS_VENDOR_SENSECAP, SENSECAP_S700_BC, 60, 9600, 10, 0, 4, 0, 0 },
    { 0, MODBUS_VENDOR_SENSECAP, SENSECAP_S700_BC, 60, 9600, 10, 0, 4, 0, 0 }
  };
  uint32_t loraFieldMask = LORA_FIELD_MASK_ALL;
  uint8_t loraSisgeoFieldMasks[MODBUS_MAX_DEVICES] = { 0 };

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
bool readSenseCapMeasurements(const ModbusDeviceConfig& device,
                             ModbusMeasurements& output);

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

struct ModbusDeviceRuntime {
  bool valid = false;
  bool complete = false;
  uint32_t lastAttemptMs = 0;
  uint32_t lastSuccessMs = 0;
  uint8_t lastException = 0;
  String lastError;
  String lastRequestHex;
  String lastResponseHex;
  ModbusMeasurements senseCap;
  float value1 = NAN;
  float value2 = NAN;
  float temperatureC = NAN;
  float humidityPct = NAN;
  float supplyVoltageV = NAN;
};

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
bool webServerReady = false;
bool webServiceActive = false;

bool adsAvailable = false;
bool dsAvailable = false;
bool inaAvailable = false;
bool oledAvailable = false;
bool sensorPowerEnabled = false;
bool sensorInterfacesReady = false;
bool fieldMeasurementPending = false;
bool sensorReadInProgress = false;
bool sensorReadRequested = false;
bool modbusInterfaceReady = false;
uint32_t modbusPowerOnAtMs = 0;
ModbusDeviceRuntime modbusRuntime[MODBUS_MAX_DEVICES];
ModbusDeviceConfig* activeModbusDevice = nullptr;
ModbusDeviceRuntime* activeModbusRuntime = nullptr;
uint8_t modbusPrimaryIndex = 0;
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
bool anyModbusDeviceEnabled();
void serviceWebWhileWaiting();
void cooperativeDelay(uint32_t durationMs);

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
bool anyModbusDeviceEnabled() {
  if (!cfg.modbusEnabled) return false;

  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    if (cfg.modbusDevices[index].enabled != 0) return true;
  }
  return false;
}

bool anySenseCapDeviceEnabled() {
  if (!cfg.modbusEnabled) return false;

  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    const ModbusDeviceConfig& device = cfg.modbusDevices[index];
    if (device.enabled != 0 && device.vendor == MODBUS_VENDOR_SENSECAP) {
      return true;
    }
  }
  return false;
}

bool anySenseCapDeviceEnabled(const Config& settings) {
  if (!settings.modbusEnabled) return false;

  for (uint8_t index = 0; index < settings.modbusDeviceCount; ++index) {
    const ModbusDeviceConfig& device = settings.modbusDevices[index];
    if (device.enabled != 0 && device.vendor == MODBUS_VENDOR_SENSECAP) {
      return true;
    }
  }
  return false;
}

uint16_t loraSisgeoDeviceMask(const Config& settings) {
  if (!settings.modbusEnabled) return 0;

  uint16_t deviceMask = 0;
  for (uint8_t index = 0; index < settings.modbusDeviceCount; ++index) {
    const ModbusDeviceConfig& device = settings.modbusDevices[index];
    if (device.enabled != 0 && device.vendor == MODBUS_VENDOR_SISGEO &&
        (settings.loraSisgeoFieldMasks[index] & SISGEO_LORA_FIELD_MASK_ALL) != 0) {
      deviceMask |= static_cast<uint16_t>(1U << index);
    }
  }
  return deviceMask;
}

uint32_t loraBaseFieldMask(const Config& settings) {
  uint32_t fieldMask = settings.loraFieldMask & LORA_FIELD_MASK_ALL;
  if (!anySenseCapDeviceEnabled(settings)) {
    fieldMask &= (1UL << 9) - 1UL;
  }
  return fieldMask;
}

uint16_t loraSisgeoDeviceMask() {
  return loraSisgeoDeviceMask(cfg);
}

uint32_t loraBaseFieldMask() {
  return loraBaseFieldMask(cfg);
}

uint32_t configuredLoraPayloadSize(const Config& settings) {
  const uint16_t sisgeoDeviceMask = loraSisgeoDeviceMask(settings);
  if (sisgeoDeviceMask == 0) return loraPayloadSize(loraBaseFieldMask(settings));
  return loraPayloadSizeV6(loraBaseFieldMask(settings),
                           settings.loraSisgeoFieldMasks,
                           sisgeoDeviceMask);
}

uint32_t configuredLoraPayloadSize() {
  return configuredLoraPayloadSize(cfg);
}

bool anyModbusRuntimeValid() {
  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    if (cfg.modbusDevices[index].enabled != 0 && modbusRuntime[index].valid) {
      return true;
    }
  }
  return false;
}

bool allEnabledModbusRuntimeComplete() {
  bool hasEnabledDevice = false;
  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    if (cfg.modbusDevices[index].enabled == 0) continue;
    hasEnabledDevice = true;
    if (!modbusRuntime[index].valid || !modbusRuntime[index].complete) return false;
  }
  return hasEnabledDevice;
}

bool modbusDeviceConfigEqual(const ModbusDeviceConfig& left,
                             const ModbusDeviceConfig& right) {
  return left.enabled == right.enabled &&
         left.vendor == right.vendor &&
         left.model == right.model &&
         left.address == right.address &&
         left.baudRate == right.baudRate &&
         left.warmupSeconds == right.warmupSeconds &&
         left.valueRegister == right.valueRegister &&
         left.functionCode == right.functionCode &&
         left.registerCount == right.registerCount &&
         left.valueFormat == right.valueFormat;
}

void normalizeModbusDevice(ModbusDeviceConfig& device) {
  if (device.vendor >= MODBUS_VENDOR_COUNT ||
      findModbusPreset(device.vendor, device.model) == nullptr) {
    applyModbusPreset(device, MODBUS_VENDOR_SENSECAP, SENSECAP_S700_BC);
  }

  device.enabled = device.enabled != 0 ? 1 : 0;
  device.address = constrain(device.address,
                             static_cast<uint8_t>(1),
                             static_cast<uint8_t>(247));
  if (!isSupportedModbusBaud(device.baudRate)) device.baudRate = 9600;
  device.warmupSeconds = constrain(device.warmupSeconds,
                                   static_cast<uint16_t>(0),
                                   static_cast<uint16_t>(3600));
  if (device.functionCode != 3 && device.functionCode != 4) {
    device.functionCode = 4;
  }
  device.registerCount = constrain(device.registerCount,
                                   static_cast<uint8_t>(1),
                                   static_cast<uint8_t>(MODBUS_MAX_REGISTERS));
  if (device.valueFormat > MODBUS_VALUE_UINT32) {
    device.valueFormat = MODBUS_VALUE_FLOAT32;
  }
}

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
  cfg.modbusAlwaysOn = preferences.getBool("mbAlwaysOn", cfg.modbusAlwaysOn);
  cfg.modbusPollSeconds = static_cast<uint16_t>(
    preferences.getUInt("mbPoll", cfg.modbusPollSeconds)
  );
  cfg.modbusDeviceCount = preferences.getUChar("mbCount", cfg.modbusDeviceCount);
  const size_t storedModbusBytes = preferences.getBytesLength("mbDevices");
  if (storedModbusBytes == sizeof(cfg.modbusDevices)) {
    preferences.getBytes("mbDevices", cfg.modbusDevices, sizeof(cfg.modbusDevices));
  } else if (storedModbusBytes ==
             sizeof(ModbusDeviceConfig) * MODBUS_LEGACY_MAX_DEVICES) {
    ModbusDeviceConfig legacyDevices[MODBUS_LEGACY_MAX_DEVICES] = {};
    preferences.getBytes("mbDevices", legacyDevices, sizeof(legacyDevices));
    for (uint8_t index = 0; index < MODBUS_LEGACY_MAX_DEVICES; ++index) {
      cfg.modbusDevices[index] = legacyDevices[index];
    }
    cfg.modbusDeviceCount = constrain(cfg.modbusDeviceCount,
                                      static_cast<uint8_t>(1),
                                      MODBUS_LEGACY_MAX_DEVICES);
  } else {
    const uint8_t model = preferences.getUChar("mbModel", SENSECAP_S700_BC);
    const uint8_t address = preferences.getUChar("mbAddress", 60);
    const uint32_t baudRate = preferences.getUInt("mbBaud", 9600);
    const uint16_t warmupSeconds = static_cast<uint16_t>(
      preferences.getUInt("mbWarmup", 10)
    );
    cfg.modbusDevices[0] = {
      static_cast<uint8_t>(cfg.modbusEnabled ? 1 : 0),
      MODBUS_VENDOR_SENSECAP,
      model,
      address,
      baudRate,
      warmupSeconds,
      0,
      4,
      0,
      MODBUS_VALUE_FLOAT32
    };
  }
  cfg.loraFieldMask = preferences.getUInt("loraMask", cfg.loraFieldMask);
  if (preferences.getBytesLength("loraSisgeo") == sizeof(cfg.loraSisgeoFieldMasks)) {
    preferences.getBytes("loraSisgeo",
                         cfg.loraSisgeoFieldMasks,
                         sizeof(cfg.loraSisgeoFieldMasks));
  }
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
  cfg.modbusDeviceCount = constrain(cfg.modbusDeviceCount,
                                    static_cast<uint8_t>(1),
                                    MODBUS_MAX_DEVICES);
  cfg.modbusPollSeconds = constrain(cfg.modbusPollSeconds,
                                    static_cast<uint16_t>(2),
                                    static_cast<uint16_t>(3600));
  for (uint8_t index = 0; index < MODBUS_MAX_DEVICES; ++index) {
    normalizeModbusDevice(cfg.modbusDevices[index]);
  }
  cfg.loraFieldMask &= LORA_FIELD_MASK_ALL;
  for (uint8_t index = 0; index < MODBUS_MAX_DEVICES; ++index) {
    cfg.loraSisgeoFieldMasks[index] &= SISGEO_LORA_FIELD_MASK_ALL;
  }

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
  preferences.putBool("mbAlwaysOn", cfg.modbusAlwaysOn);
  preferences.putUInt("mbPoll", cfg.modbusPollSeconds);
  preferences.putUChar("mbCount", cfg.modbusDeviceCount);
  preferences.putBytes("mbDevices", cfg.modbusDevices, sizeof(cfg.modbusDevices));
  // Keep the legacy keys for older firmware builds that may be flashed later.
  preferences.putUChar("mbModel", cfg.modbusDevices[0].model);
  preferences.putUChar("mbAddress", cfg.modbusDevices[0].address);
  preferences.putUInt("mbBaud", cfg.modbusDevices[0].baudRate);
  preferences.putUInt("mbWarmup", cfg.modbusDevices[0].warmupSeconds);
  preferences.putUInt("loraMask", cfg.loraFieldMask);
  preferences.putBytes("loraSisgeo",
                       cfg.loraSisgeoFieldMasks,
                       sizeof(cfg.loraSisgeoFieldMasks));
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
static constexpr size_t MODBUS_RX_BUFFER_SIZE = 128;
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
  if (activeModbusRuntime != nullptr) {
    activeModbusRuntime->lastError = message;
  }
}

uint32_t modbusResponseTimeoutMs(uint16_t quantity) {
  (void)quantity;
  return 2000UL;
}

bool initializeModbusInterface(const ModbusDeviceConfig& device) {
  rs485Serial.end();

  pinMode(PIN_RS485_RE_DE, OUTPUT);
  digitalWrite(PIN_RS485_RE_DE, LOW);
  pinMode(PIN_RS485_DI, OUTPUT);
  digitalWrite(PIN_RS485_DI, LOW);
  pinMode(PIN_RS485_RO, INPUT);

  if (cfg.modbusEnabled && device.enabled != 0) {
    rs485Serial.begin(device.baudRate,
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
  if (!cfg.modbusEnabled || activeModbusDevice == nullptr ||
      activeModbusDevice->enabled == 0) {
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

  const uint8_t address = activeModbusDevice->address;
  const uint8_t functionCode = activeModbusDevice->functionCode;
  uint8_t request[8] = {
    address,
    functionCode,
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
                address,
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

  uint32_t lastWebServiceMs = millis();
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
        if (receiveBuffer[position] != address) continue;

        const uint8_t function = receiveBuffer[position + 1];
        size_t candidateLength = 0;
        if (function == (functionCode | 0x80U)) {
          candidateLength = 5;
        } else if (function == functionCode) {
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
    if (millis() - lastWebServiceMs >= 5) {
      serviceWebWhileWaiting();
      lastWebServiceMs = millis();
    }
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
  if (frame[1] != functionCode) {
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
        cooperativeDelay(MODBUS_DATA_READY_RETRY_DELAY_MS);
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
      cooperativeDelay(MODBUS_DATA_READY_RETRY_DELAY_MS);
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

bool readSenseCapMeasurements(const ModbusDeviceConfig& device,
                              ModbusMeasurements& output) {
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

  switch (device.model) {
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
      cooperativeDelay(50);
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
  if (!anyModbusDeviceEnabled()) return false;
  if (lastModbusPollMs == 0) return true;
  return millis() - lastModbusPollMs >=
    static_cast<uint32_t>(cfg.modbusPollSeconds) * 1000UL;
}

bool readModbusFloat(uint16_t registerAddress, float& value) {
  uint16_t registers[2] = { 0, 0 };
  if (!modbusReadInputRegisters(registerAddress, 2, registers)) return false;

  const uint32_t bits = (static_cast<uint32_t>(registers[0]) << 16) |
                        static_cast<uint32_t>(registers[1]);
  memcpy(&value, &bits, sizeof(value));
  return isfinite(value);
}

bool readModbusValue(const ModbusDeviceConfig& device,
                     uint16_t registerAddress,
                     float& value) {
  if (device.valueFormat == MODBUS_VALUE_FLOAT32) {
    return readModbusFloat(registerAddress, value);
  }

  if (device.valueFormat == MODBUS_VALUE_UINT32) {
    uint16_t registers[2] = { 0, 0 };
    if (!modbusReadInputRegisters(registerAddress, 2, registers)) return false;
    const uint32_t raw = (static_cast<uint32_t>(registers[0]) << 16) |
                         static_cast<uint32_t>(registers[1]);
    value = static_cast<float>(raw);
    return true;
  }

  uint16_t registers[1] = { 0 };
  if (!modbusReadInputRegisters(registerAddress, 1, registers)) return false;
  value = device.valueFormat == MODBUS_VALUE_INT16
    ? static_cast<float>(static_cast<int16_t>(registers[0]))
    : static_cast<float>(registers[0]);
  return true;
}

bool readGenericModbusDevice(const ModbusDeviceConfig& device,
                             ModbusDeviceRuntime& runtime) {
  const uint8_t requiredRegisters =
    device.valueFormat == MODBUS_VALUE_FLOAT32 ||
    device.valueFormat == MODBUS_VALUE_UINT32
      ? 2
      : 1;
  if (device.registerCount < requiredRegisters) {
    setModbusError("unsupported value format");
    return false;
  }

  bool primaryValid = readModbusValue(device, device.valueRegister, runtime.value1);
  bool complete = primaryValid;

  if (device.vendor == MODBUS_VENDOR_SISGEO) {
    if (!readModbusValue(device, device.valueRegister + 2, runtime.value2)) complete = false;
    if (!readModbusValue(device, 0x021A, runtime.temperatureC)) complete = false;
    if (!readModbusValue(device, 0x021C, runtime.humidityPct)) complete = false;
    if (!readModbusValue(device, 0x021E, runtime.supplyVoltageV)) complete = false;
  }

  runtime.valid = primaryValid;
  runtime.complete = complete;
  return primaryValid;
}

void resetModbusRuntime(ModbusDeviceRuntime& runtime) {
  runtime.valid = false;
  runtime.complete = false;
  runtime.lastAttemptMs = millis();
  runtime.lastSuccessMs = 0;
  runtime.lastException = 0;
  runtime.lastError = "";
  runtime.lastRequestHex = "";
  runtime.lastResponseHex = "";
  runtime.senseCap = ModbusMeasurements();
  runtime.value1 = NAN;
  runtime.value2 = NAN;
  runtime.temperatureC = NAN;
  runtime.humidityPct = NAN;
  runtime.supplyVoltageV = NAN;
}

void waitForModbusDeviceWarmup(const ModbusDeviceConfig& device) {
  if (cfg.modbusAlwaysOn || device.warmupSeconds == 0 || modbusPowerOnAtMs == 0) {
    return;
  }

  const uint32_t requiredMs = static_cast<uint32_t>(device.warmupSeconds) * 1000UL;
  const uint32_t elapsedMs = millis() - modbusPowerOnAtMs;
  if (elapsedMs < requiredMs) {
    const uint32_t remainingMs = requiredMs - elapsedMs;
    Serial.printf("[MODBUS] %s warm-up: %lu ms\n",
                  modbusModelLabel(device.vendor, device.model),
                  static_cast<unsigned long>(remainingMs));
    cooperativeDelay(remainingMs);
  }
}

void pollModbusDevices(ModbusMeasurements& primaryMeasurements) {
  primaryMeasurements = ModbusMeasurements();
  lastModbusPollMs = millis();
  bool primaryAssigned = false;
  modbusPrimaryIndex = 0;

  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    ModbusDeviceConfig& device = cfg.modbusDevices[index];
    ModbusDeviceRuntime& runtime = modbusRuntime[index];
    resetModbusRuntime(runtime);
    if (device.enabled == 0) continue;

    waitForModbusDeviceWarmup(device);
    activeModbusDevice = &device;
    activeModbusRuntime = &runtime;
    modbusLastError = "";
    modbusLastRequestHex = "";
    modbusLastResponseHex = "";
    modbusLastException = 0;

    initializeModbusInterface(device);
    bool valid = false;
    if (device.vendor == MODBUS_VENDOR_SENSECAP) {
      valid = readSenseCapMeasurements(device, runtime.senseCap);
      runtime.valid = runtime.senseCap.valid;
      runtime.complete = runtime.senseCap.complete;
      if (valid && !primaryAssigned) {
        primaryMeasurements = runtime.senseCap;
        modbusPrimaryIndex = index;
        primaryAssigned = true;
      }
    } else {
      valid = readGenericModbusDevice(device, runtime);
    }

    runtime.lastError = modbusLastError;
    runtime.lastException = modbusLastException;
    runtime.lastRequestHex = modbusLastRequestHex;
    runtime.lastResponseHex = modbusLastResponseHex;
    if (valid) runtime.lastSuccessMs = millis();
    Serial.printf("[MODBUS] device %u %s: %s\n",
                  static_cast<unsigned>(index + 1),
                  modbusModelLabel(device.vendor, device.model),
                  valid ? "READY" : runtime.lastError.c_str());
  }

  activeModbusDevice = nullptr;
  activeModbusRuntime = nullptr;
  disableModbusInterface();

  if (primaryAssigned) {
    lastModbusSuccessMs = millis();
    modbusLastError = "";
  } else if (anyModbusDeviceEnabled()) {
    modbusLastError = "no valid Modbus device";
  }
}

void initializePoweredSensorInterfaces() {
  dsAvailable = initializeDS18B20();
  inaAvailable = initializeINA226();
  adsAvailable = initializeADS1220();
  modbusInterfaceReady = false;
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
    cooperativeDelay(50);
    modbusPowerOnAtMs = millis();
    sensorInterfacesReady = false;
    Serial.println("[SENSORS] external power ON");
  }

  if (!sensorInterfacesReady) initializePoweredSensorInterfaces();
}

// =========================== Sensor cache ===========================
void readAllSensors() {
  if (sensorReadInProgress) return;
  sensorReadInProgress = true;

  powerExternalSensorsOn();

  Measurements next;

  next.temperatureValid = readTemperature(next.temperatureC);
  next.wegValid = readWegsensor(next.wegRaw, next.wegVoltageV, next.wegPositionMm);
  next.inaValid = readINA226(next.inaBusVoltageV, next.inaShuntVoltageMv,
                                next.inaCurrentMa, next.inaPowerW);
  if (!anyModbusDeviceEnabled()) {
    next.modbus = ModbusMeasurements();
  } else if (modbusPollDue()) {
    pollModbusDevices(next.modbus);
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
                  modbusModelLabel(cfg.modbusDevices[modbusPrimaryIndex].vendor,
                                   cfg.modbusDevices[modbusPrimaryIndex].model),
                  cached.modbus.airTemperatureC,
                  cached.modbus.humidityPct,
                  cached.modbus.pressurePa,
                  cached.modbus.averageWindSpeedMs);
  } else if (anyModbusDeviceEnabled()) {
    Serial.printf("  Modbus: %s\n", modbusLastError.c_str());
  }
  if (cached.batteryValid) Serial.printf("  battery: %u mV / %.3f V / %u %%\n",
                                         cached.batteryMillivolts,
                                         cached.batteryVoltageV,
                                         cached.batteryPercent);

  sensorReadInProgress = false;
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

void serviceWebWhileWaiting() {
  if (!apMode || !webServerReady || webServiceActive) return;

  webServiceActive = true;
  if (apDnsReady) apDnsServer.processNextRequest();
  server.handleClient();
  webServiceActive = false;
  yield();
}

void cooperativeDelay(uint32_t durationMs) {
  const uint32_t started = millis();
  while (millis() - started < durationMs) {
    serviceWebWhileWaiting();
    delay(1);
  }
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

  const uint8_t primaryIndex = modbusPrimaryIndex < cfg.modbusDeviceCount
    ? modbusPrimaryIndex
    : 0;
  const ModbusDeviceConfig& primaryDevice = cfg.modbusDevices[primaryIndex];
  const ModbusDeviceRuntime& primaryRuntime = modbusRuntime[primaryIndex];
  const ModbusPresetInfo* primaryPreset =
    findModbusPreset(primaryDevice.vendor, primaryDevice.model);
  bool anyRuntimeValid = false;
  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    if (modbusRuntime[index].valid) {
      anyRuntimeValid = true;
      break;
    }
  }

  JsonObject modbusState = doc["modbus"].to<JsonObject>();
  modbusState["enabled"] = cfg.modbusEnabled;
  modbusState["always_on"] = cfg.modbusAlwaysOn;
  modbusState["device_count"] = cfg.modbusDeviceCount;
  modbusState["interface_ready"] = modbusInterfaceReady;
  modbusState["connected"] = sensorPowerEnabled &&
    (cached.modbus.valid || anyRuntimeValid);
  modbusState["complete"] = cached.modbus.valid
    ? cached.modbus.complete
    : primaryRuntime.complete;
  modbusState["vendor"] = modbusVendorId(primaryDevice.vendor);
  modbusState["vendor_label"] = modbusVendorLabel(primaryDevice.vendor);
  modbusState["model"] = modbusModelId(primaryDevice.vendor, primaryDevice.model);
  modbusState["model_label"] = modbusModelLabel(primaryDevice.vendor, primaryDevice.model);
  modbusState["factory_address"] = primaryPreset != nullptr
    ? primaryPreset->defaultAddress
    : primaryDevice.address;
  modbusState["address"] = primaryDevice.address;
  modbusState["baud"] = primaryDevice.baudRate;
  modbusState["warmup_seconds"] = primaryDevice.warmupSeconds;
  modbusState["poll_seconds"] = cfg.modbusPollSeconds;
  modbusState["protocol"] = "RS-485 Modbus RTU";
  modbusState["function_code"] = primaryDevice.functionCode;
  modbusState["format"] = "8N1";
  modbusState["re_de_pin"] = PIN_RS485_RE_DE;
  modbusState["tx_pin"] = PIN_RS485_DI;
  modbusState["rx_pin"] = PIN_RS485_RO;
  modbusState["cable"] = primaryDevice.vendor == MODBUS_VENDOR_SENSECAP
    ? "SenseCAP M12: A pin 1 white | B pin 7 blue | power pin 8 red | 0 V pin 2 brown"
    : primaryDevice.vendor == MODBUS_VENDOR_SISGEO
      ? "SISGEO RS-485 cable: use the sensor wiring diagram"
      : "Custom device: use the vendor wiring diagram";
  modbusState["last_error"] = primaryRuntime.lastError.length() > 0
    ? primaryRuntime.lastError
    : modbusLastError;
  modbusState["last_exception"] = primaryRuntime.lastException != 0
    ? primaryRuntime.lastException
    : modbusLastException;
  modbusState["last_request"] = primaryRuntime.lastRequestHex.length() > 0
    ? primaryRuntime.lastRequestHex
    : modbusLastRequestHex;
  modbusState["last_response"] = primaryRuntime.lastResponseHex.length() > 0
    ? primaryRuntime.lastResponseHex
    : modbusLastResponseHex;
  modbusState["last_attempt_age_s"] = lastModbusPollMs > 0
    ? (millis() - lastModbusPollMs) / 1000UL
    : 0;
  modbusState["last_success_age_s"] = primaryRuntime.lastSuccessMs > 0
    ? (millis() - primaryRuntime.lastSuccessMs) / 1000UL
    : (lastModbusSuccessMs > 0
      ? (millis() - lastModbusSuccessMs) / 1000UL
      : 0);

  JsonArray presetCatalog = doc["modbus_presets"].to<JsonArray>();
  for (size_t index = 0; index < MODBUS_PRESET_COUNT; ++index) {
    const ModbusPresetInfo& preset = MODBUS_PRESETS[index];
    JsonObject item = presetCatalog.add<JsonObject>();
    item["vendor"] = preset.vendorId;
    item["vendor_label"] = preset.vendorLabel;
    item["model"] = preset.modelId;
    item["model_label"] = preset.modelLabel;
    item["address"] = preset.defaultAddress;
    item["baud"] = preset.defaultBaudRate;
    item["warmup_seconds"] = preset.defaultWarmupSeconds;
    item["value_register"] = preset.valueRegister;
    item["function_code"] = preset.functionCode;
    item["register_count"] = preset.registerCount;
    item["value_format"] = preset.valueFormat;
  }

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

  JsonArray modbusDevices = modbusState["devices"].to<JsonArray>();
  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    const ModbusDeviceConfig& device = cfg.modbusDevices[index];
    const ModbusDeviceRuntime& runtime = modbusRuntime[index];
    JsonObject item = modbusDevices.add<JsonObject>();
    item["index"] = index;
    item["enabled"] = device.enabled != 0;
    item["vendor"] = modbusVendorId(device.vendor);
    item["vendor_label"] = modbusVendorLabel(device.vendor);
    item["model"] = modbusModelId(device.vendor, device.model);
    item["model_label"] = modbusModelLabel(device.vendor, device.model);
    item["address"] = device.address;
    item["baud"] = device.baudRate;
    item["warmup_seconds"] = device.warmupSeconds;
    item["lora_field_mask"] = cfg.loraSisgeoFieldMasks[index];
    item["valid"] = runtime.valid;
    item["complete"] = runtime.complete;
    item["last_error"] = runtime.lastError;
    item["last_request"] = runtime.lastRequestHex;
    item["last_response"] = runtime.lastResponseHex;
    item["last_success_age_s"] = runtime.lastSuccessMs > 0
      ? (millis() - runtime.lastSuccessMs) / 1000UL
      : 0;
    JsonObject itemData = item["data"].to<JsonObject>();
    if (device.vendor == MODBUS_VENDOR_SENSECAP) {
      if (isfinite(runtime.senseCap.airTemperatureC)) {
        itemData["air_temperature_c"] = runtime.senseCap.airTemperatureC;
      }
      if (isfinite(runtime.senseCap.humidityPct)) {
        itemData["humidity_pct"] = runtime.senseCap.humidityPct;
      }
      if (isfinite(runtime.senseCap.pressurePa)) {
        itemData["pressure_pa"] = runtime.senseCap.pressurePa;
      }
    } else {
      if (isfinite(runtime.value1)) itemData["value1"] = runtime.value1;
      if (isfinite(runtime.value2)) itemData["value2"] = runtime.value2;
      if (isfinite(runtime.temperatureC)) itemData["temperature_c"] = runtime.temperatureC;
      if (isfinite(runtime.humidityPct)) itemData["humidity_pct"] = runtime.humidityPct;
      if (isfinite(runtime.supplyVoltageV)) itemData["supply_voltage_v"] = runtime.supplyVoltageV;
    }
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
  modbusConfig["always_on"] = cfg.modbusAlwaysOn;
  modbusConfig["poll_seconds"] = cfg.modbusPollSeconds;
  modbusConfig["device_count"] = cfg.modbusDeviceCount;
  modbusConfig["vendor"] = modbusVendorId(primaryDevice.vendor);
  modbusConfig["model"] = modbusModelId(primaryDevice.vendor, primaryDevice.model);
  modbusConfig["address"] = primaryDevice.address;
  modbusConfig["baud"] = primaryDevice.baudRate;
  modbusConfig["warmup_seconds"] = primaryDevice.warmupSeconds;
  JsonArray configDevices = modbusConfig["devices"].to<JsonArray>();
  for (uint8_t index = 0; index < cfg.modbusDeviceCount; ++index) {
    const ModbusDeviceConfig& device = cfg.modbusDevices[index];
    JsonObject item = configDevices.add<JsonObject>();
    item["enabled"] = device.enabled != 0;
    item["vendor"] = modbusVendorId(device.vendor);
    item["model"] = modbusModelId(device.vendor, device.model);
    item["address"] = device.address;
    item["baud"] = device.baudRate;
    item["warmup_seconds"] = device.warmupSeconds;
    item["lora_field_mask"] = cfg.loraSisgeoFieldMasks[index];
    item["value_register"] = device.valueRegister;
    item["function_code"] = device.functionCode;
    item["register_count"] = device.registerCount;
    item["value_format"] = device.valueFormat;
  }

  JsonObject loraConfig = config["lora"].to<JsonObject>();
  const uint16_t sisgeoDeviceMask = loraSisgeoDeviceMask();
  loraConfig["version"] = sisgeoDeviceMask != 0
    ? LORA_PAYLOAD_VERSION_V6
    : LORA_PAYLOAD_VERSION_V5;
  loraConfig["field_mask"] = loraBaseFieldMask();
  loraConfig["payload_bytes"] = configuredLoraPayloadSize();
  loraConfig["sisgeo_device_mask"] = sisgeoDeviceMask;
  JsonArray sisgeoMasks = loraConfig["sisgeo_field_masks"].to<JsonArray>();
  for (uint8_t index = 0; index < MODBUS_MAX_DEVICES; ++index) {
    sisgeoMasks.add(cfg.loraSisgeoFieldMasks[index]);
  }

  JsonArray loraFields = doc["lora_fields"].to<JsonArray>();
  for (uint8_t id = 0; id < LORA_FIELD_COUNT; ++id) {
    JsonObject field = loraFields.add<JsonObject>();
    field["id"] = LORA_FIELDS[id].id;
    field["label"] = LORA_FIELDS[id].label;
    field["unit"] = LORA_FIELDS[id].unit;
    field["source"] = LORA_FIELDS[id].source;
    field["bytes"] = LORA_FIELDS[id].bytes;
    field["selected"] = (loraBaseFieldMask() & (1UL << id)) != 0;
  }

  JsonArray sisgeoFields = doc["lora_sisgeo_fields"].to<JsonArray>();
  for (uint8_t id = 0; id < SISGEO_LORA_FIELD_COUNT; ++id) {
    JsonObject field = sisgeoFields.add<JsonObject>();
    field["id"] = SISGEO_LORA_FIELDS[id].id;
    field["label"] = SISGEO_LORA_FIELDS[id].label;
    field["unit"] = SISGEO_LORA_FIELDS[id].unit;
    field["bytes"] = SISGEO_LORA_FIELDS[id].bytes;
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
    if (modbus["always_on"].is<bool>()) {
      updated.modbusAlwaysOn = modbus["always_on"].as<bool>();
    }
    if (modbus["poll_seconds"].is<uint32_t>()) {
      const uint32_t value = modbus["poll_seconds"].as<uint32_t>();
      if (value < 2 || value > 3600) {
        server.send(400, "text/plain", "Modbus poll range 2..3600 seconds");
        return;
      }
      updated.modbusPollSeconds = static_cast<uint16_t>(value);
    }

    if (modbus["devices"].is<JsonArrayConst>()) {
      JsonArrayConst devices = modbus["devices"].as<JsonArrayConst>();
      if (devices.size() == 0 || devices.size() > MODBUS_MAX_DEVICES) {
        server.send(400, "text/plain", "Modbus device count range 1..12");
        return;
      }

      updated.modbusDeviceCount = static_cast<uint8_t>(devices.size());
      uint8_t senseCapCount = 0;
      uint8_t sisgeoCount = 0;
      uint8_t index = 0;
      for (JsonObjectConst source : devices) {
        ModbusDeviceConfig device = updated.modbusDevices[index];
        if (!source["vendor"].is<const char*>() ||
            !source["model"].is<const char*>()) {
          server.send(400, "text/plain", "Modbus vendor and model are required");
          return;
        }

        const int vendor = parseModbusVendor(source["vendor"].as<String>());
        const int model = vendor < 0
          ? -1
          : parseModbusModel(static_cast<uint8_t>(vendor),
                             source["model"].as<String>());
        if (model < 0) {
          server.send(400, "text/plain", "invalid Modbus vendor or model");
          return;
        }
        if (vendor == MODBUS_VENDOR_SENSECAP &&
            ++senseCapCount > MODBUS_MAX_SENSECAP_DEVICES) {
          server.send(400, "text/plain", "only one SenseCAP weather station is supported");
          return;
        }
        if (vendor == MODBUS_VENDOR_SISGEO &&
            ++sisgeoCount > MODBUS_MAX_SISGEO_DEVICES) {
          server.send(400, "text/plain", "maximum 12 SISGEO devices supported");
          return;
        }
        applyModbusPreset(device,
                          static_cast<uint8_t>(vendor),
                          static_cast<uint8_t>(model));

        if (source["enabled"].is<bool>()) {
          device.enabled = source["enabled"].as<bool>() ? 1 : 0;
        }
        if (!source["address"].isNull()) {
          const int value = source["address"].as<int>();
          if (value < 1 || value > 247) {
            server.send(400, "text/plain", "Modbus address range 1..247");
            return;
          }
          device.address = static_cast<uint8_t>(value);
        }
        if (!source["baud"].isNull()) {
          const uint32_t value = source["baud"].as<uint32_t>();
          if (!isSupportedModbusBaud(value)) {
            server.send(400, "text/plain", "unsupported Modbus baud rate");
            return;
          }
          device.baudRate = value;
        }
        if (!source["warmup_seconds"].isNull()) {
          const uint32_t value = source["warmup_seconds"].as<uint32_t>();
          if (value > 3600) {
            server.send(400, "text/plain", "Modbus warm-up range 0..3600 seconds");
            return;
          }
          device.warmupSeconds = static_cast<uint16_t>(value);
        }
        if (!source["value_register"].isNull()) {
          device.valueRegister = source["value_register"].as<uint16_t>();
        }
        if (!source["function_code"].isNull()) {
          const uint8_t value = source["function_code"].as<uint8_t>();
          if (value != 3 && value != 4) {
            server.send(400, "text/plain", "Modbus function must be 3 or 4");
            return;
          }
          device.functionCode = value;
        }
        if (!source["register_count"].isNull()) {
          const uint8_t value = source["register_count"].as<uint8_t>();
          if (value == 0 || value > MODBUS_MAX_REGISTERS) {
            server.send(400, "text/plain", "Modbus register count range 1..32");
            return;
          }
          device.registerCount = value;
        }
        if (!source["value_format"].isNull()) {
          const uint8_t value = source["value_format"].as<uint8_t>();
          if (value > MODBUS_VALUE_UINT32) {
            server.send(400, "text/plain", "unsupported Modbus value format");
            return;
          }
          device.valueFormat = value;
        }

        normalizeModbusDevice(device);
        for (uint8_t previous = 0; previous < index; ++previous) {
          if (device.enabled != 0 &&
              updated.modbusDevices[previous].enabled != 0 &&
              device.address == updated.modbusDevices[previous].address) {
            server.send(400, "text/plain", "Modbus device addresses must be unique");
            return;
          }
        }
        updated.modbusDevices[index++] = device;
      }
    } else {
      ModbusDeviceConfig& device = updated.modbusDevices[0];
      uint8_t vendor = MODBUS_VENDOR_SENSECAP;
      if (modbus["vendor"].is<const char*>()) {
        const int parsedVendor = parseModbusVendor(modbus["vendor"].as<String>());
        if (parsedVendor < 0) {
          server.send(400, "text/plain", "invalid Modbus vendor");
          return;
        }
        vendor = static_cast<uint8_t>(parsedVendor);
      }
      if (modbus["model"].is<const char*>()) {
        const int model = parseModbusModel(vendor, modbus["model"].as<String>());
        if (model < 0) {
          server.send(400, "text/plain", "invalid Modbus model");
          return;
        }
        applyModbusPreset(device, vendor, static_cast<uint8_t>(model));
      }
      if (modbus["address"].is<int>()) {
        const int value = modbus["address"].as<int>();
        if (value < 1 || value > 247) {
          server.send(400, "text/plain", "Modbus address range 1..247");
          return;
        }
        device.address = static_cast<uint8_t>(value);
      }
      if (modbus["baud"].is<uint32_t>()) {
        const uint32_t value = modbus["baud"].as<uint32_t>();
        if (!isSupportedModbusBaud(value)) {
          server.send(400, "text/plain", "unsupported Modbus baud rate");
          return;
        }
        device.baudRate = value;
      }
      if (modbus["warmup_seconds"].is<uint32_t>()) {
        const uint32_t value = modbus["warmup_seconds"].as<uint32_t>();
        if (value > 3600) {
          server.send(400, "text/plain", "Modbus warm-up range 0..3600 seconds");
          return;
        }
        device.warmupSeconds = static_cast<uint16_t>(value);
      }
      normalizeModbusDevice(device);
    }
  }

  if (request["lora"].is<JsonObjectConst>()) {
    JsonObjectConst lora = request["lora"].as<JsonObjectConst>();
    if (lora["field_mask"].is<uint32_t>()) {
      uint32_t value = lora["field_mask"].as<uint32_t>();
      if ((value & ~LORA_FIELD_MASK_ALL) != 0) {
        server.send(400, "text/plain", "LoRa field selection is invalid");
        return;
      }
      if (lora["sisgeo_field_masks"].is<JsonArrayConst>()) {
        JsonArrayConst masks = lora["sisgeo_field_masks"].as<JsonArrayConst>();
        if (masks.size() > MODBUS_MAX_DEVICES) {
          server.send(400, "text/plain", "SISGEO field mask count is invalid");
          return;
        }
        for (uint8_t index = 0; index < MODBUS_MAX_DEVICES; ++index) {
          updated.loraSisgeoFieldMasks[index] = 0;
        }
        uint8_t index = 0;
        for (JsonVariantConst source : masks) {
          if (!source.is<uint32_t>()) {
            server.send(400, "text/plain", "SISGEO field mask is invalid");
            return;
          }
          const uint32_t mask = source.as<uint32_t>();
          if ((mask & ~static_cast<uint32_t>(SISGEO_LORA_FIELD_MASK_ALL)) != 0) {
            server.send(400, "text/plain", "SISGEO field selection is invalid");
            return;
          }
          updated.loraSisgeoFieldMasks[index++] = static_cast<uint8_t>(mask);
        }
      }
      if (!anySenseCapDeviceEnabled(updated)) value &= (1UL << 9) - 1UL;
      updated.loraFieldMask = value;
    }
    if (configuredLoraPayloadSize(updated) > 242) {
      server.send(400, "text/plain", "LoRa payload is too large");
      return;
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

  bool modbusConfigChanged =
    cfg.modbusEnabled != updated.modbusEnabled ||
    cfg.modbusAlwaysOn != updated.modbusAlwaysOn ||
    cfg.modbusPollSeconds != updated.modbusPollSeconds ||
    cfg.modbusDeviceCount != updated.modbusDeviceCount;
  for (uint8_t index = 0; index < MODBUS_MAX_DEVICES; ++index) {
    if (!modbusDeviceConfigEqual(cfg.modbusDevices[index], updated.modbusDevices[index])) {
      modbusConfigChanged = true;
      break;
    }
  }

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
    for (uint8_t index = 0; index < MODBUS_MAX_DEVICES; ++index) {
      resetModbusRuntime(modbusRuntime[index]);
    }
  }
  if (cached.batteryValid) {
    cached.batteryPercent = batteryPercentFromMillivolts(cached.batteryMillivolts);
  }
  if (sensorPowerEnabled) sensorReadRequested = true;

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
    powerExternalSensorsOn();
    sensorReadRequested = true;
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

void captivePortalProbe() {
  if (!cfg.autoRedirectEnabled) {
    server.send(204, "text/plain", "");
    return;
  }

  server.sendHeader("Cache-Control", "no-store");
  server.sendHeader("Location", "/", true);
  server.send(302, "text/plain", "Redirecting to MultiConnect");
}

void handleNotFound() {
  server.send(404, "text/plain", "Not found");
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

  server.on("/generate_204", HTTP_ANY, captivePortalProbe);
  server.on("/gen_204", HTTP_ANY, captivePortalProbe);
  server.on("/hotspot-detect.html", HTTP_ANY, captivePortalProbe);
  server.on("/connecttest.txt", HTTP_ANY, captivePortalProbe);
  server.on("/ncsi.txt", HTTP_ANY, captivePortalProbe);
  server.on("/redirect", HTTP_ANY, captivePortalProbe);
  server.on("/canonical.html", HTTP_ANY, captivePortalProbe);
  server.on("/success.txt", HTTP_ANY, captivePortalProbe);
  server.on("/fwlink", HTTP_ANY, captivePortalProbe);
  server.onNotFound(handleNotFound);

  server.begin();
  webServerReady = true;
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
// Payload V6 keeps the V5 fields and adds:
// 6..7 SISGEO device mask, bits 0..11
// For every selected SISGEO device: one field mask byte, then float32 values.
static constexpr uint8_t LORA_PAYLOAD_HEADER_SIZE_V5 = 6;
static constexpr uint8_t LORA_PAYLOAD_HEADER_SIZE_V6 = 8;
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

void putLoraFloat32(uint8_t& index, bool valid, float value) {
  uint32_t bits = 0x7FC00000UL;
  if (valid && isfinite(value)) memcpy(&bits, &value, sizeof(bits));
  putUInt32BE(index, bits);
}

void putSisgeoLoraField(uint8_t& index,
                        uint8_t field,
                        const ModbusDeviceRuntime& runtime) {
  switch (field) {
    case SISGEO_LORA_FIELD_VALUE1:
      putLoraFloat32(index, isfinite(runtime.value1), runtime.value1);
      break;
    case SISGEO_LORA_FIELD_VALUE2:
      putLoraFloat32(index, isfinite(runtime.value2), runtime.value2);
      break;
    case SISGEO_LORA_FIELD_TEMPERATURE:
      putLoraFloat32(index, isfinite(runtime.temperatureC), runtime.temperatureC);
      break;
    case SISGEO_LORA_FIELD_HUMIDITY:
      putLoraFloat32(index, isfinite(runtime.humidityPct), runtime.humidityPct);
      break;
    case SISGEO_LORA_FIELD_SUPPLY_VOLTAGE:
      putLoraFloat32(index, isfinite(runtime.supplyVoltageV), runtime.supplyVoltageV);
      break;
    default:
      break;
  }
}

static void prepareTxFrame(uint8_t port) {
  (void)port;

  const uint16_t sisgeoDeviceMask = loraSisgeoDeviceMask();
  const bool sisgeoPayload = sisgeoDeviceMask != 0;
  uint8_t index = 0;
  appData[index++] = sisgeoPayload
    ? LORA_PAYLOAD_VERSION_V6
    : LORA_PAYLOAD_VERSION_V5;

  uint8_t status = 0;
  if (cached.temperatureValid) status |= LORA_STATUS_TEMP;
  if (cached.wegValid) status |= LORA_STATUS_POSITION | LORA_STATUS_WEG;
  if (cached.inaValid) status |= LORA_STATUS_INA;
  if (cached.batteryValid) status |= LORA_STATUS_BATTERY;
  if (cached.modbus.valid || anyModbusRuntimeValid()) status |= LORA_STATUS_MODBUS;
  if (cached.modbus.complete || allEnabledModbusRuntimeComplete()) {
    status |= LORA_STATUS_MODBUS_COMPLETE;
  }
  appData[index++] = status;

  const uint32_t fieldMask = loraBaseFieldMask();
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

  if (sisgeoPayload) {
    putUInt16BE(index, sisgeoDeviceMask);
    for (uint8_t deviceIndex = 0; deviceIndex < cfg.modbusDeviceCount; ++deviceIndex) {
      if ((sisgeoDeviceMask & (1U << deviceIndex)) == 0) continue;

      const uint8_t fieldMaskForDevice =
        cfg.loraSisgeoFieldMasks[deviceIndex] & SISGEO_LORA_FIELD_MASK_ALL;
      appData[index++] = fieldMaskForDevice;
      for (uint8_t field = 0; field < SISGEO_LORA_FIELD_COUNT; ++field) {
        if ((fieldMaskForDevice & (1U << field)) == 0) continue;
        putSisgeoLoraField(index, field, modbusRuntime[deviceIndex]);
      }
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
  modbusPowerOnAtMs = 0;
  lastModbusPollMs = 0;
  lastModbusSuccessMs = 0;
  modbusLastError = "sensor power off";
  Serial.println("[SENSORS] external power OFF");
}

void preparePeripheralsForLowPower() {
  // OLED and Wi-Fi/Bluetooth are already disabled in FIELD mode.
  digitalWrite(PIN_BATTERY_CTRL, LOW);
  digitalWrite(PIN_RS485_RE_DE, LOW);
  // Q1/Q2 are disabled during sleep unless the installation explicitly uses
  // an always-on Modbus supply.
  if (cfg.modbusAlwaysOn && anyModbusDeviceEnabled()) {
    disableSensorInterfacesForSleep();
    sensorInterfacesReady = false;
    dsAvailable = false;
    inaAvailable = false;
    adsAvailable = false;
  } else {
    powerExternalSensorsOff();
  }
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

  if (apMode) {
    startAccessPoint();
    sensorReadRequested = true;
  } else {
    // FIELD mode performs its initial measurement before LoRaWAN starts.
    readAllSensors();
    // FIELD mode: OLED remains physically powered off from boot onward. Keep
    // the Modbus rail on only when the installation selected always-on power.
    if (!(cfg.modbusAlwaysOn && anyModbusDeviceEnabled())) {
      powerExternalSensorsOff();
    }
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

    if ((sensorReadRequested || millis() - lastSensorRead >= 2000) &&
        !sensorReadInProgress) {
      lastSensorRead = millis();
      if (sensorPowerEnabled) {
        sensorReadRequested = false;
        readAllSensors();
      }
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
      if (cfg.lowPowerEnabled ||
          (cfg.modbusAlwaysOn && anyModbusDeviceEnabled())) {
        preparePeripheralsForLowPower();
      } else {
        powerExternalSensorsOff();
      }
      LoRaWAN.cycle(appTxDutyCycle);
      fieldMeasurementPending = true;
      deviceState = DEVICE_STATE_SLEEP;
      break;

    case DEVICE_STATE_SLEEP:
      // Keep the Q1/Q2 gate low throughout sleep unless the bus is externally
      // powered and the user selected the always-on policy.
      setExternalSensorMosfet(cfg.modbusAlwaysOn && anyModbusDeviceEnabled());
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
