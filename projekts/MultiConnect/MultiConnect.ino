/*
 * =====================================================================
 *  MKP MultiConnect V1.3
 *  Universal LoRaWAN / NB-IoT-ready sensor controller
 *  Current firmware profile: LoRaWAN + DS18B20 + ADS1220 Wegsensor + INA226
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
 *    Wegsensor: AIN2 - AIN3
 *    DS18B20: GPIO47, external 4.7 kOhm pull-up to 3.3 V
 *    INA226: SDA=41, SCL=42, address 0x40 (second I2C controller)
 *    Internal battery measurement:
 *      VBAT ADC=GPIO1, divider enable=GPIO37, divider ratio=4.9
 *    Reserved: MAX3485 GPIO40/39/38, sensor MOSFET GPIO34
 *
 *  Important OneWire patch for GPIO47:
 *    OneWire 2.3.8 / util / OneWire_direct_gpio.h
 *    replace every "pin < 46" with "pin < 49".
 *
 *  Important LoRa / ADS1220 behavior:
 *    ADS1220 measurements are cached. LoRaWAN SEND uses only cached data.
 *    Do not read ADS1220 immediately before LoRaWAN.send().
 * =====================================================================
 */

#include <Arduino.h>

#ifdef LORAWAN_DEVEUI_AUTO
#undef LORAWAN_DEVEUI_AUTO
#endif
#define LORAWAN_DEVEUI_AUTO 0

#include "LoRaWan_APP.h"
#include "HT_SSD1306Wire.h"
#include <ArduinoJson.h>
#include <DallasTemperature.h>
#include <LittleFS.h>
#include <OneWire.h>
#include <Preferences.h>
#include <SPI.h>
#include <WebServer.h>
#include <WiFi.h>
#include <Wire.h>
#include <math.h>

// ============================= Firmware =============================
static constexpr const char* FW_VERSION = "1.3.0";
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

// Heltec WiFi LoRa 32 V4.x internal Li-ion battery monitor.
static constexpr uint8_t PIN_BATTERY_ADC = 1;    // ADC1_CH0 / VBAT_Read
static constexpr uint8_t PIN_BATTERY_CTRL = 37; // HIGH enables divider
static constexpr float BATTERY_DIVIDER_RATIO = 4.90f;
static constexpr float BATTERY_CALIBRATION = 1.000f;
static constexpr uint8_t BATTERY_SAMPLES = 24;

// Reserved for future firmware profiles.
static constexpr uint8_t PIN_RS485_RE_DE = 40;
static constexpr uint8_t PIN_RS485_DI = 39;
static constexpr uint8_t PIN_RS485_RO = 38;
static constexpr uint8_t PIN_SENSOR_MOSFET = 34;

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

  int32_t wegRaw0 = 0;
  int32_t wegRaw1 = 8388607;
  int32_t wegMm0_x1000 = 0;
  int32_t wegMm1_x1000 = 10000;
};

Config cfg;
Preferences preferences;

// ============================= Measurements =========================
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

  uint32_t measuredAtMs = 0;
};

Measurements cached;

// ============================== Objects =============================
SPIClass adsSPI(HSPI);
SPISettings adsSPISettings(500000, MSBFIRST, SPI_MODE1);

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

bool adsAvailable = false;
bool dsAvailable = false;
bool inaAvailable = false;
bool oledAvailable = false;

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
  cfg.wegRaw0 = preferences.getInt("wegRaw0", cfg.wegRaw0);
  cfg.wegRaw1 = preferences.getInt("wegRaw1", cfg.wegRaw1);
  cfg.wegMm0_x1000 = preferences.getInt("wegMm0", cfg.wegMm0_x1000);
  cfg.wegMm1_x1000 = preferences.getInt("wegMm1", cfg.wegMm1_x1000);

  preferences.end();

  cfg.intervalMinutes = constrain(cfg.intervalMinutes, 1UL, 1440UL);
  cfg.dataRate = constrain(cfg.dataRate, static_cast<uint8_t>(0), static_cast<uint8_t>(5));

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
  preferences.putInt("wegRaw0", cfg.wegRaw0);
  preferences.putInt("wegRaw1", cfg.wegRaw1);
  preferences.putInt("wegMm0", cfg.wegMm0_x1000);
  preferences.putInt("wegMm1", cfg.wegMm1_x1000);
  preferences.end();
}

// ============================== ADS1220 =============================
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

  if (readback != 0x10) return false;
  configureWegChannel();
  return true;
}

bool readWegsensor(int32_t& raw, float& voltageV, float& positionMm) {
  if (!adsAvailable) return false;

  configureWegChannel();

  for (uint8_t i = 0; i < ADS_DISCARD_SAMPLES; ++i) {
    int32_t dummy = 0;
    if (!adsSingleConversion(dummy)) return false;
  }

  int64_t sum = 0;
  for (uint8_t i = 0; i < ADS_AVERAGE_SAMPLES; ++i) {
    int32_t sample = 0;
    if (!adsSingleConversion(sample)) return false;
    sum += sample;
  }

  raw = static_cast<int32_t>(sum / ADS_AVERAGE_SAMPLES);
  voltageV = static_cast<float>(raw) * 3.300f / 8388608.0f;

  const int32_t rawDelta = cfg.wegRaw1 - cfg.wegRaw0;
  if (rawDelta == 0) return false;

  const float mm0 = static_cast<float>(cfg.wegMm0_x1000) / 1000.0f;
  const float mm1 = static_cast<float>(cfg.wegMm1_x1000) / 1000.0f;
  const float ratio = static_cast<float>(raw - cfg.wegRaw0) / static_cast<float>(rawDelta);
  positionMm = mm0 + ratio * (mm1 - mm0);

  const float lower = min(mm0, mm1);
  const float upper = max(mm0, mm1);
  positionMm = constrain(positionMm, lower, upper);
  return isfinite(positionMm);
}

// ============================== DS18B20 =============================
bool initializeDS18B20() {
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
static constexpr uint8_t INA226_REG_SHUNT = 0x01;
static constexpr uint8_t INA226_REG_BUS = 0x02;
static constexpr uint8_t INA226_REG_MANUFACTURER = 0xFE;
static constexpr uint8_t INA226_REG_DIE_ID = 0xFF;
static constexpr float INA226_SHUNT_OHMS = 0.010f;  // R010 = 10 mOhm

bool inaReadRegister(uint8_t reg, uint16_t& value) {
  sensorI2C.beginTransmission(INA226_ADDRESS);
  sensorI2C.write(reg);
  if (sensorI2C.endTransmission(false) != 0) return false;

  if (sensorI2C.requestFrom(INA226_ADDRESS, static_cast<uint8_t>(2)) != 2) return false;
  value = static_cast<uint16_t>(sensorI2C.read()) << 8;
  value |= static_cast<uint16_t>(sensorI2C.read());
  return true;
}

bool initializeINA226() {
  sensorI2C.begin(PIN_INA_SDA, PIN_INA_SCL, 100000);

  uint16_t manufacturer = 0;
  uint16_t dieId = 0;
  if (!inaReadRegister(INA226_REG_MANUFACTURER, manufacturer)) return false;
  if (!inaReadRegister(INA226_REG_DIE_ID, dieId)) return false;

  Serial.printf("[INA226] manufacturer=0x%04X die=0x%04X\n", manufacturer, dieId);
  return manufacturer == 0x5449;
}

bool readINA226(float& busVoltageV, float& shuntVoltageMv, float& currentMa, float& powerW) {
  if (!inaAvailable) return false;

  uint16_t rawBus = 0;
  uint16_t rawShunt = 0;
  if (!inaReadRegister(INA226_REG_BUS, rawBus)) return false;
  if (!inaReadRegister(INA226_REG_SHUNT, rawShunt)) return false;

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
  // Approximate resting-voltage curve for a single-cell Li-ion / LiPo.
  // Voltage is the primary measurement; percentage is only an estimate.
  struct Point {
    uint16_t mv;
    uint8_t percent;
  };

  static constexpr Point curve[] = {
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

  if (millivolts <= curve[0].mv) return curve[0].percent;
  const size_t count = sizeof(curve) / sizeof(curve[0]);
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

// =========================== Sensor cache ===========================
void readAllSensors() {
  Measurements next;

  next.temperatureValid = readTemperature(next.temperatureC);
  next.wegValid = readWegsensor(next.wegRaw, next.wegVoltageV, next.wegPositionMm);
  next.inaValid = readINA226(next.inaBusVoltageV, next.inaShuntVoltageMv,
                                next.inaCurrentMa, next.inaPowerW);
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
  doc["ip"] = apMode ? WiFi.softAPIP().toString() : String("-");
  doc["uptime_s"] = millis() / 1000UL;
  doc["heap_free"] = ESP.getFreeHeap();

  doc["temperature_valid"] = cached.temperatureValid;
  if (cached.temperatureValid) doc["temperature_c"] = cached.temperatureC;

  doc["weg_valid"] = cached.wegValid;
  doc["weg_raw"] = cached.wegRaw;
  if (cached.wegValid) {
    doc["weg_voltage_v"] = cached.wegVoltageV;
    doc["position_mm"] = cached.wegPositionMm;
  }

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

  JsonObject config = doc["cfg"].to<JsonObject>();
  config["devEui_msb"] = bytesToHex(cfg.devEui, sizeof(cfg.devEui));
  config["appEui_msb"] = bytesToHex(cfg.appEui, sizeof(cfg.appEui));
  config["appKey_msb"] = bytesToHex(cfg.appKey, sizeof(cfg.appKey));
  config["minutes"] = cfg.intervalMinutes;
  config["adr"] = cfg.adr;
  config["dr"] = cfg.dataRate;

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
  if (request["dr"].is<int>()) {
    const int value = request["dr"].as<int>();
    if (value < 0 || value > 5) {
      server.send(400, "text/plain", "DR range 0..5");
      return;
    }
    updated.dataRate = static_cast<uint8_t>(value);
  }

  if (request["weg_cal"].is<JsonObjectConst>()) {
    JsonObjectConst cal = request["weg_cal"].as<JsonObjectConst>();

    if (cal["raw0"].is<int32_t>()) updated.wegRaw0 = cal["raw0"].as<int32_t>();
    if (cal["raw1"].is<int32_t>()) updated.wegRaw1 = cal["raw1"].as<int32_t>();
    if (cal["mm0"].is<float>() || cal["mm0"].is<int>()) {
      updated.wegMm0_x1000 = static_cast<int32_t>(lroundf(cal["mm0"].as<float>() * 1000.0f));
    }
    if (cal["mm1"].is<float>() || cal["mm1"].is<int>()) {
      updated.wegMm1_x1000 = static_cast<int32_t>(lroundf(cal["mm1"].as<float>() * 1000.0f));
    }

    if (updated.wegRaw0 == updated.wegRaw1) {
      server.send(400, "text/plain", "calibration raw values must differ");
      return;
    }
  }

  cfg = updated;
  saveConfig();
  copyConfigToLoRaGlobals();

  // AP loop refreshes the sensor cache independently. Keep this response fast
  // so the browser does not lose the connection while saving settings.
  server.send(200, "text/plain", "saved");
}

void apiReboot() {
  server.send(200, "text/plain", "restarting");
  delay(200);
  ESP.restart();
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

  server.on("/api/state", HTTP_GET, apiState);
  server.on("/api/config", HTTP_POST, apiSaveConfig);
  server.on("/api/deveui", HTTP_GET, apiChipDevEui);
  server.on("/api/deveui/use-chip", HTTP_POST, apiUseChipDevEui);
  server.on("/api/reboot", HTTP_POST, apiReboot);

  server.onNotFound([]() {
    server.send(404, "text/plain", "Not found");
  });

  server.begin();
}

void startAccessPoint() {
  WiFi.mode(WIFI_AP);
  WiFi.setSleep(false);
  WiFi.setTxPower(WIFI_POWER_7dBm);

  const String ssid = apSsid();
  WiFi.softAP(ssid.c_str(), AP_PASSWORD);

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
// Payload V3, 22 bytes, big-endian:
// 0 version=3
// 1 status: bit0 temp, bit1 weg, bit2 INA226
// 2..3 temperature x100, int16
// 4..5 position x1000 mm, uint16
// 6..9 Wegsensor raw, int32
// 10..11 INA bus voltage mV, uint16
// 12..13 INA shunt voltage uV, int16
// 14..17 INA current mA, int32
// 18..21 INA power mW, int32

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

static void prepareTxFrame(uint8_t port) {
  (void)port;

  uint8_t index = 0;
  appData[index++] = 4;

  uint8_t status = 0;
  if (cached.temperatureValid) status |= 1U << 0;
  if (cached.wegValid) status |= 1U << 1;
  if (cached.inaValid) status |= 1U << 2;
  if (cached.batteryValid) status |= 1U << 3;
  appData[index++] = status;

  int16_t encodedTemperature = INT16_MAX;
  if (cached.temperatureValid) {
    encodedTemperature = static_cast<int16_t>(constrain(
      lroundf(cached.temperatureC * 100.0f),
      static_cast<long>(INT16_MIN),
      static_cast<long>(INT16_MAX - 1)
    ));
  }
  putInt16BE(index, encodedTemperature);

  uint16_t encodedPosition = UINT16_MAX;
  if (cached.wegValid) {
    encodedPosition = static_cast<uint16_t>(constrain(
      lroundf(cached.wegPositionMm * 1000.0f),
      0L,
      static_cast<long>(UINT16_MAX - 1)
    ));
  }
  putUInt16BE(index, encodedPosition);
  putInt32BE(index, cached.wegValid ? cached.wegRaw : 0);

  uint16_t encodedBusMv = UINT16_MAX;
  int16_t encodedShuntUv = INT16_MAX;
  if (cached.inaValid) {
    encodedBusMv = static_cast<uint16_t>(constrain(
      lroundf(cached.inaBusVoltageV * 1000.0f),
      0L,
      static_cast<long>(UINT16_MAX - 1)
    ));
    encodedShuntUv = static_cast<int16_t>(constrain(
      lroundf(cached.inaShuntVoltageMv * 1000.0f),
      static_cast<long>(INT16_MIN),
      static_cast<long>(INT16_MAX - 1)
    ));
  }
  putUInt16BE(index, encodedBusMv);
  putInt16BE(index, encodedShuntUv);

  int32_t encodedCurrentMa = INT32_MAX;
  int32_t encodedPowerMw = INT32_MAX;
  if (cached.inaValid) {
    encodedCurrentMa = static_cast<int32_t>(lroundf(cached.inaCurrentMa));
    encodedPowerMw = static_cast<int32_t>(lroundf(cached.inaPowerW * 1000.0f));
  }
  putInt32BE(index, encodedCurrentMa);
  putInt32BE(index, encodedPowerMw);

  const uint16_t encodedBatteryMv =
    cached.batteryValid ? cached.batteryMillivolts : UINT16_MAX;
  const uint8_t encodedBatteryPercent =
    cached.batteryValid ? cached.batteryPercent : UINT8_MAX;

  putUInt16BE(index, encodedBatteryMv);
  appData[index++] = encodedBatteryPercent;

  appDataSize = index;

  Serial.print("[LORA] cached payload: ");
  for (uint8_t i = 0; i < appDataSize; ++i) Serial.printf("%02X ", appData[i]);
  Serial.println();
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

  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  Serial.println();
  Serial.printf("%s FW %s (%s %s)\n", DEVICE_NAME, FW_VERSION, __DATE__, __TIME__);

  // Select the operating mode once at startup.
  apMode = selectApMode();

  // Keep reserved outputs in safe states.
  pinMode(PIN_RS485_RE_DE, OUTPUT);
  digitalWrite(PIN_RS485_RE_DE, LOW);
  pinMode(PIN_SENSOR_MOSFET, OUTPUT);
  digitalWrite(PIN_SENSOR_MOSFET, LOW);

  loadConfig();
  copyConfigToLoRaGlobals();

  oledPowerOn();
  oledSplash();
  delay(650);
  oledShowBootMode(apMode);
  delay(850);

  initializeBatteryMeasurement();
  dsAvailable = initializeDS18B20();
  inaAvailable = initializeINA226();
  adsAvailable = initializeADS1220();

  Serial.printf("[INIT] ADS1220=%s DS18B20=%s INA226=%s\n",
                adsAvailable ? "OK" : "ERROR",
                dsAvailable ? "OK" : "ERROR",
                inaAvailable ? "OK" : "ERROR");

  // Initial cached measurement is prepared before LoRaWAN initialization.
  readAllSensors();

  if (apMode) {
    startAccessPoint();
  } else {
    oledShowSensors();
    delay(1500);
    oledPowerOff();
    disableUnusedRadiosForFieldMode();
    Serial.println("[MODE] FIELD low-power mode");
  }
}

// ================================ Loop ==============================
void loop() {
  if (apMode) {
    server.handleClient();

    static uint32_t lastSensorRead = 0;
    static uint32_t lastPageChange = 0;
    static bool sensorPage = false;

    if (millis() - lastSensorRead >= 2000) {
      lastSensorRead = millis();
      readAllSensors();
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
      Serial.println("[LORA] SEND CACHED");
      prepareTxFrame(appPort);
      LoRaWAN.send();
      deviceState = DEVICE_STATE_CYCLE;
      break;

    case DEVICE_STATE_CYCLE:
      // Prepare data for the next uplink only after the current uplink.
      // This avoids ADS1220 activity immediately before LoRaWAN.send().
      readAllSensors();
      appTxDutyCycle = intervalMs();
      LoRaWAN.cycle(appTxDutyCycle);
      deviceState = DEVICE_STATE_SLEEP;
      break;

    case DEVICE_STATE_SLEEP:
      LoRaWAN.sleep(loraWanClass);
      break;

    default:
      deviceState = DEVICE_STATE_INIT;
      break;
  }
}
