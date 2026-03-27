@ -1,1633 +1,1757 @@
#include <Arduino.h>
#include <SPI.h>
#include <SD.h>
#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <LittleFS.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ctype.h>
#include <math.h>
#include "LoRaWan_APP.h"

// ============================================================
// MKP LoRaSense - Heltec WiFi LoRa 32 V3 fixed-PCB build
//
// CONFIG / AP mode
//   - fast live measurement
//   - web UI
//   - optional SD logging to CSV
//
// FIELD mode
//   - LoRaWAN only
//   - one fresh sensor snapshot is captured immediately before
//     every uplink, then the Heltec LoRaWAN stack returns to sleep
// ============================================================

// ---------------- PINS ----------------
static constexpr int PIN_SPI_MISO = 39;
static constexpr int PIN_SPI_SCK  = 2;
static constexpr int PIN_SPI_MOSI = 1;
static constexpr int PIN_SD_CS    = 7;
static constexpr int PIN_ADS_CS   = 38;
static constexpr int PIN_ADS_DRDY = 4;
static constexpr int PIN_MOSFET   = 5;
static constexpr int PIN_DS18B20  = 6;
static constexpr int PIN_AP_MODE  = 46;  // jumper to GND => CONFIG / AP mode

static constexpr const char* FW_VERSION = "1.7.0-apfield-sd";
static constexpr const char* BOARD_NAME = "Heltec WiFi LoRa 32 V3";

static constexpr uint8_t AIN2_MODE_POT  = 0;
static constexpr uint8_t AIN2_MODE_VOLT = 1;

static constexpr float TEMP_FIXED_HZ = 1.0f;
static constexpr float DMS_MIN_HZ = 0.1f;
static constexpr float DMS_MAX_HZ = 200.0f;
static constexpr float AIN2_MIN_HZ = 0.1f;
static constexpr float AIN2_MAX_HZ = 100.0f;
static constexpr float ADS_BUDGET_CONVERSIONS_HZ = 200.0f;
static constexpr float DMS_MAX_HZ = 500.0f;
static constexpr float AIN2_MIN_HZ = 1.0f;
static constexpr float AIN2_MAX_HZ = 500.0f;
static constexpr float ADS_BUDGET_CONVERSIONS_HZ = 1000.0f;

static constexpr size_t DEV_EUI_HEX_LEN = 16;
static constexpr size_t APP_EUI_HEX_LEN = 16;
static constexpr size_t APP_KEY_HEX_LEN = 32;
static constexpr size_t LOG_BASENAME_LEN = 24;

static WebServer server(80);
static Preferences prefs;
static OneWire oneWire(PIN_DS18B20);
static DallasTemperature dsSensor(&oneWire);

enum class RunMode : uint8_t { FIELD = 0, CONFIG = 1 };
static RunMode g_mode = RunMode::FIELD;
static bool g_loraEnabled = false;
static bool g_loraInitDone = false;
static uint32_t g_radioQuietUntilMs = 0;
static IPAddress g_apIp(0, 0, 0, 0);
static String g_apSsid;

// ---------------- ADS1220 ----------------
namespace ADS1220 {
  static constexpr uint8_t CMD_RESET = 0x06;
  static constexpr uint8_t CMD_START = 0x08;
  static constexpr uint8_t CMD_RDATA = 0x10;
  static constexpr uint8_t CMD_WREG  = 0x40;
}

static constexpr int32_t ADS_FS = 1 << 23;
static constexpr int32_t ADS_FAIL = INT32_MIN;
static constexpr float ADS_VREF_VOLTS = 3.3f;
static constexpr uint8_t REG0_DMS  = 0x0E; // AIN0-AIN1, gain=128, PGA enabled
static constexpr uint8_t REG0_AIN2 = 0xA1; // AIN2-AVSS, gain=1, PGA bypass
static constexpr float GAIN_DMS = 128.0f;
static constexpr float GAIN_AIN2 = 1.0f;
static constexpr float EXPECTED_MV = 3.8f;
static constexpr float TOLERANCE_MV = 1.0f;
static constexpr int RETRY_MAX = 2;
static constexpr uint32_t DRDY_TIMEOUT_US = 10000;
static constexpr uint8_t ADS_REG1 = 0xA0; // 600 SPS, single-shot
static constexpr uint8_t ADS_REG2 = 0x40; // external reference
static constexpr uint8_t ADS_REG3 = 0x00;

struct Cfg {
  char devEui[DEV_EUI_HEX_LEN + 1];
  char appEui[APP_EUI_HEX_LEN + 1];
  char appKey[APP_KEY_HEX_LEN + 1];
  uint16_t intervalMin;
  bool adr;
  uint8_t dr;

  bool dmsEnabled;
  float dmsHz;

  bool ain2Enabled;
  float ain2Hz;
  uint8_t ain2Mode;
  float ain2LengthMm;
  float ain2AdcFullscaleV;
  float ain2InputFullscaleV;

  bool tempEnabled;

  bool sdLogEnabled;
  char logBaseName[LOG_BASENAME_LEN + 1];
  uint32_t logRotateKB;
};

static Cfg cfg = {
  "631E5618DF8EB3D4",
  "0000000000000000",
  "636AC09B24824A4730328058CE636ADF",
  15,
  true,
  5,

  true,
  200.0f,

  true,
  1.0f,
  AIN2_MODE_POT,
  10.0f,
  3.123f,
  10.0f,

  true,

  false,
  "lorasense",
  1024
};
struct PerfEstimate {
  float dmsTargetHz;
  float ain2TargetHz;
  float tempTargetHz;
  float adsRequestedConvHz;
  float adsLoadPct;
  float scale;
  float dmsEstimatedHz;
  float ain2EstimatedHz;
  float totalEstimatedHz;
};
// ---------------- Runtime sensor values ----------------
static float dms_mV = NAN;
static float dms_mV_per_V = NAN;
static int32_t lastDmsRaw = 0;
static bool dms_valid = false;

static float ain2_mV = NAN;
static int32_t lastAin2Raw = 0;
static bool ain2_valid = false;

static float ds_temp_c = NAN;
static bool temp_valid = false;
static bool ds_pending = false;
static uint32_t ds_request_ms = 0;
static uint32_t last_ds_request_loop_ms = 0;

static bool selfTestOk = false;
static float selfTestDelta_mV = NAN;

static uint32_t last_dms_read_ms = 0;
static uint32_t last_ain2_read_ms = 0;

static uint32_t dmsFramesPerSecond = 0;
static uint32_t ain2FramesPerSecond = 0;
static uint32_t tempFramesPerSecond = 0;
static uint32_t totalFramesPerSecond = 0;
static uint32_t framesPerSecond = 0; // backward-compatible alias = DMS fps
static uint32_t g_lastSampleSeq = 0;

// ---------------- SD logging ----------------
static bool g_sdMounted = false;
static bool g_sdMountTried = false;
static String g_sdStatus = "disabled";
static File g_logFile;
static String g_logFileName;
static uint32_t g_logFileSeq = 0;
static uint32_t g_logLineCount = 0;
static uint32_t g_logLastFlushMs = 0;
static uint32_t g_lastLoggedSampleSeq = 0;
static volatile bool g_sdBusy = false;
static uint32_t g_lastLogWriteMs = 0;
static uint32_t g_logPeriodMs = 1000;   // запис на SD раз на 1 секунду
static uint32_t g_logFlushPeriodMs = 5000; // flush раз на 5 секунд

struct LogSample {
  uint32_t t_ms;
  uint8_t dms_on;
  uint8_t dms_valid;
  int32_t dms_raw;
  float dms_mV;
  float dms_mV_per_V;
  uint8_t ain2_on;
  uint8_t ain2_valid;
  uint8_t ain2_mode;
  int32_t ain2_raw;
  float ain2_mV;
  float ain2_value;
  uint8_t temp_on;
  uint8_t temp_valid;
  float temp_C;
  uint8_t selftest_ok;
};

static constexpr uint16_t LOG_BUF_SIZE = 1024;
static constexpr uint16_t LOG_BATCH_SIZE = 64;
static constexpr uint32_t LOG_FORCE_FLUSH_MS = 250;
static LogSample g_logBuf[LOG_BUF_SIZE];
static uint16_t g_logBufHead = 0;
static uint16_t g_logBufTail = 0;
static uint16_t g_logBufCount = 0;
static uint16_t g_logBufMax = 0;
static uint32_t g_logDropped = 0;
static uint32_t g_logLastDrainMs = 0;
// ---------------- LoRaWAN globals required by Heltec ----------------
uint8_t devEui[8]   = { 0 };
uint8_t appEui[8]   = { 0 };
uint8_t appKey[16]  = { 0 };
uint8_t nwkSKey[16] = { 0 };
uint8_t appSKey[16] = { 0 };
uint32_t devAddr    = 0;

// Compatibility aliases for older Heltec cores.
uint8_t DevEui[8]   = { 0 };
uint8_t AppEui[8]   = { 0 };
uint8_t AppKey[16]  = { 0 };
uint8_t NwkSKey[16] = { 0 };
uint8_t AppSKey[16] = { 0 };
uint32_t DevAddr    = 0;

extern uint8_t appData[];
extern uint8_t appDataSize;

uint32_t appTxDutyCycle = 15UL * 60UL * 1000UL;
bool overTheAirActivation = true;
bool loraWanAdr = true;
bool isTxConfirmed = true;
uint8_t appPort = 2;
uint8_t confirmedNbTrials = 3;
uint16_t userChannelsMask[6] = { 0x00FF, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t loraWanClass = CLASS_A;

// ============================================================
// Helpers
// ============================================================
static inline void adsCS(bool en) { digitalWrite(PIN_ADS_CS, en ? LOW : HIGH); }
static inline void adsSck(bool high) { digitalWrite(PIN_SPI_SCK, high ? HIGH : LOW); }
static inline void adsMosi(bool high) { digitalWrite(PIN_SPI_MOSI, high ? HIGH : LOW); }

static void restoreAdsBusAfterSd() {
  digitalWrite(PIN_SD_CS, HIGH);
  delayMicroseconds(5);

  pinMode(PIN_SPI_SCK, OUTPUT);
  pinMode(PIN_SPI_MISO, INPUT);
  pinMode(PIN_SPI_MOSI, OUTPUT);

  digitalWrite(PIN_SPI_SCK, LOW);
  digitalWrite(PIN_SPI_MOSI, LOW);

  adsCS(false);
  delayMicroseconds(5);

  if (cfg.dmsEnabled) {
    restoreDmsRunMode();
  }
}

static inline void adsBusPrepare() {
  // SD must be fully deselected before bit-banged ADS traffic starts
  digitalWrite(PIN_SD_CS, HIGH);
  delayMicroseconds(2);
}

static inline void sdBusPrepare() {
  // ADS must be fully deselected before SD uses hardware SPI
  adsCS(false);
  adsSck(false);
  adsMosi(false);
  delayMicroseconds(2);
}

static uint8_t adsXfer(uint8_t b) {
  uint8_t rx = 0;
  for (int bit = 7; bit >= 0; --bit) {
    adsMosi((b >> bit) & 0x01);
    delayMicroseconds(1);
    adsSck(true);
    delayMicroseconds(1);
    rx = (uint8_t)((rx << 1) | (digitalRead(PIN_SPI_MISO) ? 1 : 0));
    adsSck(false);
    delayMicroseconds(1);
  }
  return rx;
}

static inline bool isApModeRequested() {
  pinMode(PIN_AP_MODE, INPUT_PULLUP);
  delay(2);
  return digitalRead(PIN_AP_MODE) == LOW;
}

static float rawTo_mV_gain(int32_t raw, float gain) {
  float v = ((float)raw * ADS_VREF_VOLTS) / (gain * (float)ADS_FS);
  return v * 1000.0f;
}

static float dmsTo_mV_per_V(float dms_mV_local) {
  return dms_mV_local / ADS_VREF_VOLTS;
}

static float clampf(float v, float lo, float hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

static const char* ain2ModeName() {
  return (cfg.ain2Mode == AIN2_MODE_VOLT) ? "volt" : "pot";
}

static const char* ain2ModeNameFor(uint8_t mode) {
  return (mode == AIN2_MODE_VOLT) ? "volt" : "pot";
}

static const char* ain2DerivedUnitFor(uint8_t mode) {
  return (mode == AIN2_MODE_VOLT) ? "V" : "mm";
}

static uint8_t hexNibble(char c) {
  if (c >= '0' && c <= '9') return (uint8_t)(c - '0');
  if (c >= 'A' && c <= 'F') return (uint8_t)(10 + c - 'A');
  if (c >= 'a' && c <= 'f') return (uint8_t)(10 + c - 'a');
  return 0;
}

static bool parseHexToBytes(const char* hex, uint8_t* out, size_t outLen) {
  size_t hexLen = strlen(hex);
  if (hexLen != outLen * 2) return false;
  for (size_t i = 0; i < outLen; ++i) {
    out[i] = (uint8_t)((hexNibble(hex[i * 2]) << 4) | hexNibble(hex[i * 2 + 1]));
  }
  return true;
}

static void bytesToHex(const uint8_t* in, size_t len, char* out) {
  static const char* lut = "0123456789ABCDEF";
  for (size_t i = 0; i < len; ++i) {
    out[i * 2] = lut[(in[i] >> 4) & 0x0F];
    out[i * 2 + 1] = lut[in[i] & 0x0F];
  }
  out[len * 2] = '\0';
}

static void copyUpperNoSpace(char* dst, size_t dstLen, const char* src) {
  if (dstLen == 0) return;
  size_t j = 0;
  for (size_t i = 0; src && src[i] != '\0'; ++i) {
    unsigned char c = (unsigned char)src[i];
    if (isspace(c)) continue;
    if (j + 1 < dstLen) dst[j++] = (char)toupper(c);
  }
  dst[j] = '\0';
}

static void sanitizeFileStem(char* dst, size_t dstLen, const char* src) {
  if (dstLen == 0) return;
  size_t j = 0;
  for (size_t i = 0; src && src[i] != '\0'; ++i) {
    char c = src[i];
    bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
    if (!ok) continue;
    if (j + 1 < dstLen) dst[j++] = c;
  }
  if (j == 0) {
    strlcpy(dst, "lorasense", dstLen);
    return;
  }
  dst[j] = '\0';
}

static bool isExactHex(const char* s, size_t len) {
  if (!s || strlen(s) != len) return false;
  for (size_t i = 0; i < len; ++i) {
    unsigned char c = (unsigned char)s[i];
    if (!isxdigit(c)) return false;
  }
  return true;
}

static float hzToPeriodMsFloat(float hz) {
  if (hz <= 0.0001f) return 1000000.0f;
  return 1000.0f / hz;
}

static uint32_t hzToPeriodMsClamped(float hz, uint32_t minMs, uint32_t maxMs) {
  float ms = hzToPeriodMsFloat(hz);
  if (ms < (float)minMs) ms = (float)minMs;
  if (ms > (float)maxMs) ms = (float)maxMs;
  return (uint32_t)(ms + 0.5f);
}

static float periodMsToHz(uint32_t ms) {
  if (ms == 0) return 0.0f;
  return 1000.0f / (float)ms;
}

static void syncLoraCompatAliases() {
  memcpy(DevEui, devEui, sizeof(devEui));
  memcpy(AppEui, appEui, sizeof(appEui));
  memcpy(AppKey, appKey, sizeof(appKey));
  memcpy(NwkSKey, nwkSKey, sizeof(nwkSKey));
  memcpy(AppSKey, appSKey, sizeof(appSKey));
  DevAddr = devAddr;
}


static PerfEstimate makePerfEstimate() {
  PerfEstimate p{};
  p.dmsTargetHz = cfg.dmsEnabled ? cfg.dmsHz : 0.0f;
  p.ain2TargetHz = cfg.ain2Enabled ? cfg.ain2Hz : 0.0f;
  p.tempTargetHz = cfg.tempEnabled ? TEMP_FIXED_HZ : 0.0f;

  float ain2Conv = 0.0f;
  if (cfg.ain2Enabled) ain2Conv = cfg.dmsEnabled ? 3.0f : 2.0f; // discard + read + optional DMS restore

  p.adsRequestedConvHz = p.dmsTargetHz + p.ain2TargetHz * ain2Conv;
  p.scale = 1.0f;
  if (p.adsRequestedConvHz > ADS_BUDGET_CONVERSIONS_HZ && p.adsRequestedConvHz > 0.0001f) {
    p.scale = ADS_BUDGET_CONVERSIONS_HZ / p.adsRequestedConvHz;
  }
  p.adsLoadPct = clampf((p.adsRequestedConvHz / ADS_BUDGET_CONVERSIONS_HZ) * 100.0f, 0.0f, 999.0f);
  p.dmsEstimatedHz = p.dmsTargetHz * p.scale;
  p.ain2EstimatedHz = p.ain2TargetHz * p.scale;
  p.totalEstimatedHz = p.dmsEstimatedHz + p.ain2EstimatedHz + p.tempTargetHz;
  return p;
}

static void getChipMac48(uint8_t out[6]) {
  uint64_t mac = ESP.getEfuseMac();
  for (int i = 0; i < 6; ++i) out[5 - i] = (uint8_t)((mac >> (i * 8)) & 0xFF);
}

static void makeChipDevEui(uint8_t out[8]) {
  uint8_t mac[6];
  getChipMac48(mac);
  out[0] = mac[0] ^ 0x02;
  out[1] = mac[1];
  out[2] = mac[2];
  out[3] = 0xFF;
  out[4] = 0xFE;
  out[5] = mac[3];
  out[6] = mac[4];
  out[7] = mac[5];
}

static void chipMac48Hex(char out[13]) {
  uint8_t mac[6];
  getChipMac48(mac);
  bytesToHex(mac, 6, out);
}

static void chipDevEuiHex(char out[17]) {
  uint8_t eui[8];
  makeChipDevEui(eui);
  bytesToHex(eui, 8, out);
}

static void normalizeCfg(Cfg& c) {
  copyUpperNoSpace(c.devEui, sizeof(c.devEui), c.devEui);
  copyUpperNoSpace(c.appEui, sizeof(c.appEui), c.appEui);
  copyUpperNoSpace(c.appKey, sizeof(c.appKey), c.appKey);
  sanitizeFileStem(c.logBaseName, sizeof(c.logBaseName), c.logBaseName);

  if (c.intervalMin < 1) c.intervalMin = 1;
  if (c.intervalMin > 1440) c.intervalMin = 1440;
  if (c.dr > 5) c.dr = 5;

  c.dmsHz = clampf(c.dmsHz, DMS_MIN_HZ, DMS_MAX_HZ);
  c.ain2Hz = clampf(c.ain2Hz, AIN2_MIN_HZ, AIN2_MAX_HZ);

  if (c.ain2Mode != AIN2_MODE_POT && c.ain2Mode != AIN2_MODE_VOLT) c.ain2Mode = AIN2_MODE_POT;
  if (!(c.ain2LengthMm > 0.0f)) c.ain2LengthMm = 10.0f;
  if (!(c.ain2AdcFullscaleV > 0.05f)) c.ain2AdcFullscaleV = 3.123f;
  if (!(c.ain2InputFullscaleV > 0.05f)) c.ain2InputFullscaleV = 10.0f;
  if (c.logRotateKB < 64) c.logRotateKB = 64;
  if (c.logRotateKB > 1024UL * 1024UL) c.logRotateKB = 1024UL * 1024UL;
}

static bool validateCfg(const Cfg& c, String* errOut = nullptr) {
  if (!isExactHex(c.devEui, DEV_EUI_HEX_LEN)) {
    if (errOut) *errOut = "invalid DevEUI (need 16 hex chars)";
    return false;
  }
  if (!isExactHex(c.appEui, APP_EUI_HEX_LEN)) {
    if (errOut) *errOut = "invalid AppEUI (need 16 hex chars)";
    return false;
  }
  if (!isExactHex(c.appKey, APP_KEY_HEX_LEN)) {
    if (errOut) *errOut = "invalid AppKey (need 32 hex chars)";
    return false;
  }
  return true;
}

static void printHexLine(const char* label, const uint8_t* data, size_t len) {
  char buf[65];
  if (len * 2 >= sizeof(buf)) return;
  bytesToHex(data, len, buf);
  Serial.print(label);
  Serial.println(buf);
}

static void dumpLoraBuffers(const char* tag) {
  Serial.print("[LORA] ");
  Serial.println(tag);
  printHexLine("  devEui=", devEui, sizeof(devEui));
  printHexLine("  appEui=", appEui, sizeof(appEui));
  printHexLine("  appKey=", appKey, sizeof(appKey));
}

static void loadCfg() {
  Cfg defaults = cfg;

  prefs.begin("lorasense", true);
  String dE = prefs.getString("devEui", defaults.devEui);
  String aE = prefs.getString("appEui", defaults.appEui);
  String aK = prefs.getString("appKey", defaults.appKey);
  cfg.intervalMin = prefs.getUShort("minutes", defaults.intervalMin);
  cfg.adr = prefs.getBool("adr", defaults.adr);
  cfg.dr = prefs.getUChar("dr", defaults.dr);

  uint32_t oldDmsPer = prefs.getUInt("dmsper", hzToPeriodMsClamped(defaults.dmsHz, 5, 600000UL));
  uint32_t oldAin2Per = prefs.getUInt("aper", hzToPeriodMsClamped(defaults.ain2Hz, 5, 600000UL));

  cfg.dmsEnabled = prefs.getBool("dmson", defaults.dmsEnabled);
  cfg.dmsHz = prefs.getFloat("dmsHz", periodMsToHz(oldDmsPer));

  cfg.ain2Enabled = prefs.getBool("a2on", defaults.ain2Enabled);
  cfg.ain2Hz = prefs.getFloat("a2Hz", periodMsToHz(oldAin2Per));
  cfg.ain2Mode = prefs.getUChar("a2mode", defaults.ain2Mode);
  cfg.ain2LengthMm = prefs.getFloat("a2len", defaults.ain2LengthMm);
  cfg.ain2AdcFullscaleV = prefs.getFloat("a2adcfs", defaults.ain2AdcFullscaleV);
  cfg.ain2InputFullscaleV = prefs.getFloat("a2infs", defaults.ain2InputFullscaleV);

  cfg.tempEnabled = prefs.getBool("tmpon", defaults.tempEnabled);

  cfg.sdLogEnabled = prefs.getBool("sdon", defaults.sdLogEnabled);
  String base = prefs.getString("sdbase", defaults.logBaseName);
  cfg.logRotateKB = prefs.getUInt("sdkb", defaults.logRotateKB);
  prefs.end();

  strlcpy(cfg.devEui, dE.c_str(), sizeof(cfg.devEui));
  strlcpy(cfg.appEui, aE.c_str(), sizeof(cfg.appEui));
  strlcpy(cfg.appKey, aK.c_str(), sizeof(cfg.appKey));
  strlcpy(cfg.logBaseName, base.c_str(), sizeof(cfg.logBaseName));

  normalizeCfg(cfg);
  String err;
  if (!validateCfg(cfg, &err)) {
    Serial.print("[CFG] invalid stored config, using defaults: ");
    Serial.println(err);
    cfg = defaults;
    normalizeCfg(cfg);
  }
}

static void saveCfg() {
  normalizeCfg(cfg);
  prefs.begin("lorasense", false);
  prefs.putString("devEui", cfg.devEui);
  prefs.putString("appEui", cfg.appEui);
  prefs.putString("appKey", cfg.appKey);
  prefs.putUShort("minutes", cfg.intervalMin);
  prefs.putBool("adr", cfg.adr);
  prefs.putUChar("dr", cfg.dr);

  prefs.putBool("dmson", cfg.dmsEnabled);
  prefs.putFloat("dmsHz", cfg.dmsHz);
  prefs.putUInt("dmsper", hzToPeriodMsClamped(cfg.dmsHz, 5, 600000UL));

  prefs.putBool("a2on", cfg.ain2Enabled);
  prefs.putFloat("a2Hz", cfg.ain2Hz);
  prefs.putUInt("aper", hzToPeriodMsClamped(cfg.ain2Hz, 5, 600000UL));
  prefs.putUChar("a2mode", cfg.ain2Mode);
  prefs.putFloat("a2len", cfg.ain2LengthMm);
  prefs.putFloat("a2adcfs", cfg.ain2AdcFullscaleV);
  prefs.putFloat("a2infs", cfg.ain2InputFullscaleV);

  prefs.putBool("tmpon", cfg.tempEnabled);

  prefs.putBool("sdon", cfg.sdLogEnabled);
  prefs.putString("sdbase", cfg.logBaseName);
  prefs.putUInt("sdkb", cfg.logRotateKB);
  prefs.end();
}

static void applyLoraConfig() {
  memset(devEui, 0, sizeof(devEui));
  memset(appEui, 0, sizeof(appEui));
  memset(appKey, 0, sizeof(appKey));
  memset(nwkSKey, 0, sizeof(nwkSKey));
  memset(appSKey, 0, sizeof(appSKey));
  devAddr = 0;

  parseHexToBytes(cfg.devEui, devEui, sizeof(devEui));
  parseHexToBytes(cfg.appEui, appEui, sizeof(appEui));
  parseHexToBytes(cfg.appKey, appKey, sizeof(appKey));

  syncLoraCompatAliases();
  appTxDutyCycle = (uint32_t)cfg.intervalMin * 60UL * 1000UL;
  loraWanAdr = cfg.adr;
}

static float ain2DerivedValue() {
  if (!ain2_valid || isnan(ain2_mV)) return NAN;
  if (cfg.ain2Mode == AIN2_MODE_VOLT) {
    if (cfg.ain2AdcFullscaleV <= 0.001f) return NAN;
    float v = (ain2_mV / 1000.0f) * (cfg.ain2InputFullscaleV / cfg.ain2AdcFullscaleV);
    return clampf(v, 0.0f, cfg.ain2InputFullscaleV);
  }
  if (cfg.ain2AdcFullscaleV <= 0.001f) return NAN;
  float ratio = (ain2_mV / 1000.0f) / cfg.ain2AdcFullscaleV;
  return clampf(ratio, 0.0f, 1.0f) * cfg.ain2LengthMm;
}

static const char* ain2DerivedUnit() {
  return (cfg.ain2Mode == AIN2_MODE_VOLT) ? "V" : "mm";
}

static void invalidateDms() {
  dms_valid = false;
  dms_mV = NAN;
  dms_mV_per_V = NAN;
  lastDmsRaw = 0;
}

static void invalidateAin2() {
  ain2_valid = false;
  ain2_mV = NAN;
  lastAin2Raw = 0;
}

static void invalidateTemp() {
  temp_valid = false;
  ds_temp_c = NAN;
  ds_pending = false;
}

static void prepareTxFrame(uint8_t port);
static void syncSdLogger(bool forceReopen);

// ============================================================
// ADS low-level
// ============================================================
static void adsCommand(uint8_t cmd) {
  adsBusPrepare();
  adsCS(true);
  adsXfer(cmd);
  adsCS(false);
}

static void adsWriteRegs4(uint8_t r0, uint8_t r1, uint8_t r2, uint8_t r3) {
  adsBusPrepare();
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x03);
  adsXfer(r0);
  adsXfer(r1);
  adsXfer(r2);
  adsXfer(r3);
  adsCS(false);
}

static void adsWriteReg0(uint8_t r0) {
  adsBusPrepare();
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x00);
  adsXfer(r0);
  adsCS(false);
}

static int32_t adsReadRaw_RDATA() {
  adsBusPrepare();
  adsCS(true);
  adsXfer(ADS1220::CMD_RDATA);
  uint8_t b2 = adsXfer(0xFF);
  uint8_t b1 = adsXfer(0xFF);
  uint8_t b0 = adsXfer(0xFF);
  adsCS(false);

  int32_t raw = ((int32_t)b2 << 16) | ((int32_t)b1 << 8) | b0;
  if (raw & 0x800000) raw |= 0xFF000000;
  return raw;
}

static bool adsWaitDrdyLow(uint32_t timeout_us) {
  uint32_t start = micros();
  while (digitalRead(PIN_ADS_DRDY) == HIGH) {
    if ((uint32_t)(micros() - start) >= timeout_us) return false;
    delayMicroseconds(5);
  }
  return true;
}

static int32_t readSingleShot_DRDY() {
  for (int attempt = 0; attempt <= RETRY_MAX; ++attempt) {
    adsCommand(ADS1220::CMD_START);
    if (!adsWaitDrdyLow(DRDY_TIMEOUT_US)) {
      delayMicroseconds(100);
      continue;
    }
    return adsReadRaw_RDATA();
  }
  return ADS_FAIL;
}

static bool readChannelRaw(uint8_t reg0, float gain, float& out_mV, int32_t* rawOut) {
  adsWriteReg0(reg0);
  (void)readSingleShot_DRDY(); // discard after mux switch
  int32_t raw = readSingleShot_DRDY();
  if (raw == ADS_FAIL) {
    out_mV = NAN;
    if (rawOut) *rawOut = 0;
    return false;
  }
  if (rawOut) *rawOut = raw;
  out_mV = rawTo_mV_gain(raw, gain);
  return true;
}

static bool readDmsFast(float& out_mV, int32_t* rawOut = nullptr) {
  int32_t raw = readSingleShot_DRDY();
  if (raw == ADS_FAIL) {
    out_mV = NAN;
    if (rawOut) *rawOut = 0;
    return false;
  }
  if (rawOut) *rawOut = raw;
  out_mV = rawTo_mV_gain(raw, GAIN_DMS);
  return true;
}

static void adsInit() {
  pinMode(PIN_SD_CS, OUTPUT);
  digitalWrite(PIN_SD_CS, HIGH);

  pinMode(PIN_SPI_SCK, OUTPUT);
  pinMode(PIN_SPI_MISO, INPUT);
  pinMode(PIN_SPI_MOSI, OUTPUT);
  adsSck(false);
  adsMosi(false);

  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);
  pinMode(PIN_ADS_DRDY, INPUT);

  pinMode(PIN_MOSFET, OUTPUT);
  digitalWrite(PIN_MOSFET, LOW);

  adsCommand(ADS1220::CMD_RESET);
  delay(5);
  adsWriteRegs4(REG0_DMS, ADS_REG1, ADS_REG2, ADS_REG3);
}

static void restoreDmsRunMode() {
  adsWriteReg0(REG0_DMS);
  float dummy = NAN;
  int32_t raw = 0;
  (void)readDmsFast(dummy, &raw);
}

static bool runSelfTest() {
  if (!cfg.dmsEnabled) {
    selfTestOk = false;
    selfTestDelta_mV = NAN;
    return false;
  }

  Serial.println("# ===== SELFTEST START =====");

  adsWriteReg0(REG0_DMS);
  digitalWrite(PIN_MOSFET, LOW);

  float base5[5];
  float on5[5];

  auto avg5 = [](const float a[5]) -> float {
    double s = 0.0;
    for (int i = 0; i < 5; ++i) s += a[i];
    return (float)(s / 5.0);
  };

  for (int i = 0; i < 5; ) {
    float mv = NAN;
    int32_t raw = 0;
    if (!readChannelRaw(REG0_DMS, GAIN_DMS, mv, &raw)) continue;
    base5[i++] = mv;
  }
  float bAvg = avg5(base5);

  digitalWrite(PIN_MOSFET, HIGH);
  delay(50);

  for (int i = 0; i < 5; ) {
    float mv = NAN;
    int32_t raw = 0;
    if (!readChannelRaw(REG0_DMS, GAIN_DMS, mv, &raw)) continue;
    on5[i++] = mv;
  }

  digitalWrite(PIN_MOSFET, LOW);

  float mAvg = avg5(on5);
  selfTestDelta_mV = mAvg - bAvg;
  selfTestOk = (fabs(fabs(selfTestDelta_mV) - EXPECTED_MV) <= TOLERANCE_MV);

  Serial.print("# SELFTEST delta=");
  Serial.print(selfTestDelta_mV, 6);
  Serial.print(" mV  result=");
  Serial.println(selfTestOk ? "OK" : "FAIL");

  restoreDmsRunMode();
  return selfTestOk;
}

// ============================================================
// Sensor runtime
// ============================================================
static void dsInit() {
  dsSensor.begin();
  dsSensor.setWaitForConversion(false);

  uint8_t devCount = dsSensor.getDeviceCount();
  for (uint8_t i = 0; i < devCount; ++i) {
    DeviceAddress addr;
    if (dsSensor.getAddress(addr, i)) dsSensor.setResolution(addr, 9);
  }

  ds_pending = false;
  ds_request_ms = 0;
  last_ds_request_loop_ms = 0;
}

static bool readTempBlocking(float& outTempC) {
  dsSensor.requestTemperatures();
  delay(120);
  float tmp = dsSensor.getTempCByIndex(0);
  if (tmp == DEVICE_DISCONNECTED_C) return false;
  outTempC = tmp;
  return true;
}

static void sensorLoopAp() {
  static uint32_t dmsCount = 0;
  static uint32_t ain2Count = 0;
  static uint32_t tempCount = 0;
  static uint32_t totalCount = 0;
  static uint32_t lastFrameReportMs = 0;

  uint32_t now = millis();
  bool radioQuiet = g_loraEnabled && ((int32_t)(now - g_radioQuietUntilMs) < 0);
  bool sampleChanged = false;

  if (!cfg.dmsEnabled) invalidateDms();
  if (!cfg.ain2Enabled) invalidateAin2();
  if (!cfg.tempEnabled) invalidateTemp();

  // ---------------- DMS + AIN2 ----------------
  if (!radioQuiet) {
    if (cfg.dmsEnabled) {
      uint32_t dmsPeriodMs = hzToPeriodMsClamped(cfg.dmsHz, 5, 600000UL);
      if ((uint32_t)(now - last_dms_read_ms) >= dmsPeriodMs) {
        if (readDmsFast(dms_mV, &lastDmsRaw)) {
          dms_mV_per_V = dmsTo_mV_per_V(dms_mV);
          dms_valid = true;
          dmsCount++;
          totalCount++;
          sampleChanged = true;
        } else {
          invalidateDms();
        }
        last_dms_read_ms = now;
      }
    }

    if (cfg.ain2Enabled) {
      uint32_t ain2PeriodMs = hzToPeriodMsClamped(cfg.ain2Hz, 5, 600000UL);
      if ((uint32_t)(now - last_ain2_read_ms) >= ain2PeriodMs) {
        float tmpAin2 = NAN;
        if (readChannelRaw(REG0_AIN2, GAIN_AIN2, tmpAin2, &lastAin2Raw)) {
          ain2_mV = tmpAin2;
          ain2_valid = true;
          ain2Count++;
          totalCount++;
          sampleChanged = true;
        } else {
          invalidateAin2();
        }

        if (cfg.dmsEnabled) restoreDmsRunMode();
        last_ain2_read_ms = now;
      }
    }
  }

  // ---------------- Temperature ----------------
  if (cfg.tempEnabled) {
    if (!ds_pending && ((uint32_t)(now - last_ds_request_loop_ms) >= 1000UL)) {
      dsSensor.requestTemperatures();
      ds_pending = true;
      ds_request_ms = now;
      last_ds_request_loop_ms = now;
    }

    if (ds_pending && ((uint32_t)(now - ds_request_ms) >= 120UL)) {
      float tmp = dsSensor.getTempCByIndex(0);
      if (tmp != DEVICE_DISCONNECTED_C) {
        ds_temp_c = tmp;
        temp_valid = true;
        tempCount++;
        totalCount++;
        sampleChanged = true;
      } else {
        invalidateTemp();
      }
      ds_pending = false;
    }
  }

  // ---------------- FPS/stat ----------------
  if ((uint32_t)(now - lastFrameReportMs) >= 1000UL) {
    dmsFramesPerSecond = dmsCount;
    ain2FramesPerSecond = ain2Count;
    tempFramesPerSecond = tempCount;
    totalFramesPerSecond = totalCount;
    framesPerSecond = dmsFramesPerSecond;

    dmsCount = 0;
    ain2Count = 0;
    tempCount = 0;
    totalCount = 0;
    lastFrameReportMs = now;
  }

  if (sampleChanged) {
    g_lastSampleSeq++;
    captureLogSample(now);
  }
}

static void captureFieldSnapshot() {
  if (cfg.dmsEnabled) {
    if (readDmsFast(dms_mV, &lastDmsRaw)) {
      dms_mV_per_V = dmsTo_mV_per_V(dms_mV);
      dms_valid = true;
    } else {
      invalidateDms();
    }
  } else {
    invalidateDms();
  }

  if (cfg.ain2Enabled) {
    float tmpAin2 = NAN;
    if (readChannelRaw(REG0_AIN2, GAIN_AIN2, tmpAin2, &lastAin2Raw)) {
      ain2_mV = tmpAin2;
      ain2_valid = true;
    } else {
      invalidateAin2();
    }
    if (cfg.dmsEnabled) restoreDmsRunMode();
  } else {
    invalidateAin2();
  }

  if (cfg.tempEnabled) {
    float tmpC = NAN;
    if (readTempBlocking(tmpC)) {
      ds_temp_c = tmpC;
      temp_valid = true;
    } else {
      invalidateTemp();
    }
  } else {
    invalidateTemp();
  }

  dmsFramesPerSecond = 0;
  ain2FramesPerSecond = 0;
  tempFramesPerSecond = 0;
  totalFramesPerSecond = 0;
  framesPerSecond = 0;
}

static void resetLogBuffer() {
  g_logBufHead = 0;
  g_logBufTail = 0;
  g_logBufCount = 0;
  g_logBufMax = 0;
  g_logDropped = 0;
}

static bool pushLogSample(const LogSample& s) {
  if (g_logBufCount >= LOG_BUF_SIZE) {
    g_logDropped++;
    return false;
  }
  g_logBuf[g_logBufHead] = s;
  g_logBufHead = (uint16_t)((g_logBufHead + 1U) % LOG_BUF_SIZE);
  g_logBufCount++;
  if (g_logBufCount > g_logBufMax) g_logBufMax = g_logBufCount;
  return true;
}

static bool popLogSample(LogSample& s) {
  if (g_logBufCount == 0) return false;
  s = g_logBuf[g_logBufTail];
  g_logBufTail = (uint16_t)((g_logBufTail + 1U) % LOG_BUF_SIZE);
  g_logBufCount--;
  return true;
}

static void captureLogSample(uint32_t t_ms) {
  if (g_mode != RunMode::CONFIG) return;
  if (!cfg.sdLogEnabled) return;

  LogSample s{};
  s.t_ms = t_ms;
  s.dms_on = cfg.dmsEnabled ? 1U : 0U;
  s.dms_valid = dms_valid ? 1U : 0U;
  s.dms_raw = lastDmsRaw;
  s.dms_mV = dms_valid ? dms_mV : NAN;
  s.dms_mV_per_V = dms_valid ? dms_mV_per_V : NAN;
  s.ain2_on = cfg.ain2Enabled ? 1U : 0U;
  s.ain2_valid = ain2_valid ? 1U : 0U;
  s.ain2_mode = cfg.ain2Mode;
  s.ain2_raw = lastAin2Raw;
  s.ain2_mV = ain2_valid ? ain2_mV : NAN;
  s.ain2_value = ain2_valid ? ain2DerivedValue() : NAN;
  s.temp_on = cfg.tempEnabled ? 1U : 0U;
  s.temp_valid = temp_valid ? 1U : 0U;
  s.temp_C = temp_valid ? ds_temp_c : NAN;
  s.selftest_ok = selfTestOk ? 1U : 0U;
  pushLogSample(s);
}

// ============================================================
// SD logging
// ============================================================
static uint32_t g_lastSdMountTryMs = 0;
static uint32_t g_sdMountRetryMs = 3000;

static inline void sdSpiBegin() {
  pinMode(PIN_SD_CS, OUTPUT);
  digitalWrite(PIN_SD_CS, HIGH);
  sdBusPrepare();
  SPI.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI, PIN_SD_CS);
  delayMicroseconds(5);
}

static inline void sdSpiEnd() {
  digitalWrite(PIN_SD_CS, HIGH);
  delayMicroseconds(5);
  SPI.end();
  restoreAdsBusAfterSd();
}

static bool sdMountIfNeeded() {
  if (g_sdMounted) return true;
  if (g_mode != RunMode::CONFIG) return false;

  const uint32_t now = millis();
  if ((uint32_t)(now - g_lastSdMountTryMs) < g_sdMountRetryMs) {
    return false;
  }
  g_lastSdMountTryMs = now;

  g_sdMountTried = true;
  g_sdBusy = true;
  g_sdStatus = "mounting";

  bool ok = false;
  const uint32_t freqs[] = {1000000UL, 400000UL, 250000UL};
  for (size_t i = 0; i < sizeof(freqs) / sizeof(freqs[0]); ++i) {
    sdSpiBegin();
    ok = SD.begin(PIN_SD_CS, SPI, freqs[i]);
    sdSpiEnd();
    if (ok) break;
    delay(30);
  }

  g_sdMounted = ok;
  g_sdStatus = ok ? "mounted" : "mount_failed";
  g_sdBusy = false;

  if (!ok) {
    Serial.println("[SD] mount failed");
    return false;
  }

  Serial.println("[SD] mounted");
  return true;
}

static void closeLogFile() {
  g_sdBusy = true;

  if (g_logFile) {
    sdSpiBegin();
    g_logFile.flush();
    g_logFile.close();
    sdSpiEnd();
  } else {
    restoreAdsBusAfterSd();
  }

  g_sdMounted = false;
  g_sdBusy = false;
  g_logFileName = "";
  g_logLineCount = 0;
  g_lastLoggedSampleSeq = 0;
  resetLogBuffer();
}

static bool openNextLogFile() {
  if (!sdMountIfNeeded()) return false;

  if (g_logFile) {
    g_sdBusy = true;
    sdSpiBegin();
    g_logFile.flush();
    g_logFile.close();
    sdSpiEnd();
    g_sdBusy = false;

    g_logFileName = "";
    g_logLineCount = 0;
    g_lastLoggedSampleSeq = 0;
  }

  char path[64];
  for (uint32_t seq = g_logFileSeq + 1; seq < 1000000UL; ++seq) {
    snprintf(path, sizeof(path), "/%s_%03lu.csv", cfg.logBaseName, (unsigned long)seq);

    g_sdBusy = true;
    sdSpiBegin();

    Serial.print("[SD] probe file: ");
    Serial.println(path);

    bool exists = SD.exists(path);
    if (!exists) {
      g_logFile = SD.open(path, FILE_WRITE);
      if (!g_logFile) {
        sdSpiEnd();
        g_sdBusy = false;
        g_sdStatus = "open_failed";
        Serial.print("[SD] open failed: ");
        Serial.println(path);
        return false;
      }

      g_logFileSeq = seq;
      g_logFileName = String(path);
      g_logLineCount = 0;

      g_logFile.println("t_ms,mode,dms_on,dms_raw,dms_mV,dms_mV_per_V,ain2_on,ain2_mode,ain2_raw,ain2_mV,ain2_value,ain2_unit,temp_on,temp_C,selftest_ok");
      g_logFile.println("t_ms,mode,dms_on,dms_valid,dms_raw,dms_mV,dms_mV_per_V,ain2_on,ain2_valid,ain2_mode,ain2_raw,ain2_mV,ain2_value,ain2_unit,temp_on,temp_valid,temp_C,selftest_ok");
      g_logFile.flush();
      g_logLastFlushMs = millis();
      g_logLastDrainMs = millis();

      sdSpiEnd();
      g_sdBusy = false;

      g_sdStatus = "logging";
      Serial.print("[SD] logging to ");
      Serial.println(g_logFileName);
      return true;
    }

    sdSpiEnd();
    g_sdBusy = false;
  }

  g_sdStatus = "no_filename";
  return false;
}

static void syncSdLogger(bool forceReopen) {
  if (g_mode != RunMode::CONFIG) {
    closeLogFile();
    g_sdStatus = "field_disabled";
    return;
  }

  if (!cfg.sdLogEnabled) {
    closeLogFile();
    g_sdStatus = "disabled";
    return;
  }

  if (!sdMountIfNeeded()) return;

  if (forceReopen || !g_logFile) {
    (void)openNextLogFile();
  }
}

static void loggerLoop() {
  if (g_mode != RunMode::CONFIG) return;
  if (!cfg.sdLogEnabled) return;
  if (g_logBufCount == 0) return;

  const uint32_t now = millis();

  if (g_lastLoggedSampleSeq == g_lastSampleSeq) return;
  if ((uint32_t)(now - g_lastLogWriteMs) < g_logPeriodMs) return;
  if (g_logBufCount < LOG_BATCH_SIZE && (uint32_t)(now - g_logLastDrainMs) < LOG_FORCE_FLUSH_MS) {
    return;
  }

  syncSdLogger(false);
  if (!g_logFile) return;

  g_sdBusy = true;
  sdSpiBegin();

  if (cfg.logRotateKB > 0 && (uint32_t)g_logFile.size() >= cfg.logRotateKB * 1024UL) {
    sdSpiEnd();
    g_sdBusy = false;

    if (!openNextLogFile()) return;

    g_sdBusy = true;
    sdSpiBegin();
  }

  char line[256];
  snprintf(
    line, sizeof(line),
    "%lu,CONFIG,%d,%ld,%.6f,%.6f,%d,%s,%ld,%.6f,%.3f,%s,%d,%.3f,%d",
    (unsigned long)now,
    cfg.dmsEnabled ? 1 : 0,
    (long)lastDmsRaw,
    dms_valid ? dms_mV : NAN,
    dms_valid ? dms_mV_per_V : NAN,
    cfg.ain2Enabled ? 1 : 0,
    ain2ModeName(),
    (long)lastAin2Raw,
    ain2_valid ? ain2_mV : NAN,
    ain2_valid ? ain2DerivedValue() : NAN,
    ain2DerivedUnit(),
    cfg.tempEnabled ? 1 : 0,
    temp_valid ? ds_temp_c : NAN,
    selfTestOk ? 1 : 0
  );

  g_logFile.println(line);
  g_logLineCount++;
  g_lastLoggedSampleSeq = g_lastSampleSeq;
  g_lastLogWriteMs = now;
  char out[4096];
  size_t pos = 0;
  uint16_t writtenSamples = 0;
  LogSample s;

  while (writtenSamples < LOG_BATCH_SIZE && g_logBufCount > 0) {
    if (!popLogSample(s)) break;

    int n = snprintf(
      out + pos, sizeof(out) - pos,
      "%lu,CONFIG,%u,%u,%ld,%.6f,%.6f,%u,%u,%s,%ld,%.6f,%.3f,%s,%u,%u,%.3f,%u\n",
      (unsigned long)s.t_ms,
      (unsigned)s.dms_on,
      (unsigned)s.dms_valid,
      (long)s.dms_raw,
      s.dms_mV,
      s.dms_mV_per_V,
      (unsigned)s.ain2_on,
      (unsigned)s.ain2_valid,
      ain2ModeNameFor(s.ain2_mode),
      (long)s.ain2_raw,
      s.ain2_mV,
      s.ain2_value,
      ain2DerivedUnitFor(s.ain2_mode),
      (unsigned)s.temp_on,
      (unsigned)s.temp_valid,
      s.temp_C,
      (unsigned)s.selftest_ok
    );

    if (n <= 0 || (size_t)n >= (sizeof(out) - pos)) {
      // if buffer is full, put sample back and stop
      g_logBufTail = (uint16_t)((g_logBufTail + LOG_BUF_SIZE - 1U) % LOG_BUF_SIZE);
      g_logBuf[g_logBufTail] = s;
      g_logBufCount++;
      break;
    }

    pos += (size_t)n;
    writtenSamples++;
  }

  if (pos > 0) {
    size_t wrote = g_logFile.write((const uint8_t*)out, pos);
    if (wrote == pos) {
      g_logLineCount += writtenSamples;
      g_lastLoggedSampleSeq = g_lastSampleSeq;
      g_lastLogWriteMs = now;
    }
  }

  if ((uint32_t)(now - g_logLastFlushMs) >= g_logFlushPeriodMs) {
    g_logFile.flush();
    g_logLastFlushMs = now;
  }

  g_logLastDrainMs = now;
  sdSpiEnd();
  g_sdBusy = false;
}

// ============================================================
// JSON / Web
// ============================================================
static String modeToString() {
  return (g_mode == RunMode::CONFIG) ? "CONFIG" : "FIELD";
}

static void appendPerf(JsonObject obj) {
  PerfEstimate p = makePerfEstimate();
  obj["dms_target_hz"] = p.dmsTargetHz;
  obj["ain2_target_hz"] = p.ain2TargetHz;
  obj["temp_target_hz"] = p.tempTargetHz;
  obj["ads_conv_hz"] = p.adsRequestedConvHz;
  obj["ads_load_pct"] = p.adsLoadPct;
  obj["dms_est_hz"] = p.dmsEstimatedHz;
  obj["ain2_est_hz"] = p.ain2EstimatedHz;
  obj["total_est_hz"] = p.totalEstimatedHz;
}

static void appendConfig(JsonObject obj) {
  obj["devEui"] = cfg.devEui;
  obj["appEui"] = cfg.appEui;
  obj["appKey"] = cfg.appKey;
  obj["minutes"] = cfg.intervalMin;
  obj["adr"] = cfg.adr;
  obj["dr"] = cfg.dr;

  obj["dms_enabled"] = cfg.dmsEnabled ? 1 : 0;
  obj["dms_hz"] = cfg.dmsHz;

  obj["ain2_enabled"] = cfg.ain2Enabled ? 1 : 0;
  obj["ain2_hz"] = cfg.ain2Hz;
  obj["ain2_mode"] = ain2ModeName();
  obj["ain2_length_mm"] = cfg.ain2LengthMm;
  obj["ain2_adc_fullscale_v"] = cfg.ain2AdcFullscaleV;
  obj["ain2_input_fullscale_v"] = cfg.ain2InputFullscaleV;

  obj["temp_enabled"] = cfg.tempEnabled ? 1 : 0;
  obj["temp_hz"] = TEMP_FIXED_HZ;

  obj["sd_log_enabled"] = cfg.sdLogEnabled ? 1 : 0;
  obj["sd_base"] = cfg.logBaseName;
  obj["sd_rotate_kb"] = cfg.logRotateKB;

  JsonObject perfObj = obj["perf"].to<JsonObject>();
  appendPerf(perfObj);
}

static void appendLive(JsonObject obj) {
  obj["selftest_ok"] = selfTestOk ? 1 : 0;
  obj["selftest_delta_mV"] = selfTestDelta_mV;

  obj["dms_enabled"] = cfg.dmsEnabled ? 1 : 0;
  obj["dms_valid"] = dms_valid ? 1 : 0;
  obj["dms_raw"] = lastDmsRaw;
  obj["dms_mV"] = dms_valid ? dms_mV : 0.0f;
  obj["dms_mV_per_V"] = dms_valid ? dms_mV_per_V : 0.0f;
  obj["dms_rate_hz_actual"] = dmsFramesPerSecond;

  obj["ain2_enabled"] = cfg.ain2Enabled ? 1 : 0;
  obj["ain2_valid"] = ain2_valid ? 1 : 0;
  obj["ain2_raw"] = lastAin2Raw;
  obj["ain2_mV"] = ain2_valid ? ain2_mV : 0.0f;
  obj["ain2_mode"] = ain2ModeName();
  obj["ain2_value"] = ain2_valid ? ain2DerivedValue() : 0.0f;
  obj["ain2_unit"] = ain2DerivedUnit();
  obj["ain2_rate_hz_actual"] = ain2FramesPerSecond;

  obj["temp_enabled"] = cfg.tempEnabled ? 1 : 0;
  obj["temp_valid"] = temp_valid ? 1 : 0;
  obj["temp_C"] = temp_valid ? ds_temp_c : 0.0f;
  obj["temp_rate_hz_actual"] = tempFramesPerSecond;

  obj["frames_s"] = framesPerSecond;
  obj["total_rate_hz_actual"] = totalFramesPerSecond;

  JsonObject perfObj = obj["perf"].to<JsonObject>();
  appendPerf(perfObj);

  obj["sd_mounted"] = g_sdMounted ? 1 : 0;
  obj["sd_status"] = g_sdStatus;
  obj["sd_file"] = g_logFileName;
  obj["sd_lines"] = g_logLineCount;
  obj["sd_buffered"] = g_logBufCount;
  obj["sd_buffered_max"] = g_logBufMax;
  obj["sd_dropped"] = g_logDropped;
}

static void appendController(JsonObject obj) {
  char mac48[13];
  char chipEui[17];
  chipMac48Hex(mac48);
  chipDevEuiHex(chipEui);

  obj["board"] = BOARD_NAME;
  obj["chip_model"] = ESP.getChipModel();
  obj["chip_rev"] = ESP.getChipRevision();
  obj["cpu_mhz"] = ESP.getCpuFreqMHz();
  obj["flash_mb"] = (uint32_t)(ESP.getFlashChipSize() / (1024UL * 1024UL));
  obj["chip_mac48"] = mac48;
  obj["chip_eui64"] = chipEui;
  obj["ap_ssid"] = (g_mode == RunMode::CONFIG) ? g_apSsid : String("-");
  obj["sd_status"] = g_sdStatus;
}

static void handleApiLive() {
  StaticJsonDocument<1024> doc;
  doc["mode"] = modeToString();
  appendLive(doc.to<JsonObject>());
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

static void handleApiState() {
  StaticJsonDocument<2304> doc;
  doc["fw"] = FW_VERSION;
  doc["build_date"] = __DATE__;
  doc["build_time"] = __TIME__;
  doc["mode"] = modeToString();
  doc["ip"] = (g_mode == RunMode::CONFIG) ? g_apIp.toString() : String("-");
  doc["uptime_s"] = millis() / 1000UL;
  doc["heap_free"] = ESP.getFreeHeap();
  doc["lora_enabled"] = g_loraEnabled ? 1 : 0;
  doc["lora_init_done"] = g_loraInitDone ? 1 : 0;
  appendLive(doc.to<JsonObject>());
  appendConfig(doc["cfg"].to<JsonObject>());
  appendController(doc["controller"].to<JsonObject>());
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

static void handleApiConfigGet() {
  StaticJsonDocument<1024> doc;
  appendConfig(doc.to<JsonObject>());
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

static void handleApiConfigPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"missing body\"}");
    return;
  }

  StaticJsonDocument<1536> doc;
  DeserializationError err = deserializeJson(doc, server.arg("plain"));
  if (err) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"json\"}");
    return;
  }

  Cfg next = cfg;

  if (!doc["devEui"].isNull()) copyUpperNoSpace(next.devEui, sizeof(next.devEui), doc["devEui"].as<const char*>());
  if (!doc["appEui"].isNull()) copyUpperNoSpace(next.appEui, sizeof(next.appEui), doc["appEui"].as<const char*>());
  if (!doc["appKey"].isNull()) copyUpperNoSpace(next.appKey, sizeof(next.appKey), doc["appKey"].as<const char*>());
  if (!doc["minutes"].isNull()) next.intervalMin = doc["minutes"].as<uint16_t>();
  if (!doc["adr"].isNull()) next.adr = doc["adr"].as<bool>();
  if (!doc["dr"].isNull()) next.dr = doc["dr"].as<uint8_t>();

  if (!doc["dms_enabled"].isNull()) next.dmsEnabled = doc["dms_enabled"].as<int>() != 0;
  if (!doc["dms_hz"].isNull()) next.dmsHz = doc["dms_hz"].as<float>();

  if (!doc["ain2_enabled"].isNull()) next.ain2Enabled = doc["ain2_enabled"].as<int>() != 0;
  if (!doc["ain2_hz"].isNull()) next.ain2Hz = doc["ain2_hz"].as<float>();
  if (!doc["ain2_mode"].isNull()) {
    const char* m = doc["ain2_mode"].as<const char*>();
    next.ain2Mode = (m && strcmp(m, "volt") == 0) ? AIN2_MODE_VOLT : AIN2_MODE_POT;
  }
  if (!doc["ain2_length_mm"].isNull()) next.ain2LengthMm = doc["ain2_length_mm"].as<float>();
  if (!doc["ain2_adc_fullscale_v"].isNull()) next.ain2AdcFullscaleV = doc["ain2_adc_fullscale_v"].as<float>();
  if (!doc["ain2_input_fullscale_v"].isNull()) next.ain2InputFullscaleV = doc["ain2_input_fullscale_v"].as<float>();

  if (!doc["temp_enabled"].isNull()) next.tempEnabled = doc["temp_enabled"].as<int>() != 0;

  if (!doc["sd_log_enabled"].isNull()) next.sdLogEnabled = doc["sd_log_enabled"].as<int>() != 0;
  if (!doc["sd_base"].isNull()) {
    const char* base = doc["sd_base"].as<const char*>();
    if (base) strlcpy(next.logBaseName, base, sizeof(next.logBaseName));
  }
  if (!doc["sd_rotate_kb"].isNull()) next.logRotateKB = doc["sd_rotate_kb"].as<uint32_t>();

  normalizeCfg(next);
  String error;
  if (!validateCfg(next, &error)) {
    String out = String("{\"ok\":false,\"error\":\"") + error + "\"}";
    server.send(400, "application/json", out);
    return;
  }

  bool reopenSd = (strcmp(next.logBaseName, cfg.logBaseName) != 0) || (next.sdLogEnabled != cfg.sdLogEnabled) || (next.logRotateKB != cfg.logRotateKB);

  cfg = next;
  saveCfg();
  applyLoraConfig();
  if (!cfg.dmsEnabled) invalidateDms();
  if (!cfg.ain2Enabled) invalidateAin2();
  if (!cfg.tempEnabled) invalidateTemp();
  if (g_mode == RunMode::CONFIG) syncSdLogger(reopenSd);

  StaticJsonDocument<1152> resp;
  resp["ok"] = true;
  appendConfig(resp["cfg"].to<JsonObject>());
  String out;
  serializeJson(resp, out);
  server.send(200, "application/json", out);
}

static void handleApiReset() {
  server.send(200, "application/json", "{\"ok\":true}");
  delay(200);
  ESP.restart();
}

static void handleApiDevEuiGet() {
  StaticJsonDocument<384> doc;
  char mac48[13];
  char chipHex[17];
  chipMac48Hex(mac48);
  chipDevEuiHex(chipHex);
  doc["activeLoRaDevEUI"] = cfg.devEui;
  doc["chipMac48"] = mac48;
  doc["chipEUI"] = chipHex;
  doc["storedDevEUI"] = cfg.devEui;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

static void handleApiDevEuiPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"missing body\"}");
    return;
  }

  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, server.arg("plain"));
  if (err) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"json\"}");
    return;
  }

  Cfg next = cfg;
  if (!doc["use"].isNull()) {
    const char* use = doc["use"].as<const char*>();
    if (use && strcmp(use, "chip") == 0) {
      char chipHex[17];
      chipDevEuiHex(chipHex);
      strlcpy(next.devEui, chipHex, sizeof(next.devEui));
    }
  }
  if (!doc["devEui"].isNull()) copyUpperNoSpace(next.devEui, sizeof(next.devEui), doc["devEui"].as<const char*>());

  normalizeCfg(next);
  String error;
  if (!validateCfg(next, &error)) {
    String out = String("{\"ok\":false,\"error\":\"") + error + "\"}";
    server.send(400, "application/json", out);
    return;
  }

  cfg = next;
  saveCfg();
  applyLoraConfig();
  handleApiDevEuiGet();
}

static bool serveFile(const char* path, const char* mime) {
  if (!LittleFS.exists(path)) return false;
  File f = LittleFS.open(path, "r");
  if (!f) return false;
  server.streamFile(f, mime);
  f.close();
  return true;
}

static void handleRoot() {
  if (serveFile("/index.html", "text/html")) return;
  server.send(200, "text/html",
              "<html><body><h1>MKP LoRaSense</h1>"
              "<p><a href=\"/api/state\">/api/state</a></p>"
              "</body></html>");
}

static void startApAndWeb() {
  if (!LittleFS.begin(true)) {
    Serial.println("[FS] LittleFS mount failed");
  }

  g_apSsid = String("LoRaSense-") + String(&cfg.devEui[8]);
  WiFi.mode(WIFI_AP);
  WiFi.softAP(g_apSsid.c_str());
  g_apIp = WiFi.softAPIP();

  server.on("/", HTTP_GET, handleRoot);
  server.on("/index.html", HTTP_GET, handleRoot);
  server.on("/i18n.json", HTTP_GET, []() {
    if (!serveFile("/i18n.json", "application/json")) server.send(404, "text/plain", "not found");
  });
  server.on("/logo.svg", HTTP_GET, []() {
    if (!serveFile("/logo.svg", "image/svg+xml")) server.send(404, "text/plain", "not found");
  });
  server.on("/api/live", HTTP_GET, handleApiLive);
  server.on("/api/state", HTTP_GET, handleApiState);
  server.on("/api/config", HTTP_GET, handleApiConfigGet);
  server.on("/api/config", HTTP_POST, handleApiConfigPost);
  server.on("/api/reset", HTTP_POST, handleApiReset);
  server.on("/api/deveui", HTTP_GET, handleApiDevEuiGet);
  server.on("/api/deveui", HTTP_POST, handleApiDevEuiPost);
  server.begin();

  syncSdLogger(true);

  Serial.print("[AP] SSID=");
  Serial.print(g_apSsid);
  Serial.print(" IP=");
  Serial.println(g_apIp);
}

// ============================================================
// LoRa
// ============================================================
static void prepareTxFrame(uint8_t port) {
  (void)port;
  StaticJsonDocument<384> doc;
  doc["dms_mV"] = dms_valid ? dms_mV : 0.0f;
  doc["dms_mV_per_V"] = dms_valid ? dms_mV_per_V : 0.0f;
  doc["ain2_mV"] = ain2_valid ? ain2_mV : 0.0f;
  if (cfg.ain2Mode == AIN2_MODE_VOLT) doc["ain2_V"] = ain2_valid ? ain2DerivedValue() : 0.0f;
  else doc["ain2_mm"] = ain2_valid ? ain2DerivedValue() : 0.0f;
  doc["temp_C"] = temp_valid ? ds_temp_c : 0.0f;
  doc["frames_s"] = framesPerSecond;
  doc["selftest_ok"] = selfTestOk ? 1 : 0;
  appDataSize = (uint8_t)serializeJson(doc, appData, LORAWAN_APP_DATA_MAX_SIZE);
}

static void loraLoop() {
  if (!g_loraEnabled) return;

  switch (deviceState) {
    case DEVICE_STATE_INIT:
      Serial.println("[LORA] INIT");
      syncLoraCompatAliases();
      dumpLoraBuffers("buffers before LoRaWAN.init");
      LoRaWAN.init(loraWanClass, loraWanRegion);
      LoRaWAN.setDefaultDR(cfg.dr);
      g_loraInitDone = true;
      deviceState = DEVICE_STATE_JOIN;
      break;

    case DEVICE_STATE_JOIN:
      Serial.println("[LORA] JOIN");
      g_radioQuietUntilMs = millis() + 7000UL;
      LoRaWAN.join();
      break;

    case DEVICE_STATE_SEND:
      Serial.println("[LORA] SEND");
      captureFieldSnapshot();
      prepareTxFrame(appPort);
      g_radioQuietUntilMs = millis() + 3000UL;
      LoRaWAN.send();
      deviceState = DEVICE_STATE_CYCLE;
      break;

    case DEVICE_STATE_CYCLE:
      Serial.println("[LORA] CYCLE");
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

// ============================================================
// Setup / loop
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(300);

  loadCfg();
  applyLoraConfig();

  g_mode = isApModeRequested() ? RunMode::CONFIG : RunMode::FIELD;

  adsInit();
  dsInit();
  runSelfTest();
  if (cfg.dmsEnabled) restoreDmsRunMode();

  Serial.print("FW ");
  Serial.print(FW_VERSION);
  Serial.print(" build ");
  Serial.print(__DATE__);
  Serial.print(" ");
  Serial.println(__TIME__);
  Serial.print("Mode: ");
  Serial.println(modeToString());
  Serial.print("DevEUI=");
  Serial.print(cfg.devEui);
  Serial.print(" AppEUI=");
  Serial.print(cfg.appEui);
  Serial.print(" AppKey=");
  Serial.println(cfg.appKey);
  Serial.print("LoRa interval: ");
  Serial.print(appTxDutyCycle);
  Serial.println(" ms");

  Serial.println("[ADS] software SPI on fixed PCB pins; LoRa SPI isolated");
  dumpLoraBuffers("buffers before Mcu.begin");

  if (g_mode == RunMode::FIELD) {
    WiFi.persistent(false);
    WiFi.disconnect(true, true);
    WiFi.mode(WIFI_OFF);
    delay(20);
    Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);
    g_loraEnabled = true;
    g_sdStatus = "field_disabled";
  } else {
    startApAndWeb();
    g_loraEnabled = false;
    g_sdStatus = cfg.sdLogEnabled ? g_sdStatus : "disabled";
  }
}

void loop() {
  if (g_mode == RunMode::CONFIG) {
    sensorLoopAp();
    loggerLoop();
    server.handleClient();
    delay(1);
  } else {
    loraLoop();
    delay(2);
  }
}
