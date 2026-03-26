#include <Arduino.h>
#include <SPI.h>
#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <LittleFS.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include "LoRaWan_APP.h"

// ============================================================
// LoRaSense v3
// Mode logic:
//   - FIELD MODE  : jumper inactive -> Sensors + LoRaWAN
//   - CONFIG MODE : jumper active   -> Sensors + AP/Web
// This avoids AP/LoRa startup conflicts.
// ============================================================

// ---------------- PINS ----------------
static constexpr int PIN_SPI_MISO   = 39;
static constexpr int PIN_SPI_SCK    = 2;
static constexpr int PIN_SPI_MOSI   = 1;
static constexpr int PIN_SD_CS      = 7;
static constexpr int PIN_ADS_CS     = 38;
static constexpr int PIN_ADS_DRDY   = 4;
static constexpr int PIN_MOSFET     = 5;
static constexpr int PIN_DS18B20    = 6;
static constexpr int PIN_AP_MODE    = 46;   // jumper to GND => config/AP mode

static WebServer server(80);
static Preferences prefs;

// ---------------- ADS1220 ----------------
namespace ADS1220 {
  static constexpr uint8_t CMD_RESET = 0x06;
  static constexpr uint8_t CMD_START = 0x08;
  static constexpr uint8_t CMD_RDATA = 0x10;
  static constexpr uint8_t CMD_WREG  = 0x40;
}

// ---------------- ADS SETTINGS ----------------
static constexpr int32_t ADS_FS = 1 << 23;
static constexpr float   ADS_VREF_VOLTS = 3.3f;

static constexpr uint8_t REG0_DMS  = 0x0E; // AIN0-AIN1, GAIN=128, PGA enabled
static constexpr uint8_t REG0_AIN2 = 0xA1; // AIN2-AVSS, GAIN=1, PGA_BYPASS=1

static constexpr float GAIN_DMS  = 128.0f;
static constexpr float GAIN_AIN2 = 1.0f;

static constexpr float EXPECTED_MV  = 3.8f;
static constexpr float TOLERANCE_MV = 1.0f;
static constexpr int   RETRY_MAX    = 2;
static constexpr uint32_t DRDY_TIMEOUT_US = 10000;

static constexpr uint8_t ADS_REG1 = 0xA0;   // normal mode, 600 SPS, single-shot
static constexpr uint8_t ADS_REG2 = 0x40;   // ext ref
static constexpr uint8_t ADS_REG3 = 0x00;

// ---------------- DS18B20 ----------------
static OneWire oneWire(PIN_DS18B20);
static DallasTemperature dsSensor(&oneWire);

// ---------------- CONFIG ----------------
struct Cfg {
  char appEui[17];
  char appKey[33];
  uint16_t intervalMin;
  bool adr;
  uint8_t dr;
  uint32_t tempPeriodMs;
  uint32_t ain2PeriodMs;
};

static Cfg cfg = {
  "0000000000000000",
  "636AC09B24824A4730328058CE636ADF",
  15,
  true,
  3,
  1000,
  1000
};

static char g_devEuiHex[17] = "631E5618DF8EB3D4";

// ---------------- RUNTIME ----------------
enum class RunMode : uint8_t {
  FIELD = 0,
  CONFIG = 1
};

static RunMode g_mode = RunMode::FIELD;
static bool g_apStarted = false;
static bool g_loraEnabled = false;
static bool g_loraInitDone = false;
static uint32_t g_radioQuietUntilMs = 0;

static float dms_mV = NAN;
static float dms_mV_per_V = NAN;
static float ain2_mV = NAN;
static float ds_temp_c = NAN;
static uint32_t framesPerSecond = 0;
static int32_t lastDmsRaw = 0;

static bool selfTestOk = false;
static float selfTestDelta_mV = NAN;

static bool ds_pending = false;
static uint32_t ds_request_ms = 0;
static uint32_t last_ds_request_loop_ms = 0;

static uint32_t last_ain2_read_ms = 0;

// ---------------- LoRaWAN globals required by Heltec ----------------
uint8_t devEui[8]   = { 0x63, 0x1E, 0x56, 0x18, 0xDF, 0x8E, 0xB3, 0xD4 };
uint8_t appEui[8]   = { 0 };
uint8_t appKey[16]  = { 0 };
uint8_t nwkSKey[16] = { 0 };
uint8_t appSKey[16] = { 0 };
uint32_t devAddr    = 0;

// Compatibility aliases for older Heltec cores that still reference
// the historical uppercase symbols.
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

// ---------------- Helpers ----------------
static inline void adsCS(bool en) { digitalWrite(PIN_ADS_CS, en ? LOW : HIGH); }

// ADS1220 uses the same fixed PCB pins (GPIO 2/39/1), but via bit-banged SPI,
// so the Heltec LoRaWAN stack can keep the global SPI bus exclusively for the
// onboard SX1262 radio. This is a software-only fix: no PCB reroute is needed.
static inline void adsSck(bool high) { digitalWrite(PIN_SPI_SCK, high ? HIGH : LOW); }
static inline void adsMosi(bool high) { digitalWrite(PIN_SPI_MOSI, high ? HIGH : LOW); }

static uint8_t adsXfer(uint8_t b) {
  uint8_t rx = 0;
  for (int bit = 7; bit >= 0; --bit) {
    adsMosi((b >> bit) & 0x01);
    delayMicroseconds(1);
    adsSck(true);
    delayMicroseconds(1);
    adsSck(false);
    delayMicroseconds(1);
    rx = (uint8_t)((rx << 1) | (digitalRead(PIN_SPI_MISO) ? 1 : 0));
  }
  return rx;
}

static bool isApModeRequested() {
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
    out[i * 2]     = lut[(in[i] >> 4) & 0x0F];
    out[i * 2 + 1] = lut[in[i] & 0x0F];
  }
  out[len * 2] = '\0';
}


static void syncLoraCompatAliases() {
  memcpy(DevEui, devEui, sizeof(devEui));
  memcpy(AppEui, appEui, sizeof(appEui));
  memcpy(AppKey, appKey, sizeof(appKey));
  memcpy(NwkSKey, nwkSKey, sizeof(nwkSKey));
  memcpy(AppSKey, appSKey, sizeof(appSKey));
  DevAddr = devAddr;
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
  prefs.begin("lorasense", true);
  String aE = prefs.getString("appEui", cfg.appEui);
  String aK = prefs.getString("appKey", cfg.appKey);
  cfg.intervalMin = prefs.getUShort("minutes", cfg.intervalMin);
  cfg.adr = prefs.getBool("adr", cfg.adr);
  cfg.dr = prefs.getUChar("dr", cfg.dr);
  cfg.tempPeriodMs = prefs.getUInt("tper", cfg.tempPeriodMs);
  cfg.ain2PeriodMs = prefs.getUInt("aper", cfg.ain2PeriodMs);
  prefs.end();

  strlcpy(cfg.appEui, aE.c_str(), sizeof(cfg.appEui));
  strlcpy(cfg.appKey, aK.c_str(), sizeof(cfg.appKey));
}

static void saveCfg() {
  prefs.begin("lorasense", false);
  prefs.putString("appEui", cfg.appEui);
  prefs.putString("appKey", cfg.appKey);
  prefs.putUShort("minutes", cfg.intervalMin);
  prefs.putBool("adr", cfg.adr);
  prefs.putUChar("dr", cfg.dr);
  prefs.putUInt("tper", cfg.tempPeriodMs);
  prefs.putUInt("aper", cfg.ain2PeriodMs);
  prefs.end();
}

static void applyLoraConfig() {
  memset(appEui, 0, sizeof(appEui));
  memset(appKey, 0, sizeof(appKey));
  memset(nwkSKey, 0, sizeof(nwkSKey));
  memset(appSKey, 0, sizeof(appSKey));
  devAddr = 0;

  bool okDev = parseHexToBytes(g_devEuiHex, devEui, sizeof(devEui));
  bool okAppEui = parseHexToBytes(cfg.appEui, appEui, sizeof(appEui));
  bool okAppKey = parseHexToBytes(cfg.appKey, appKey, sizeof(appKey));

  if (!okDev) {
    Serial.println("[LORA] ERROR: invalid DevEUI hex");
  }
  if (!okAppEui) {
    Serial.println("[LORA] ERROR: invalid AppEUI hex (need 16 hex chars)");
  }
  if (!okAppKey) {
    Serial.println("[LORA] ERROR: invalid AppKey hex (need 32 hex chars)");
  }

  syncLoraCompatAliases();

  appTxDutyCycle = (uint32_t)cfg.intervalMin * 60UL * 1000UL;
  loraWanAdr = cfg.adr;
}

static void prepareTxFrame(uint8_t port) {
  (void)port;

  StaticJsonDocument<192> doc;
  doc["dms_mV"] = isnan(dms_mV) ? 0.0f : dms_mV;
  doc["dms_mV_per_V"] = isnan(dms_mV_per_V) ? 0.0f : dms_mV_per_V;
  doc["ain2_mV"] = isnan(ain2_mV) ? 0.0f : ain2_mV;
  doc["temp_C"] = isnan(ds_temp_c) ? 0.0f : ds_temp_c;
  doc["frames_s"] = framesPerSecond;
  doc["selftest_ok"] = selfTestOk ? 1 : 0;

  appDataSize = (uint8_t)serializeJson(doc, appData, LORAWAN_APP_DATA_MAX_SIZE);
}

// ---------------- ADS low-level ----------------
static void adsCommand(uint8_t cmd) {
  adsCS(true);
  adsXfer(cmd);
  adsCS(false);
}

static void adsWriteRegs4(uint8_t r0, uint8_t r1, uint8_t r2, uint8_t r3) {
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x03);
  adsXfer(r0);
  adsXfer(r1);
  adsXfer(r2);
  adsXfer(r3);
  adsCS(false);
}

static void adsWriteReg0(uint8_t r0) {
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x00);
  adsXfer(r0);
  adsCS(false);
}

static int32_t adsReadRaw_RDATA() {
  adsCS(true);
  adsXfer(ADS1220::CMD_RDATA);
  uint8_t b2 = adsXfer(0xFF);
  uint8_t b1 = adsXfer(0xFF);
  uint8_t b0 = adsXfer(0xFF);
  adsCS(false);

  int32_t raw = ((uint32_t)b2 << 16) | ((uint32_t)b1 << 8) | b0;
  if (raw & 0x00800000) raw |= 0xFF000000;
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
  return -1;
}

static bool readChannel(uint8_t reg0, float gain, float& out_mV) {
  adsWriteReg0(reg0);
  (void)readSingleShot_DRDY();  // discard after mux switch
  int32_t raw = readSingleShot_DRDY();

  if (raw == -1) {
    out_mV = NAN;
    return false;
  }

  out_mV = rawTo_mV_gain(raw, gain);
  return true;
}

static bool readDmsFast(float& out_mV, int32_t* rawOut = nullptr) {
  int32_t raw = readSingleShot_DRDY();

  if (raw == -1) {
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
  adsSck(false);
  pinMode(PIN_SPI_MISO, INPUT);
  pinMode(PIN_SPI_MOSI, OUTPUT);
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
  readDmsFast(dummy);
}

static bool runSelfTest() {
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
    if (!readChannel(REG0_DMS, GAIN_DMS, mv)) continue;
    base5[i++] = mv;
  }

  float bAvg = avg5(base5);

  digitalWrite(PIN_MOSFET, HIGH);
  delay(50);

  for (int i = 0; i < 5; ) {
    float mv = NAN;
    if (!readChannel(REG0_DMS, GAIN_DMS, mv)) continue;
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

// ---------------- Sensor runtime ----------------
static void dsInit() {
  dsSensor.begin();
  dsSensor.setWaitForConversion(false);

  uint8_t devCount = dsSensor.getDeviceCount();
  for (uint8_t i = 0; i < devCount; ++i) {
    DeviceAddress addr;
    if (dsSensor.getAddress(addr, i)) {
      dsSensor.setResolution(addr, 9);
    }
  }

  ds_pending = false;
  ds_request_ms = 0;
  last_ds_request_loop_ms = 0;
}

static void sensorLoop() {
  static uint32_t frameCount = 0;
  static uint32_t lastFrameReportMs = 0;

  uint32_t now = millis();
  bool radioQuiet = g_loraEnabled && ((int32_t)(now - g_radioQuietUntilMs) < 0);

  if (!radioQuiet) {
    if (readDmsFast(dms_mV, &lastDmsRaw)) {
      dms_mV_per_V = dmsTo_mV_per_V(dms_mV);
      frameCount++;
    } else {
      dms_mV_per_V = NAN;
      lastDmsRaw = 0;
    }

    if (now - last_ain2_read_ms >= cfg.ain2PeriodMs) {
      float tmpAin2 = NAN;
      if (readChannel(REG0_AIN2, GAIN_AIN2, tmpAin2)) {
        ain2_mV = tmpAin2;
      }
      restoreDmsRunMode();
      last_ain2_read_ms = now;
    }
  }

  if (now - lastFrameReportMs >= 1000) {
    framesPerSecond = frameCount;
    frameCount = 0;
    lastFrameReportMs = now;
  }

  if (!ds_pending && (now - last_ds_request_loop_ms >= cfg.tempPeriodMs)) {
    dsSensor.requestTemperatures();
    ds_pending = true;
    ds_request_ms = now;
    last_ds_request_loop_ms = now;
  }

  if (ds_pending && (now - ds_request_ms >= 150)) {
    float tmp = dsSensor.getTempCByIndex(0);
    ds_temp_c = (tmp != DEVICE_DISCONNECTED_C) ? tmp : NAN;
    ds_pending = false;
  }
}

// ---------------- AP/Web ----------------
static String modeToString() {
  return (g_mode == RunMode::CONFIG) ? "CONFIG" : "FIELD";
}

static void handleApiState() {
  StaticJsonDocument<512> doc;
  doc["mode"] = modeToString();
  doc["ap_mode_pin"] = isApModeRequested() ? 1 : 0;
  doc["lora_enabled"] = g_loraEnabled ? 1 : 0;
  doc["lora_init_done"] = g_loraInitDone ? 1 : 0;
  doc["selftest_ok"] = selfTestOk ? 1 : 0;
  doc["selftest_delta_mV"] = selfTestDelta_mV;
  doc["dms_raw"] = lastDmsRaw;
  doc["dms_mV"] = isnan(dms_mV) ? 0.0f : dms_mV;
  doc["dms_mV_per_V"] = isnan(dms_mV_per_V) ? 0.0f : dms_mV_per_V;
  doc["ain2_mV"] = isnan(ain2_mV) ? 0.0f : ain2_mV;
  doc["temp_C"] = isnan(ds_temp_c) ? 0.0f : ds_temp_c;
  doc["frames_s"] = framesPerSecond;
  doc["devEui"] = g_devEuiHex;
  doc["appEui"] = cfg.appEui;
  doc["appKey"] = cfg.appKey;
  doc["minutes"] = cfg.intervalMin;
  doc["adr"] = cfg.adr ? 1 : 0;
  doc["dr"] = cfg.dr;
  doc["temp_period_ms"] = cfg.tempPeriodMs;
  doc["ain2_period_ms"] = cfg.ain2PeriodMs;

  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

static void handleApiConfigGet() {
  handleApiState();
}

static void handleApiConfigPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"missing body\"}");
    return;
  }

  StaticJsonDocument<384> doc;
  DeserializationError err = deserializeJson(doc, server.arg("plain"));
  if (err) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"json\"}");
    return;
  }

  if (doc["appEui"].is<const char*>()) {
    strlcpy(cfg.appEui, doc["appEui"], sizeof(cfg.appEui));
  }
  if (doc["appKey"].is<const char*>()) {
    strlcpy(cfg.appKey, doc["appKey"], sizeof(cfg.appKey));
  }
  if (doc["minutes"].is<uint16_t>()) {
    cfg.intervalMin = doc["minutes"];
  }
  if (doc["adr"].is<bool>()) {
    cfg.adr = doc["adr"];
  }
  if (doc["dr"].is<uint8_t>()) {
    cfg.dr = doc["dr"];
  }
  if (doc["temp_period_ms"].is<uint32_t>()) {
    cfg.tempPeriodMs = doc["temp_period_ms"];
  }
  if (doc["ain2_period_ms"].is<uint32_t>()) {
    cfg.ain2PeriodMs = doc["ain2_period_ms"];
  }

  saveCfg();
  applyLoraConfig();
  server.send(200, "application/json", "{\"ok\":true}");
}

static void handleApiSend() {
  if (g_mode != RunMode::FIELD || !g_loraEnabled) {
    server.send(409, "application/json", "{\"ok\":false,\"error\":\"lora disabled in config mode\"}");
    return;
  }
  deviceState = DEVICE_STATE_SEND;
  server.send(200, "application/json", "{\"ok\":true}");
}

static void handleApiReset() {
  server.send(200, "application/json", "{\"ok\":true}");
  delay(200);
  ESP.restart();
}

static void handleApiDevEui() {
  StaticJsonDocument<128> doc;
  doc["activeLoRaDevEUI"] = g_devEuiHex;

  uint64_t chipMac = ESP.getEfuseMac();
  uint8_t chipBytes[8];
  for (int i = 0; i < 8; ++i) {
    chipBytes[7 - i] = (uint8_t)((chipMac >> (i * 8)) & 0xFF);
  }
  char chipHex[17];
  bytesToHex(chipBytes, 8, chipHex);
  doc["chipEUI"] = chipHex;

  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
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
              "<html><body><h1>MKP LoRaSense v3</h1>"
              "<p>Mode: " + modeToString() + "</p>"
              "<p><a href=\"/api/state\">/api/state</a></p>"
              "</body></html>");
}

static void startApAndWeb() {
  if (!LittleFS.begin(true)) {
    Serial.println("[FS] LittleFS mount failed");
  }

  String ssid = String("LoRaSense-") + String(&g_devEuiHex[8]);
  WiFi.mode(WIFI_AP);
  WiFi.softAP(ssid.c_str());
  IPAddress ip = WiFi.softAPIP();

  server.on("/", HTTP_GET, handleRoot);
  server.on("/i18n.json", HTTP_GET, []() {
    if (!serveFile("/i18n.json", "application/json")) {
      server.send(404, "text/plain", "not found");
    }
  });
  server.on("/api/state", HTTP_GET, handleApiState);
  server.on("/api/config", HTTP_GET, handleApiConfigGet);
  server.on("/api/config", HTTP_POST, handleApiConfigPost);
  server.on("/api/send", HTTP_POST, handleApiSend);
  server.on("/api/reset", HTTP_POST, handleApiReset);
  server.on("/api/deveui", HTTP_GET, handleApiDevEui);
  server.begin();

  g_apStarted = true;
  Serial.print("[AP] SSID=");
  Serial.print(ssid);
  Serial.print(" IP=");
  Serial.println(ip);
}

// ---------------- LoRa loop ----------------
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

// ---------------- Setup / loop ----------------
void setup() {
  Serial.begin(115200);
  delay(300);

  loadCfg();
  applyLoraConfig();

  g_mode = isApModeRequested() ? RunMode::CONFIG : RunMode::FIELD;

  adsInit();
  dsInit();
  runSelfTest();
  restoreDmsRunMode();

  Serial.print("FW 1.1.2-v3safe build ");
  Serial.print(__DATE__);
  Serial.print(" ");
  Serial.println(__TIME__);
  Serial.print("Mode: ");
  Serial.println(modeToString());
  Serial.print("DevEUI=");
  Serial.print(g_devEuiHex);
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
  } else {
    startApAndWeb();
    g_loraEnabled = false;
  }
}

void loop() {
  sensorLoop();

  if (g_mode == RunMode::FIELD) {
    loraLoop();
  } else {
    server.handleClient();
  }

  delayMicroseconds(50);
}
