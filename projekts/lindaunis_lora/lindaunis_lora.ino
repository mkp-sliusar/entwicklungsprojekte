/*
 * =====================================================================
 *  Project: Heltec ESP32-S3 (LoRa V3) — Temperature Monitoring Node
 *  Variant A: AP mode always responsive + mandatory measurement each 10s
 * ---------------------------------------------------------------------
 *  Requirements implemented:
 *   - Mandatory measurement every cfg.meas_period_s (default 10s) in BOTH modes
 *   - AP mode: WiFi AP + WebServer + (optional OLED) must stay responsive
 *   - LNS mode: WiFi OFF
 *   - NO ESP32 deep sleep
 *   - LoRaWAN.join retry every 30s until joined (no spam)
 *   - TX every cfg.minutes (and/or via /api/send)
 *   - LoRaMAC/Radio "pump" is done in a dedicated FreeRTOS task
 *     (LoRaWAN.sleep(CLASS_A) is kept there, does NOT block AP/web/measurements)
 * =====================================================================
 */

#include "LoRaWan_APP.h"
#ifdef CLASS
#undef CLASS
#endif
#ifdef DR
#undef DR
#endif
#ifdef LORAWAN_DEVEUI_AUTO
#undef LORAWAN_DEVEUI_AUTO
#endif
#define LORAWAN_DEVEUI_AUTO 0

#include "data/logoMKP.h"

#include <WiFi.h>
#include <WebServer.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <LittleFS.h>

#include "HT_SSD1306Wire.h"
#include <OneWire.h>
#include <DallasTemperature.h>
#include <math.h>

// ===== FreeRTOS (ESP32-S3) =====
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"

// ========================== Firmware version ======================= //
static constexpr const char* FW_VER = "1.8.3A-FRTOS";

// ========================== LoRaWAN key slots ======================= //
uint8_t devEui[8] = {0}, appEui[8] = {0};   // Heltec expects globals
uint8_t appKey[16] = {0};
uint8_t DevEui[8] = {0}, AppEui[8] = {0}, AppKey[16] = {0};

// =========================== HW constants ========================== //
static constexpr uint8_t OLED_ADDR = 0x3C;

static constexpr int PIN_ONE_WIRE = 4;
static constexpr int PIN_VBAT_ADC = 1;
static constexpr int PIN_ADC_CTRL = 37;
static constexpr int PIN_MODE_OUT = 45;
static constexpr int PIN_MODE_IN  = 46;

static constexpr int PIN_SENS_EN = 7;
static constexpr bool SENS_ACTIVE_HIGH = true;

static constexpr uint8_t DS_RES_BITS = 10;

static constexpr float VBAT_ADC_FACTOR = 1.0f / 4096.0f / 0.210282997855762f;

// ============================ System cfg =========================== //
static constexpr char AP_PSK[] = "12345678";
static constexpr size_t JSON_STATE_DOC = 2600;
static constexpr size_t JSON_CFG_DOC   = 1100;

static constexpr uint32_t MS_PER_SEC = 1000UL;
static constexpr uint8_t  UPLINK_PORT = 2;

static inline uint32_t now_sec() { return millis() / MS_PER_SEC; }

// ====================== Measurement / Aggregation ================== //
static constexpr uint16_t MEAS_PERIOD_MIN_S = 1;
static constexpr uint16_t MEAS_PERIOD_MAX_S = 3600;

static constexpr uint16_t MEAS_BUF_MAX = 600;
static constexpr uint32_t MINUTES_MAX  = 100;

static int16_t  t_buf[MEAS_BUF_MAX];
static uint16_t t_count = 0;
static uint16_t t_wr    = 0;
static uint16_t window_samples = 60;
static int16_t  t_last_c_x100  = INT16_MIN;

static uint32_t last_meas_ms    = 0;
static uint32_t window_start_ms = 0;

// Web log
static constexpr uint16_t WEB_LOG_N = 300;
static int16_t  web_t[WEB_LOG_N];
static uint32_t web_ts_s[WEB_LOG_N];
static uint16_t web_wr  = 0;
static uint16_t web_cnt = 0;

// ========================== Peripherals ============================ //
SSD1306Wire OLED_Display(OLED_ADDR, 400000, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);

OneWire oneWire(PIN_ONE_WIRE);
DallasTemperature sensors(&oneWire);
uint8_t firstDs[8] = {0};
bool firstDsFound = false;

bool apMode = false;
Preferences prefs;

// ========================= Persisted config ======================== //
struct Cfg {
  uint8_t devEui[8]  = {0};
  uint8_t appEui[8]  = {0};
  uint8_t appKey[16] = {0};

  uint32_t minutes = 10;
  uint16_t meas_period_s = 10;

  uint8_t lora_dr = 3;
  bool lora_adr = true; // UI only; policy forces ADR off in TX
} cfg;

// ============================ LoRa globals ========================== //
uint16_t userChannelsMask[6] = { 0x00FF, 0, 0, 0, 0, 0 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t   loraWanClass  = CLASS_A;

uint32_t appTxDutyCycle = 60000; // ms (required by Heltec core)

bool overTheAirActivation = true;
bool loraWanAdr = true;

bool isTxConfirmed = false;
uint8_t appPort = UPLINK_PORT;
uint8_t confirmedNbTrials = 1;

uint8_t nwkSKey[16] = {0};
uint8_t appSKey[16] = {0};
uint32_t devAddr = 0;

// ========================= Scheduling flags ======================== //
static volatile bool loraInited    = false;
static volatile bool txPending     = false;
static volatile bool forceTxOnce   = false; // /api/send
static uint32_t lastJoinTryMs = 0;

// ========================= Concurrency ============================= //
static SemaphoreHandle_t gMux = nullptr;
static TaskHandle_t measTaskH = nullptr;
static TaskHandle_t loraTaskH = nullptr;

// ========================= Helpers ================================= //
static inline void nvsPut(const char* k, const void* p, size_t n) {
  prefs.begin("uhfb", false);
  prefs.putBytes(k, p, n);
  prefs.end();
}
// === ADD this OLED section (you already have OLED_Display + logoMKP.h) ===

enum OledPhase { OLED_LOGO, OLED_SSID, OLED_SENS };
static OledPhase oledPhase = OLED_LOGO;
static uint32_t oledPhaseStart = 0;
static uint32_t oledRefreshMs = 0;

static void oledSplash() {
  OLED_Display.init();
  OLED_Display.clear();
  OLED_Display.setColor(WHITE);
  const int x = (128 - logoMKP_width) / 2;
  OLED_Display.drawXbm(x, 0, logoMKP_width, logoMKP_height, (const uint8_t*)logoMKP_bits);
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_CENTER);
  OLED_Display.drawString(64, logoMKP_height + 2, "MARX KRONTAL PARTNER");
  OLED_Display.display();
  delay(900);
}

static void oledShowSSID(const String& ssid) {
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 0, "Access Point");
  OLED_Display.drawString(0, 16, "SSID: " + ssid);
  OLED_Display.drawString(0, 32, "IP: " + WiFi.softAPIP().toString());
  OLED_Display.display();
}

static void oledSensorsOnce() {
  int16_t t_avg=0, t_min=0, t_max=0;
  bool ok = false;
  int16_t t_cur = INT16_MIN;
  uint16_t bufN = 0, bufMax = 0;

  if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(80)) == pdTRUE) {
    ok = computeStats(t_avg, t_min, t_max);
    t_cur = t_last_c_x100;
    bufN = t_count;
    bufMax = window_samples;
    xSemaphoreGive(gMux);
  }

  sensorsPowerOn();
  uint16_t bat = readBattery_mV();
  sensorsPowerOff();

  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);

  int y = 0, dy = 10;

  if (t_cur != INT16_MIN) {
    OLED_Display.drawString(0, y, String("Cur ") + String((float)t_cur / 100.0f, 1) + " C");
  } else {
    OLED_Display.drawString(0, y, "Cur N/C");
  }
  y += dy;

  if (ok) {
    OLED_Display.drawString(0, y, String("Avg ") + String((float)t_avg / 100.0f, 1) + " C"); y += dy;
    OLED_Display.drawString(0, y, String("Min ") + String((float)t_min / 100.0f, 1) + " C"); y += dy;
    OLED_Display.drawString(0, y, String("Max ") + String((float)t_max / 100.0f, 1) + " C"); y += dy;
  } else {
    OLED_Display.drawString(0, y, "Avg -  Min -  Max -"); y += dy;
  }

  OLED_Display.drawString(0, y, String("Bat ") + String(bat) + " mV"); y += dy;
  OLED_Display.drawString(0, y, String("TX ") + String(cfg.minutes) + "m  Meas " + String(cfg.meas_period_s) + "s"); y += dy;
  OLED_Display.drawString(0, y, String("Buf ") + String(bufN) + "/" + String(bufMax));

  OLED_Display.display();
}

static inline bool parseHexStraight(String s, uint8_t* out, size_t n) {
  s.trim();
  s.replace(" ", ""); s.replace("\n", ""); s.replace("\r", ""); s.replace("\t", "");
  s.toUpperCase();
  if (s.length() != (int)(n * 2)) return false;

  auto hexNibble = [](char c)->int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
  };

  for (size_t i = 0; i < n; i++) {
    int hi = hexNibble(s[2*i]);
    int lo = hexNibble(s[2*i + 1]);
    if (hi < 0 || lo < 0) return false;
    out[i] = (uint8_t)((hi << 4) | lo);
  }
  return true;
}

static inline bool parseHexLSB(const String& s, uint8_t* out, size_t n) {
  return parseHexStraight(s, out, n);
}

static String toHex(const uint8_t* v, size_t n) {
  char b[3];
  String s; s.reserve(n * 2);
  for (size_t i = 0; i < n; i++) { sprintf(b, "%02X", v[i]); s += b; }
  return s;
}

static void devEuiFromChipMSB(uint8_t out[8]) {
  uint64_t mac = ESP.getEfuseMac();
  uint8_t m[6];
  for (int i = 0; i < 6; i++) m[i] = (mac >> (8 * (5 - i))) & 0xFF;
  out[0]=m[0]; out[1]=m[1]; out[2]=m[2];
  out[3]=0xFF; out[4]=0xFE;
  out[5]=m[3]; out[6]=m[4]; out[7]=m[5];
}

static String devEuiSuffix6LSB() {
  char buf[7];
  sprintf(buf, "%02X%02X%02X", cfg.devEui[5], cfg.devEui[6], cfg.devEui[7]);
  return String(buf);
}

// ===================== Power + sensor management =================== //
static inline void SensorsON() {
  pinMode(PIN_SENS_EN, OUTPUT);
  digitalWrite(PIN_SENS_EN, SENS_ACTIVE_HIGH ? HIGH : LOW);
}
static inline void SensorsOFF() {
  digitalWrite(PIN_SENS_EN, SENS_ACTIVE_HIGH ? LOW : HIGH);
  pinMode(PIN_SENS_EN, INPUT);
}

static void sensorsPowerOn() {
  SensorsON();
  delay(12);

  if (!firstDsFound) {
    sensors.begin();
    firstDsFound = sensors.getAddress(firstDs, 0);
    if (!firstDsFound) {
      OneWire ow(PIN_ONE_WIRE);
      uint8_t a[8];
      ow.reset_search();
      while (ow.search(a)) {
        if (a[0] == 0x28) { memcpy(firstDs, a, 8); firstDsFound = true; break; }
      }
    }
  }
  if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
}

static void sensorsPowerOff() {
  pinMode(PIN_ONE_WIRE, INPUT);
  SensorsOFF();
}

static uint16_t readBattery_mV() {
  analogSetAttenuation(ADC_0db);
  pinMode(PIN_ADC_CTRL, OUTPUT);
  digitalWrite(PIN_ADC_CTRL, HIGH);
  delay(2);
  int raw = analogRead(PIN_VBAT_ADC);
  digitalWrite(PIN_ADC_CTRL, LOW);
  return (uint16_t)(VBAT_ADC_FACTOR * raw * 1000.0f);
}

static int16_t readTemp_c_x100_signed() {
  if (!firstDsFound) return INT16_MIN;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200);
  float t = sensors.getTempC(firstDs);
  if (!isfinite(t) || t < -55.0f || t > 125.0f) return INT16_MIN;
  long v = lroundf(t * 100.0f);
  if (v < -32768) v = -32768;
  if (v >  32767) v =  32767;
  return (int16_t)v;
}

// ====================== Window + Web log helpers =================== //
static uint16_t samplesPerWindowFromCfg() {
  uint32_t m = cfg.minutes;
  if (m < 1) m = 1;
  if (m > MINUTES_MAX) m = MINUTES_MAX;

  uint32_t s = cfg.meas_period_s;
  if (s < MEAS_PERIOD_MIN_S) s = MEAS_PERIOD_MIN_S;
  if (s > MEAS_PERIOD_MAX_S) s = MEAS_PERIOD_MAX_S;

  uint32_t n = (m * 60UL) / s;
  if (n < 1) n = 1;
  if (n > MEAS_BUF_MAX) n = MEAS_BUF_MAX;
  return (uint16_t)n;
}

static void resetMeasWindow() {
  window_start_ms = millis();
  last_meas_ms = 0;
  t_count = 0;
  t_wr = 0;
  window_samples = samplesPerWindowFromCfg();
}

static void webLogSample(int16_t t_c_x100) {
  if (t_c_x100 == INT16_MIN) return;
  web_t[web_wr] = t_c_x100;
  web_ts_s[web_wr] = now_sec();
  web_wr = (web_wr + 1) % WEB_LOG_N;
  if (web_cnt < WEB_LOG_N) web_cnt++;
}

static void addTempSample(int16_t t_c_x100) {
  if (t_c_x100 == INT16_MIN) return;
  t_last_c_x100 = t_c_x100;

  t_buf[t_wr] = t_c_x100;
  t_wr++;
  if (t_wr >= window_samples) t_wr = 0;
  if (t_count < window_samples) t_count++;

  webLogSample(t_c_x100);
}

static bool computeStats(int16_t& t_avg, int16_t& t_min, int16_t& t_max) {
  if (t_count == 0) return false;

  int32_t sum = 0;
  t_min =  32767;
  t_max = -32768;

  int32_t idx = (int32_t)t_wr - (int32_t)t_count;
  while (idx < 0) idx += window_samples;

  for (uint16_t i = 0; i < t_count; i++) {
    int16_t v = t_buf[idx];
    sum += v;
    if (v < t_min) t_min = v;
    if (v > t_max) t_max = v;
    idx++;
    if (idx >= window_samples) idx = 0;
  }

  t_avg = (int16_t)lroundf((float)sum / (float)t_count);
  return true;
}

// ============================ LoRa helpers ========================== //
static inline uint8_t mapDR(uint8_t i) {
  switch (i) {
    case 0: return DR_0; case 1: return DR_1; case 2: return DR_2;
    case 3: return DR_3; case 4: return DR_4; case 5: return DR_5;
    default: return DR_3;
  }
}

static inline void applyLoraDR() { LoRaWAN.setDefaultDR(mapDR(cfg.lora_dr)); }

static void enforceTxPolicy() {
  isTxConfirmed = true;
  confirmedNbTrials = 3;
  loraWanAdr = false;
}

static bool isJoined() {
  MibRequestConfirm_t mibReq;
  mibReq.Type = MIB_NETWORK_JOINED;
  LoRaMacMibGetRequestConfirm(&mibReq);
  return mibReq.Param.IsNetworkJoined;
}

// ========================= Uplink payload (10B) ===================== //
void prepareTxFrame(uint8_t) {
  const uint16_t vb = readBattery_mV();

  int16_t t_avg=0, t_min=0, t_max=0;
  bool ok = computeStats(t_avg, t_min, t_max);

  int16_t t_cur = 0;
  if (t_count > 0 && t_last_c_x100 != INT16_MIN) t_cur = t_last_c_x100;
  if (!ok) t_avg = t_min = t_max = t_cur;

  appDataSize = 10;

  auto putI16 = [&](uint8_t idx, int16_t v) {
    uint16_t u = (uint16_t)v;
    appData[idx] = (uint8_t)(u >> 8);
    appData[idx + 1] = (uint8_t)(u);
  };
  auto putU16 = [&](uint8_t idx, uint16_t u) {
    appData[idx] = (uint8_t)(u >> 8);
    appData[idx + 1] = (uint8_t)(u);
  };

  putI16(0, t_cur);
  putI16(2, t_avg);
  putI16(4, t_min);
  putI16(6, t_max);
  putU16(8, vb);

  Serial.printf("TX 10B: Tcur=%d Tavg=%d Tmin=%d Tmax=%d VB=%u HEX=%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X\n",
    (int)t_cur, (int)t_avg, (int)t_min, (int)t_max, (unsigned)vb,
    appData[0], appData[1], appData[2], appData[3], appData[4], appData[5], appData[6], appData[7], appData[8], appData[9]);
}

// ============================== Web API ============================ //
WebServer http(80);

static inline String ipStr() {
  if (WiFi.getMode() == WIFI_AP) return WiFi.softAPIP().toString();
  if (WiFi.status() == WL_CONNECTED) return WiFi.localIP().toString();
  return "-";
}

static inline String contentTypeFor(const String& p) {
  if (p.endsWith(".html")) return "text/html; charset=utf-8";
  if (p.endsWith(".svg"))  return "image/svg+xml";
  if (p.endsWith(".json")) return "application/json; charset=utf-8";
  if (p.endsWith(".css"))  return "text/css; charset=utf-8";
  if (p.endsWith(".js"))   return "application/javascript; charset=utf-8";
  return "application/octet-stream";
}

static bool streamFileFS(const char* p) {
  if (!LittleFS.exists(p)) return false;
  File f = LittleFS.open(p, "r");
  if (!f) return false;
  http.streamFile(f, contentTypeFor(p));
  f.close();
  return true;
}

static void api_state() {
  StaticJsonDocument<JSON_STATE_DOC> d;

  d["mode"] = (WiFi.getMode() == WIFI_AP) ? "AP" : "LNS";
  d["ip"] = ipStr();
  d["uptime_s"] = (uint32_t)(millis() / MS_PER_SEC);
  d["heap_free"] = ESP.getFreeHeap();
  d["fw"] = FW_VER;
  d["fw_build"] = String(__DATE__) + " " + String(__TIME__);

  d["joined"] = isJoined();

  int16_t t_avg=0, t_min=0, t_max=0;
  bool ok = false;

  if (xSemaphoreTake(gMux, pdMS_TO_TICKS(50)) == pdTRUE) {
    ok = computeStats(t_avg, t_min, t_max);
    if (t_count > 0 && t_last_c_x100 != INT16_MIN) d["t_cur_c"] = (double)t_last_c_x100 / 100.0;
    else d["t_cur_c"] = nullptr;

    if (ok) {
      d["t_avg_c"] = (double)t_avg / 100.0;
      d["t_min_c"] = (double)t_min / 100.0;
      d["t_max_c"] = (double)t_max / 100.0;
    } else {
      d["t_avg_c"] = nullptr;
      d["t_min_c"] = nullptr;
      d["t_max_c"] = nullptr;
    }

    d["tx_period_min"] = cfg.minutes;
    d["meas_period_s"] = cfg.meas_period_s;
    d["window_samples"] = window_samples;
    d["window_count"] = t_count;

    const uint32_t reportPeriodMs = max<uint32_t>(60000UL, 60000UL * (uint32_t)cfg.minutes);
    const uint32_t elapsed = millis() - window_start_ms;
    d["next_tx_s"] = (elapsed >= reportPeriodMs) ? 0 : (uint32_t)((reportPeriodMs - elapsed) / 1000UL);

    JsonArray log = d.createNestedArray("log");
    uint32_t nowS = now_sec();
    for (uint16_t i = 0; i < web_cnt; i++) {
      int idx = (int)web_wr - 1 - (int)i;
      while (idx < 0) idx += WEB_LOG_N;
      JsonObject row = log.createNestedObject();
      row["age_s"] = (nowS >= web_ts_s[idx]) ? (nowS - web_ts_s[idx]) : 0;
      row["t_c"] = (double)web_t[idx] / 100.0;
    }

    JsonObject c = d.createNestedObject("cfg");
    c["minutes"] = cfg.minutes;
    c["meas_period_s"] = cfg.meas_period_s;
    c["devEui_lsb"] = toHex(cfg.devEui, 8);
    c["appEui_lsb"] = toHex(cfg.appEui, 8);
    c["appKey_msb"] = toHex(cfg.appKey, 16);
    c["dr"] = cfg.lora_dr;
    c["adr"] = cfg.lora_adr;

    xSemaphoreGive(gMux);
  } else {
    d["t_cur_c"] = nullptr;
    d["t_avg_c"] = nullptr;
    d["t_min_c"] = nullptr;
    d["t_max_c"] = nullptr;
  }

  // battery read (kept as in your code)
  sensorsPowerOn();
  uint16_t batmV = readBattery_mV();
  sensorsPowerOff();
  d["batt_mV"] = batmV;

  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}

static void api_send() {
  if (!isJoined()) { http.send(409, "text/plain", "not joined"); return; }
  forceTxOnce = true;
  http.send(200, "text/plain", "queued");
}

static void api_reset() {
  http.send(200, "text/plain", "restarting");
  delay(150);
  ESP.restart();
}

static void api_cfg_get() {
  StaticJsonDocument<JSON_CFG_DOC> d;
  d["devEui_lsb"] = toHex(cfg.devEui, 8);
  d["appEui_lsb"] = toHex(cfg.appEui, 8);
  d["appKey_msb"] = toHex(cfg.appKey, 16);
  d["minutes"] = cfg.minutes;
  d["meas_period_s"] = cfg.meas_period_s;
  d["dr"] = cfg.lora_dr;
  d["adr"] = cfg.lora_adr;
  String out; serializeJson(d, out);
  http.send(200, "application/json", out);
}

static void api_cfg_lora() {
  if (!http.hasArg("plain")) { http.send(400, "text/plain", "bad json"); return; }
  StaticJsonDocument<900> d;
  if (deserializeJson(d, http.arg("plain"))) { http.send(400, "text/plain", "bad json"); return; }

  bool okHex = true;
  bool needRestart = false;
  bool needResetWindow = false;

  if (d.containsKey("devEui_lsb")) { okHex &= parseHexLSB((const char*)d["devEui_lsb"], cfg.devEui, 8); needRestart = true; }
  if (d.containsKey("appEui_lsb")) { okHex &= parseHexLSB((const char*)d["appEui_lsb"], cfg.appEui, 8); needRestart = true; }
  if (d.containsKey("appKey_msb")) { okHex &= parseHexLSB((const char*)d["appKey_msb"], cfg.appKey, 16); needRestart = true; }

  if (d.containsKey("minutes")) {
    uint32_t newM = d["minutes"].as<uint32_t>();
    if (newM < 1 || newM > MINUTES_MAX) { http.send(400, "text/plain", "minutes range"); return; }
    if (newM != cfg.minutes) {
      cfg.minutes = newM;
      prefs.begin("uhfb", false); prefs.putUInt("minutes", cfg.minutes); prefs.end();
      needResetWindow = true;
      appTxDutyCycle = 60000UL * (uint32_t)cfg.minutes;
      if (appTxDutyCycle < 60000UL) appTxDutyCycle = 60000UL;
    }
  }

  if (d.containsKey("meas_period_s")) {
    int s = d["meas_period_s"].as<int>();
    if (s < (int)MEAS_PERIOD_MIN_S || s > (int)MEAS_PERIOD_MAX_S) { http.send(400, "text/plain", "meas_period_s range"); return; }
    if ((uint16_t)s != cfg.meas_period_s) {
      cfg.meas_period_s = (uint16_t)s;
      prefs.begin("uhfb", false); prefs.putUShort("meas_s", cfg.meas_period_s); prefs.end();
      needResetWindow = true;
    }
  }

  if (d.containsKey("dr")) {
    int v = d["dr"].as<int>();
    if (v < 0 || v > 5) { http.send(400, "text/plain", "dr range"); return; }
    cfg.lora_dr = (uint8_t)v;
    prefs.begin("uhfb", false); prefs.putUChar("lora_dr", cfg.lora_dr); prefs.end();
    needRestart = true;
  }

  if (d.containsKey("adr")) {
    cfg.lora_adr = d["adr"].as<bool>();
    prefs.begin("uhfb", false); prefs.putBool("lora_adr", cfg.lora_adr); prefs.end();
  }

  if (!okHex) { http.send(400, "text/plain", "hex error"); return; }

  nvsPut("devEui", cfg.devEui, 8);
  nvsPut("appEui", cfg.appEui, 8);
  nvsPut("appKey", cfg.appKey, 16);

  http.send(200, "text/plain", "ok");

  if (needResetWindow) {
    if (xSemaphoreTake(gMux, pdMS_TO_TICKS(100)) == pdTRUE) {
      resetMeasWindow();
      xSemaphoreGive(gMux);
    } else {
      resetMeasWindow();
    }
  }

  if (needRestart) { delay(150); ESP.restart(); }
}

static void attachHttpFS() {
  http.on("/", [](){ if (!streamFileFS("/index.html")) http.send(404, "text/plain", "index missing"); });
  http.on("/app.js", [](){ if (!streamFileFS("/app.js")) http.send(404, "text/plain", "app.js missing"); });
  http.on("/style.css", [](){ if (!streamFileFS("/style.css")) http.send(404, "text/plain", "style.css missing"); });
  http.on("/logo.svg", [](){ if (!streamFileFS("/logo.svg")) http.send(404, "text/plain", "logo missing"); });
  http.on("/i18n.json", [](){ if (!streamFileFS("/i18n.json")) http.send(404, "text/plain", "i18n missing"); });

  http.on("/api/state", api_state);
  http.on("/api/send", HTTP_POST, api_send);
  http.on("/api/reset", HTTP_POST, api_reset);
  http.on("/api/config", api_cfg_get);
  http.on("/api/config/lora", HTTP_POST, api_cfg_lora);

  http.onNotFound([](){ if (!streamFileFS("/index.html")) http.send(404, "text/plain", "404"); });
  http.begin();
}

// ========================= NVS load config ========================= //
static void loadCfg() {
  prefs.begin("uhfb", true);
  prefs.getBytes("devEui", cfg.devEui, 8);
  prefs.getBytes("appEui", cfg.appEui, 8);
  prefs.getBytes("appKey", cfg.appKey, 16);
  cfg.minutes = prefs.getUInt("minutes", cfg.minutes);
  cfg.meas_period_s = prefs.getUShort("meas_s", cfg.meas_period_s);
  cfg.lora_dr = prefs.getUChar("lora_dr", cfg.lora_dr);
  cfg.lora_adr = prefs.getBool("lora_adr", cfg.lora_adr);
  prefs.end();

  if (cfg.minutes < 1) cfg.minutes = 1;
  if (cfg.minutes > MINUTES_MAX) cfg.minutes = MINUTES_MAX;
  if (cfg.meas_period_s < MEAS_PERIOD_MIN_S) cfg.meas_period_s = MEAS_PERIOD_MIN_S;
  if (cfg.meas_period_s > MEAS_PERIOD_MAX_S) cfg.meas_period_s = MEAS_PERIOD_MAX_S;
}

// ========================= Mandatory measurement task ============== //
static void measTask(void*) {
  for (;;) {
    uint32_t periodMs = (uint32_t)cfg.meas_period_s * 1000UL;
    if (periodMs < 1000UL) periodMs = 1000UL;

    sensorsPowerOn();
    int16_t t = readTemp_c_x100_signed();
    sensorsPowerOff();

    if (xSemaphoreTake(gMux, pdMS_TO_TICKS(100)) == pdTRUE) {
      addTempSample(t);
      xSemaphoreGive(gMux);
    }

    vTaskDelay(pdMS_TO_TICKS(periodMs));
  }
}

// ========================= LoRa task (join/tx + pump) ============== //
static void loraInitOnce() {
  if (loraInited) return;

  memcpy(devEui, cfg.devEui, 8);
  memcpy(appEui, cfg.appEui, 8);
  memcpy(appKey, cfg.appKey, 16);
  memcpy(DevEui, devEui, 8);
  memcpy(AppEui, appEui, 8);
  memcpy(AppKey, appKey, 16);

  loraWanAdr = cfg.lora_adr;
  LoRaWAN.init(CLASS_A, ACTIVE_REGION);
  applyLoraDR();
  enforceTxPolicy();

  Serial.println("[LoRa] join()");
  LoRaWAN.join();
  lastJoinTryMs = millis();

  loraInited = true;
}

static void loraTask(void*) {
  for (;;) {
    loraInitOnce();

    const uint32_t now = millis();

    if (!isJoined()) {
      if (now - lastJoinTryMs >= 30000UL) {
        Serial.println("[LoRa] join retry");
        LoRaWAN.join();
        enforceTxPolicy();
        lastJoinTryMs = now;
      }
    } else {
      // periodic TX trigger (minutes window) OR forced TX
      uint32_t reportPeriodMs = 60000UL * (uint32_t)cfg.minutes;
      if (reportPeriodMs < 60000UL) reportPeriodMs = 60000UL;

      bool due = false;
      if ((now - window_start_ms) >= reportPeriodMs) due = true;
      if (forceTxOnce) due = true;

      if (due) {
        enforceTxPolicy();

        if (xSemaphoreTake(gMux, pdMS_TO_TICKS(200)) == pdTRUE) {
          prepareTxFrame(UPLINK_PORT);
          xSemaphoreGive(gMux);
        } else {
          prepareTxFrame(UPLINK_PORT);
        }

        LoRaWAN.send();

        if (xSemaphoreTake(gMux, pdMS_TO_TICKS(200)) == pdTRUE) {
          resetMeasWindow();
          xSemaphoreGive(gMux);
        } else {
          resetMeasWindow();
        }

        forceTxOnce = false;
      }
    }

    // pump LoRaMAC/Radio; does NOT stop WiFi task/measurements because we are in a separate task
    LoRaWAN.sleep(CLASS_A);

    // avoid 100% CPU
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// ============================== Setup/Loop ========================= //
static constexpr char AP_SSID_PREFIX[] = "Riss-";

void setup() {
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  gMux = xSemaphoreCreateMutex();

  pinMode(PIN_MODE_OUT, OUTPUT);
  digitalWrite(PIN_MODE_OUT, LOW);
  pinMode(PIN_MODE_IN, INPUT_PULLUP);
  delay(5);

  Serial.printf("FW %s build %s %s\n", FW_VER, __DATE__, __TIME__);

  for (int i = 0; i < 5; i++) { apMode |= (digitalRead(PIN_MODE_IN) == LOW); delay(2); }

  analogReadResolution(12);
  loadCfg();

  appTxDutyCycle = 60000UL * (uint32_t)cfg.minutes;
  if (appTxDutyCycle < 60000UL) appTxDutyCycle = 60000UL;

  resetMeasWindow();

  if (apMode) {
    // Power for OLED (Heltec): enable Vext FIRST
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, LOW);   // Vext ON

    // WiFi AP + FS + HTTP
    WiFi.mode(WIFI_AP);
    WiFi.setTxPower(WIFI_POWER_7dBm);

    bool fsOk = LittleFS.begin(true);
    if (fsOk) attachHttpFS();
    else Serial.println("[FS] mount failed");

    String ssid = String(AP_SSID_PREFIX) + devEuiSuffix6LSB();
    WiFi.softAP(ssid.c_str(), AP_PSK);

    // OLED init AFTER Vext ON, and AFTER AP is up (so IP is valid)
    oledSplash();
    oledPhase = OLED_SSID;
    oledPhaseStart = millis();
    oledShowSSID(ssid);

  } else {
    WiFi.persistent(false);
    WiFi.disconnect(true, true);
    WiFi.mode(WIFI_OFF);
  #if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
    btStop();
  #endif
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, HIGH); // Vext OFF
  }


  // start tasks
  xTaskCreatePinnedToCore(measTask, "meas", 4096, nullptr, 2, &measTaskH, 1);
  xTaskCreatePinnedToCore(loraTask, "lora", 6144, nullptr, 1, &loraTaskH, 0);

  // Note: AP/web remains in loop() (non-blocking); could be moved to a task if needed
}

// === REPLACE loop() with this (keeps AP web + OLED refresh non-blocking) ===

void loop() {
  if (apMode) {
    http.handleClient();

    const uint32_t now = millis();

    // after SSID screen -> sensor screen
    if (oledPhase == OLED_SSID && (now - oledPhaseStart) >= 1500UL) {
      oledPhase = OLED_SENS;
      oledRefreshMs = 0;
      oledSensorsOnce();
    }

    // refresh sensors screen ~1 Hz
    if (oledPhase == OLED_SENS && (now - oledRefreshMs) >= 1000UL) {
      oledRefreshMs = now;
      oledSensorsOnce();
    }
  }

  delay(5);
}

