/*
 * =====================================================================
 *  Project: Heltec ESP32-S3 (LoRa V3) — Temperature + ADS1115 Crack Node
 *  Variant: AP mode (Web UI + OLED) like old + LoRaWAN in field
 * ---------------------------------------------------------------------
 *  NEW LOGIC REQUIREMENT:
 *    - Measure strictly every 10 seconds
 *    - Keep Cur/Avg/Min/Max for Temp (int16 x100) and Crack (uint16 mm*1000)
 *    - Payload 18 bytes (unchanged layout)
 *    - Field mode: MAX power save using LIGHT SLEEP (no deep sleep)
 *      * Measurement every 10 seconds (strict)
 *      * Uplink every cfg.minutes (default 10 minutes)
 *
 *  AP mode stays like old (Web UI + OLED live), light sleep is NOT used there.
 *
 * ---------------------------------------------------------------------
 *  OPTIMIZATIONS APPLIED (ADS warmup kept as-is):
 *    1) Field mode uplink battery read no longer powers sensors/ADS
 *       (battery read is independent and saves power/time)
 *    2) Strict 10s scheduler improved with "catch-up" to avoid rapid wake loops
 *       after long TX (no change in 10s grid / no drift accumulation)
 *    3) Removed unnecessary delay(20) after LoRaWAN.send()
 *    4) DS18B20 conversion wait tightened (10-bit) and uses non-blocking mode
 *       to avoid extra waiting; still robust
 *    5) Sleep power domains: RTC_PERIPH OFF (safe for timer wakeup)
 *    6) Reduced serial chatter in field mode (kept key prints only)
 * =====================================================================
 */

#ifdef LORA_MAC_PRIVATE_SYNCWORD
#undef LORA_MAC_PRIVATE_SYNCWORD
#endif
#ifdef LORA_MAC_PUBLIC_SYNCWORD
#undef LORA_MAC_PUBLIC_SYNCWORD
#endif
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

#include <Wire.h>
#include <Adafruit_ADS1X15.h>

#include "HT_SSD1306Wire.h"
#include <OneWire.h>
#include <DallasTemperature.h>
#include <math.h>

// ===== FreeRTOS (ESP32-S3) =====
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"

// ===== Light sleep + PM =====
#include "esp_sleep.h"
#include "esp_timer.h"
#include "esp_pm.h"

// ========================== Firmware version ======================= //
static constexpr const char* FW_VER = "2.0.0-AP+OLED+WEB-10sMEAS-18B-LS-OPT1";

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

// Tightened DS conversion wait (10-bit ~187.5ms typical). Keep margin.
static constexpr uint16_t DS_WAIT_MS = 190;

static constexpr float VBAT_ADC_FACTOR = 1.0f / 4096.0f / 0.210282997855762f;

// ADS1115 (I2C1)
static constexpr int I2C_ADS_SDA = 47;
static constexpr int I2C_ADS_SCL = 48;
static constexpr uint32_t I2C_FREQ_HZ = 400000;
static constexpr uint8_t ADS1115_ADDR = 0x48;

TwoWire I2C_ADS = TwoWire(1);
Adafruit_ADS1115 ads;
static bool adsOk = false;

// ============================ System cfg =========================== //
static constexpr char AP_PSK[] = "12345678";
static constexpr size_t JSON_STATE_DOC = 4700;
static constexpr size_t JSON_CFG_DOC   = 1900;

static constexpr uint32_t MS_PER_SEC = 1000UL;
static constexpr uint8_t  UPLINK_PORT = 2;

static inline uint32_t now_sec() { return millis() / MS_PER_SEC; }

// ====================== Measurement / Aggregation ================== //
static constexpr uint16_t MEAS_PERIOD_FIXED_S = 10;   // FIXED 10s
static constexpr uint16_t MEAS_BUF_MAX = 600;
static constexpr uint32_t MINUTES_MAX  = 100;

// Temperature buffers
static int16_t  t_buf[MEAS_BUF_MAX];
static uint16_t t_count = 0;
static uint16_t t_wr    = 0;
static uint16_t window_samples = 60;
static int16_t  t_last_c_x100  = INT16_MIN;

// Crack buffers (mm*1000, uint16)
static uint16_t c_buf[MEAS_BUF_MAX];
static uint16_t c_count = 0;
static uint16_t c_wr    = 0;
static uint16_t c_last  = 0xFFFF;     // sentinel invalid
static bool     c_last_valid = false;

static uint32_t window_start_ms = 0;

// Web log
static constexpr uint16_t WEB_LOG_N = 300;
static int16_t  web_t[WEB_LOG_N];
static uint16_t web_c[WEB_LOG_N];
static bool     web_c_ok[WEB_LOG_N];
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

// ========================= Crack mapping (old logic) =============== //
enum RefMode : uint8_t { REF_AUTO = 0, REF_MANUAL = 1 };
static constexpr int16_t RAW_MIN_STABLE = 6;

struct CrackMeas {
  int16_t raw0;
  int16_t raw1;
  float mm;
  bool auto_used;
};
static CrackMeas readCrackOnce(); // ensure visible

// ========================= Persisted config ======================== //
struct Cfg {
  uint8_t devEui[8]  = {0};
  uint8_t appEui[8]  = {0};
  uint8_t appKey[16] = {0};

  uint32_t minutes = 10;     // uplink period (minutes)
  // kept for backward compat only (fixed runtime)
  uint16_t meas_period_s = MEAS_PERIOD_FIXED_S;

  // ---- crack calibration ----
  int16_t  crack_r0 = 5;
  int16_t  crack_r1 = 25480;
  uint16_t crack_mm0_x100 = 0;        // 0.01 mm
  uint16_t crack_mm1_x100 = 1000;     // 10.00 mm default
  uint16_t crack_len_mm_x100 = 1000;  // L_full in 0.01 mm
  uint8_t  ref_mode = REF_AUTO;

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
static volatile bool loraInited  = false;
static volatile bool forceTxOnce = false; // /api/send
static uint32_t lastJoinTryMs = 0;

// ========================= Concurrency ============================= //
// gMux: protects cfg + measurement buffers + window_start_ms + web log
// gSensMux: protects sensorsPowerOn/Off + DS/ADS reads and I2C ownership
static SemaphoreHandle_t gMux = nullptr;
static SemaphoreHandle_t gSensMux = nullptr;

// AP mode tasks (kept)
static TaskHandle_t measTaskH = nullptr;
static TaskHandle_t loraTaskH = nullptr;

// Field mode light sleep scheduler
static uint64_t nextMeasUs = 0;
static uint64_t lastJoinTryUs = 0;
static uint32_t measCounter = 0;

// ========================= Helpers ================================= //
static inline void nvsPut(const char* k, const void* p, size_t n) {
  prefs.begin("uhfb", false);
  prefs.putBytes(k, p, n);
  prefs.end();
}

// ============================== OLED =============================== //
enum OledPhase { OLED_LOGO, OLED_SSID, OLED_SENS };
static OledPhase oledPhase = OLED_LOGO;
static uint32_t oledPhaseStart = 0;
static uint32_t oledRefreshMs = 0;

// forward decls used by OLED / API
static bool computeStats(int16_t& t_avg, int16_t& t_min, int16_t& t_max);
static bool computeCrackStats(uint16_t& c_avg, uint16_t& c_min, uint16_t& c_max);

static void sensorsPowerOn();
static void sensorsPowerOff();
static uint16_t readBattery_mV();

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
  bool tok = false;
  int16_t t_cur = INT16_MIN;

  uint16_t c_avg=0, c_min=0, c_max=0;
  bool cok = false;
  uint16_t c_cur = 0xFFFF;
  bool cvalid = false;

  if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(80)) == pdTRUE) {
    tok = computeStats(t_avg, t_min, t_max);
    t_cur = t_last_c_x100;

    cok = computeCrackStats(c_avg, c_min, c_max);
    cvalid = c_last_valid;
    c_cur = c_last;

    xSemaphoreGive(gMux);
  }

  uint16_t bat = 0;
  if (gSensMux && xSemaphoreTake(gSensMux, pdMS_TO_TICKS(250)) == pdTRUE) {
    sensorsPowerOn();
    bat = readBattery_mV();
    sensorsPowerOff();
    xSemaphoreGive(gSensMux);
  }

  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);

  int y = 0, dy = 10;

  if (t_cur != INT16_MIN) OLED_Display.drawString(0, y, String("T Cur ") + String((float)t_cur/100.0f, 1) + "C");
  else OLED_Display.drawString(0, y, "T Cur N/C");
  y += dy;

  if (tok) OLED_Display.drawString(0, y, String("T Avg ") + String((float)t_avg/100.0f, 1) + "C");
  else OLED_Display.drawString(0, y, "T Avg -");
  y += dy;

  if (cvalid && c_cur != 0xFFFF) OLED_Display.drawString(0, y, String("Crk Cur ") + String((float)c_cur/1000.0f, 3) + "mm");
  else OLED_Display.drawString(0, y, "Crk Cur N/C");
  y += dy;

  if (cok) OLED_Display.drawString(0, y, String("Crk Avg ") + String((float)c_avg/1000.0f, 3) + "mm");
  else OLED_Display.drawString(0, y, "Crk Avg -");
  y += dy;

  OLED_Display.drawString(0, y, String("Bat ") + String(bat) + " mV"); y += dy;
  OLED_Display.drawString(0, y, String("TX ") + String(cfg.minutes) + "m  Meas 10s");

  OLED_Display.display();
}

// ============================ Hex helpers ========================== //
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
static inline bool parseHexLSB(const String& s, uint8_t* out, size_t n) { return parseHexStraight(s, out, n); }

static String toHex(const uint8_t* v, size_t n) {
  char b[3];
  String s; s.reserve(n * 2);
  for (size_t i = 0; i < n; i++) { sprintf(b, "%02X", v[i]); s += b; }
  return s;
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

// ADS init + read (KEEP AS-IS: ADS power-cycled, warmup affects data quality)
static void adsInitOnce() {
  ads.setGain(GAIN_ONE);
  ads.setDataRate(RATE_ADS1115_128SPS);
  (void)ads.readADC_SingleEnded(0);
  delay(4);
}
static inline int16_t readADS_CH(uint8_t ch) {
  if (!adsOk) return 0;
  (void)ads.readADC_SingleEnded(ch);   // pre-warm
  delayMicroseconds(8000);             // >=1 period @128SPS
  int16_t v = ads.readADC_SingleEnded(ch);
  return (v < 0) ? 0 : v;
}

static void sensorsPowerOn() {
  SensorsON();
  delay(40);

  I2C_ADS.begin(I2C_ADS_SDA, I2C_ADS_SCL, I2C_FREQ_HZ);
  adsOk = ads.begin(ADS1115_ADDR, &I2C_ADS);
  if (adsOk) adsInitOnce();

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
  if (firstDsFound) {
    sensors.setResolution(firstDs, DS_RES_BITS);
    sensors.setWaitForConversion(false); // optimization: tighter waits in readTemp()
  }
}

static void sensorsPowerOff() {
  I2C_ADS.end();
  pinMode(I2C_ADS_SDA, INPUT);
  pinMode(I2C_ADS_SCL, INPUT);

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

  // non-blocking conversion, but we still wait DS_WAIT_MS; no extra over-wait
  sensors.requestTemperaturesByAddress(firstDs);
  delay(DS_WAIT_MS);

  float t = sensors.getTempC(firstDs);
  if (!isfinite(t) || t < -55.0f || t > 125.0f) return INT16_MIN;
  long v = lroundf(t * 100.0f);
  if (v < -32768) v = -32768;
  if (v >  32767) v =  32767;
  return (int16_t)v;
}

// ====================== Crack mapping helpers ====================== //
static inline float mapCrackManual(int M) {
  const float M0  = (float)cfg.crack_r0;
  const float M1  = (float)cfg.crack_r1;
  const float mm0 = (float)cfg.crack_mm0_x100 / 100.0f;
  const float mm1 = (float)cfg.crack_mm1_x100 / 100.0f;
  if (M1 == M0) return 0.0f;

  float mm = mm0 + (mm1 - mm0) * ((float)M - M0) / (M1 - M0);
  if (mm < 0) mm = 0;
  const float L = (float)cfg.crack_len_mm_x100 / 100.0f;
  if (mm > L) mm = L;
  return mm;
}

static CrackMeas readCrackOnce() {
  CrackMeas r{0,0,0.0f,false};
  if (!adsOk) return r;

  const int16_t ch0 = readADS_CH(0);
  const int16_t ch1 = readADS_CH(1);
  r.raw0 = ch0;
  r.raw1 = ch1;

  const float L = (float)cfg.crack_len_mm_x100 / 100.0f;

  if (cfg.ref_mode == REF_MANUAL) {
    r.mm = mapCrackManual(ch0);
    r.auto_used = false;
  } else {
    int16_t rf = ch1;
    if (rf < RAW_MIN_STABLE) rf = RAW_MIN_STABLE;
    float mm = L * ((float)ch0 / (float)rf);
    if (mm < 0) mm = 0;
    if (mm > L) mm = L;
    r.mm = mm;
    r.auto_used = true;
  }
  return r;
}

// ====================== Window + Web log helpers =================== //
static uint16_t samplesPerWindowFromCfg_locked() {
  uint32_t m = cfg.minutes;
  if (m < 1) m = 1;
  if (m > MINUTES_MAX) m = MINUTES_MAX;

  const uint32_t s = MEAS_PERIOD_FIXED_S;

  uint32_t n = (m * 60UL) / s;
  if (n < 1) n = 1;
  if (n > MEAS_BUF_MAX) n = MEAS_BUF_MAX;
  return (uint16_t)n;
}

static void resetMeasWindow_locked() {
  window_start_ms = millis();

  t_count = 0;
  t_wr = 0;
  t_last_c_x100 = INT16_MIN;

  c_count = 0;
  c_wr = 0;
  c_last = 0xFFFF;
  c_last_valid = false;

  window_samples = samplesPerWindowFromCfg_locked();
}

static void webLogSample2_locked(int16_t t_c_x100, bool cr_ok, uint16_t cr_x1000) {
  web_t[web_wr] = t_c_x100;
  web_c_ok[web_wr] = cr_ok;
  web_c[web_wr] = cr_ok ? cr_x1000 : 0;
  web_ts_s[web_wr] = now_sec();

  web_wr = (web_wr + 1) % WEB_LOG_N;
  if (web_cnt < WEB_LOG_N) web_cnt++;
}

static void addTempSample_locked(int16_t t_c_x100) {
  if (t_c_x100 == INT16_MIN) return;
  t_last_c_x100 = t_c_x100;

  t_buf[t_wr] = t_c_x100;
  t_wr++;
  if (t_wr >= window_samples) t_wr = 0;
  if (t_count < window_samples) t_count++;
}

static void addCrackSample_locked(bool ok, uint16_t cr_x1000) {
  if (!ok) return;

  c_last = cr_x1000;
  c_last_valid = true;

  c_buf[c_wr] = cr_x1000;
  c_wr++;
  if (c_wr >= window_samples) c_wr = 0;
  if (c_count < window_samples) c_count++;
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

static bool computeCrackStats(uint16_t& c_avg, uint16_t& c_min, uint16_t& c_max) {
  if (c_count == 0) return false;

  uint32_t sum = 0;
  c_min = 65535;
  c_max = 0;

  int32_t idx = (int32_t)c_wr - (int32_t)c_count;
  while (idx < 0) idx += window_samples;

  for (uint16_t i = 0; i < c_count; i++) {
    uint16_t v = c_buf[idx];
    sum += v;
    if (v < c_min) c_min = v;
    if (v > c_max) c_max = v;
    idx++;
    if (idx >= window_samples) idx = 0;
  }

  c_avg = (uint16_t)((sum + (c_count/2)) / c_count);
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

// ========================= Uplink payload (18B) ===================== //
static bool buildPayload18_locked(uint16_t vb_mV) {
  // Must be called with gMux held
  int16_t t_avg=0, t_min=0, t_max=0;
  bool tok = computeStats(t_avg, t_min, t_max);

  int16_t t_cur = (t_last_c_x100 != INT16_MIN) ? t_last_c_x100 : (int16_t)0;

  if (!tok) {
    // no samples yet: mark as 0 and keep consistent
    t_avg = t_min = t_max = t_cur;
  }

  uint16_t c_avg=0, c_min=0, c_max=0;
  bool cok = computeCrackStats(c_avg, c_min, c_max);

  uint16_t c_cur = c_last_valid ? c_last : 0xFFFF;

  if (!cok) {
    if (c_cur == 0xFFFF) { c_avg = c_min = c_max = 0xFFFF; }
    else { c_avg = c_min = c_max = c_cur; }
  }

  appDataSize = 18;

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

  putU16(8,  c_cur);
  putU16(10, c_avg);
  putU16(12, c_min);
  putU16(14, c_max);

  putU16(16, vb_mV);

  Serial.printf(
    "TX 18B: Tcur=%d Tavg=%d Tmin=%d Tmax=%d | Crk=%u/%u/%u/%u (x1000) | VB=%u\n",
    (int)t_cur,(int)t_avg,(int)t_min,(int)t_max,
    (unsigned)c_cur,(unsigned)c_avg,(unsigned)c_min,(unsigned)c_max,
    (unsigned)vb_mV
  );
  return true;
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

  if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(80)) == pdTRUE) {
    int16_t t_avg=0, t_min=0, t_max=0;
    bool tok = computeStats(t_avg, t_min, t_max);

    if (t_count > 0 && t_last_c_x100 != INT16_MIN) d["t_cur_c"] = (double)t_last_c_x100 / 100.0;
    else d["t_cur_c"].set(nullptr);

    if (tok) {
      d["t_avg_c"] = (double)t_avg / 100.0;
      d["t_min_c"] = (double)t_min / 100.0;
      d["t_max_c"] = (double)t_max / 100.0;
    } else {
      d["t_avg_c"].set(nullptr);
      d["t_min_c"].set(nullptr);
      d["t_max_c"].set(nullptr);
    }

    uint16_t c_avg=0, c_min=0, c_max=0;
    bool cok = computeCrackStats(c_avg, c_min, c_max);
    uint16_t c_cur = (c_last_valid ? c_last : 0xFFFF);

    if (c_cur == 0xFFFF) d["crack_cur_x1000"].set(nullptr);
    else                 d["crack_cur_x1000"] = (uint32_t)c_cur;

    if (cok) d["crack_avg_x1000"] = (uint32_t)c_avg; else d["crack_avg_x1000"].set(nullptr);
    if (cok) d["crack_min_x1000"] = (uint32_t)c_min; else d["crack_min_x1000"].set(nullptr);
    if (cok) d["crack_max_x1000"] = (uint32_t)c_max; else d["crack_max_x1000"].set(nullptr);

    if (c_cur == 0xFFFF) d["crack_cur_mm"].set(nullptr);
    else                 d["crack_cur_mm"] = (double)c_cur / 1000.0;

    if (cok) d["crack_avg_mm"] = (double)c_avg / 1000.0; else d["crack_avg_mm"].set(nullptr);
    if (cok) d["crack_min_mm"] = (double)c_min / 1000.0; else d["crack_min_mm"].set(nullptr);
    if (cok) d["crack_max_mm"] = (double)c_max / 1000.0; else d["crack_max_mm"].set(nullptr);

    d["tx_period_min"] = cfg.minutes;
    d["meas_period_s"] = MEAS_PERIOD_FIXED_S;
    d["window_samples"] = window_samples;
    d["window_count"] = t_count;
    d["crack_window_count"] = c_count;

    const uint32_t reportPeriodMs = max<uint32_t>(60000UL, 60000UL * (uint32_t)cfg.minutes);
    const uint32_t elapsed = millis() - window_start_ms;
    d["next_tx_s"] = (elapsed >= reportPeriodMs) ? 0 : (uint32_t)((reportPeriodMs - elapsed) / 1000UL);

    // -------- log ----------
    JsonArray log = d.createNestedArray("log");
    uint32_t nowS = now_sec();

    for (uint16_t i = 0; i < web_cnt; i++) {
      int idx = (int)web_wr - 1 - (int)i;
      while (idx < 0) idx += WEB_LOG_N;

      JsonObject row = log.createNestedObject();
      row["age_s"] = (nowS >= web_ts_s[idx]) ? (nowS - web_ts_s[idx]) : 0;

      if (web_t[idx] == INT16_MIN) row["t_c"].set(nullptr);
      else                         row["t_c"] = (double)web_t[idx] / 100.0;

      if (!web_c_ok[idx]) row["crack_mm"].set(nullptr);
      else                row["crack_mm"] = (double)web_c[idx] / 1000.0;
    }

    // -------- cfg ----------
    JsonObject c = d.createNestedObject("cfg");
    c["minutes"] = cfg.minutes;
    c["meas_period_s"] = MEAS_PERIOD_FIXED_S;

    JsonObject cc = c.createNestedObject("crack");
    cc["ref_mode"] = (cfg.ref_mode == REF_MANUAL) ? "manual" : "auto";
    cc["len_mm"] = (double)cfg.crack_len_mm_x100 / 100.0;
    cc["r0"] = cfg.crack_r0;
    cc["r1"] = cfg.crack_r1;
    cc["mm0"] = (double)cfg.crack_mm0_x100 / 100.0;
    cc["mm1"] = (double)cfg.crack_mm1_x100 / 100.0;

    c["devEui_lsb"] = toHex(cfg.devEui, 8);
    c["appEui_lsb"] = toHex(cfg.appEui, 8);
    c["appKey_msb"] = toHex(cfg.appKey, 16);
    c["dr"] = cfg.lora_dr;
    c["adr"] = cfg.lora_adr;

    xSemaphoreGive(gMux);
  } else {
    d["t_cur_c"].set(nullptr);
    d["t_avg_c"].set(nullptr);
    d["t_min_c"].set(nullptr);
    d["t_max_c"].set(nullptr);

    d["crack_cur_x1000"].set(nullptr);
    d["crack_avg_x1000"].set(nullptr);
    d["crack_min_x1000"].set(nullptr);
    d["crack_max_x1000"].set(nullptr);

    d["crack_cur_mm"].set(nullptr);
    d["crack_avg_mm"].set(nullptr);
    d["crack_min_mm"].set(nullptr);
    d["crack_max_mm"].set(nullptr);
  }

  uint16_t batmV = 0;
  if (gSensMux && xSemaphoreTake(gSensMux, pdMS_TO_TICKS(250)) == pdTRUE) {
    sensorsPowerOn();
    batmV = readBattery_mV();
    sensorsPowerOff();
    xSemaphoreGive(gSensMux);
  }
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
  d["meas_period_s"] = MEAS_PERIOD_FIXED_S;

  JsonObject cr = d.createNestedObject("crack");
  cr["ref_mode"] = (cfg.ref_mode == REF_MANUAL) ? "manual" : "auto";
  cr["len_mm"] = (double)cfg.crack_len_mm_x100 / 100.0;
  cr["r0"] = cfg.crack_r0;
  cr["r1"] = cfg.crack_r1;
  cr["mm0"] = (double)cfg.crack_mm0_x100 / 100.0;
  cr["mm1"] = (double)cfg.crack_mm1_x100 / 100.0;

  d["dr"] = cfg.lora_dr;
  d["adr"] = cfg.lora_adr;

  String out; serializeJson(d, out);
  http.send(200, "application/json", out);
}

static void api_cfg_lora() {
  if (!http.hasArg("plain")) { http.send(400, "text/plain", "bad json"); return; }
  StaticJsonDocument<1500> d;
  if (deserializeJson(d, http.arg("plain"))) { http.send(400, "text/plain", "bad json"); return; }

  bool okHex = true;
  bool needRestart = false;
  bool needResetWindow = false;

  if (gMux) xSemaphoreTake(gMux, pdMS_TO_TICKS(200));

  if (d.containsKey("devEui_lsb")) { okHex &= parseHexLSB((const char*)d["devEui_lsb"], cfg.devEui, 8); needRestart = true; }
  if (d.containsKey("appEui_lsb")) { okHex &= parseHexLSB((const char*)d["appEui_lsb"], cfg.appEui, 8); needRestart = true; }
  if (d.containsKey("appKey_msb")) { okHex &= parseHexLSB((const char*)d["appKey_msb"], cfg.appKey, 16); needRestart = true; }

  if (d.containsKey("minutes")) {
    uint32_t newM = d["minutes"].as<uint32_t>();
    if (newM < 1 || newM > MINUTES_MAX) {
      if (gMux) xSemaphoreGive(gMux);
      http.send(400, "text/plain", "minutes range");
      return;
    }
    if (newM != cfg.minutes) {
      cfg.minutes = newM;
      prefs.begin("uhfb", false); prefs.putUInt("minutes", cfg.minutes); prefs.end();
      needResetWindow = true;
      appTxDutyCycle = 60000UL * (uint32_t)cfg.minutes;
      if (appTxDutyCycle < 60000UL) appTxDutyCycle = 60000UL;
    }
  }

  // meas fixed 10s (reject changes)
  if (d.containsKey("meas_period_s")) {
    int s = d["meas_period_s"].as<int>();
    if (s != (int)MEAS_PERIOD_FIXED_S) {
      if (gMux) xSemaphoreGive(gMux);
      http.send(400, "text/plain", "meas_period_s fixed=10");
      return;
    }
  }

  if (d.containsKey("crack")) {
    JsonObject cr = d["crack"].as<JsonObject>();

    if (cr.containsKey("ref_mode")) {
      String m = cr["ref_mode"].as<String>();
      cfg.ref_mode = (m == "manual") ? REF_MANUAL : REF_AUTO;
      prefs.begin("uhfb", false); prefs.putUChar("cr_ref", cfg.ref_mode); prefs.end();
    }

    if (cr.containsKey("len_mm")) {
      float L = cr["len_mm"].as<float>();
      if (!isfinite(L) || L < 0.01f || L > 1000.0f) {
        if (gMux) xSemaphoreGive(gMux);
        http.send(400, "text/plain", "len range");
        return;
      }
      cfg.crack_len_mm_x100 = (uint16_t)lrintf(L * 100.0f);
      prefs.begin("uhfb", false); prefs.putUShort("cr_len_x100", cfg.crack_len_mm_x100); prefs.end();
      needResetWindow = true;
    }

    if (cr.containsKey("r0")) cfg.crack_r0 = cr["r0"].as<int16_t>();
    if (cr.containsKey("r1")) cfg.crack_r1 = cr["r1"].as<int16_t>();
    if (cr.containsKey("mm0")) {
      float v = cr["mm0"].as<float>();
      if (!isfinite(v) || v < 0 || v > 1000) {
        if (gMux) xSemaphoreGive(gMux);
        http.send(400, "text/plain", "mm0 range");
        return;
      }
      cfg.crack_mm0_x100 = (uint16_t)lrintf(v * 100.0f);
    }
    if (cr.containsKey("mm1")) {
      float v = cr["mm1"].as<float>();
      if (!isfinite(v) || v < 0 || v > 1000) {
        if (gMux) xSemaphoreGive(gMux);
        http.send(400, "text/plain", "mm1 range");
        return;
      }
      cfg.crack_mm1_x100 = (uint16_t)lrintf(v * 100.0f);
    }

    if (cfg.crack_r0 == cfg.crack_r1) {
      if (gMux) xSemaphoreGive(gMux);
      http.send(400, "text/plain", "cal error");
      return;
    }
    prefs.begin("uhfb", false);
    prefs.putShort("cr_r0", cfg.crack_r0);
    prefs.putShort("cr_r1", cfg.crack_r1);
    prefs.putUShort("cr_mm0", cfg.crack_mm0_x100);
    prefs.putUShort("cr_mm1", cfg.crack_mm1_x100);
    prefs.end();
    needResetWindow = true;
  }

  if (d.containsKey("dr")) {
    int v = d["dr"].as<int>();
    if (v < 0 || v > 5) {
      if (gMux) xSemaphoreGive(gMux);
      http.send(400, "text/plain", "dr range");
      return;
    }
    cfg.lora_dr = (uint8_t)v;
    prefs.begin("uhfb", false); prefs.putUChar("lora_dr", cfg.lora_dr); prefs.end();
    needRestart = true;
  }

  if (d.containsKey("adr")) {
    cfg.lora_adr = d["adr"].as<bool>();
    prefs.begin("uhfb", false); prefs.putBool("lora_adr", cfg.lora_adr); prefs.end();
  }

  if (!okHex) {
    if (gMux) xSemaphoreGive(gMux);
    http.send(400, "text/plain", "hex error");
    return;
  }

  nvsPut("devEui", cfg.devEui, 8);
  nvsPut("appEui", cfg.appEui, 8);
  nvsPut("appKey", cfg.appKey, 16);

  if (needResetWindow) resetMeasWindow_locked();

  if (gMux) xSemaphoreGive(gMux);

  http.send(200, "text/plain", "ok");

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

  cfg.meas_period_s = prefs.getUShort("meas_s", MEAS_PERIOD_FIXED_S);
  cfg.meas_period_s = MEAS_PERIOD_FIXED_S;

  cfg.crack_len_mm_x100 = prefs.getUShort("cr_len_x100", cfg.crack_len_mm_x100);
  cfg.crack_r0 = prefs.getShort("cr_r0", cfg.crack_r0);
  cfg.crack_r1 = prefs.getShort("cr_r1", cfg.crack_r1);
  cfg.crack_mm0_x100 = prefs.getUShort("cr_mm0", cfg.crack_mm0_x100);
  cfg.crack_mm1_x100 = prefs.getUShort("cr_mm1", cfg.crack_mm1_x100);
  cfg.ref_mode = prefs.getUChar("cr_ref", cfg.ref_mode);

  cfg.lora_dr = prefs.getUChar("lora_dr", cfg.lora_dr);
  cfg.lora_adr = prefs.getBool("lora_adr", cfg.lora_adr);
  prefs.end();

  if (cfg.minutes < 1) cfg.minutes = 1;
  if (cfg.minutes > MINUTES_MAX) cfg.minutes = MINUTES_MAX;

  if (cfg.crack_r0 == cfg.crack_r1) cfg.crack_r1 = 25480;
}

// ========================= AP mode tasks (legacy) ================== //
static void measTask(void*) {
  const TickType_t periodTicks = pdMS_TO_TICKS((uint32_t)MEAS_PERIOD_FIXED_S * 1000UL);

  for (;;) {
    int16_t t = INT16_MIN;

    bool cr_ok = false;
    uint16_t cr_x1000 = 0;

    if (gSensMux && xSemaphoreTake(gSensMux, pdMS_TO_TICKS(800)) == pdTRUE) {
      sensorsPowerOn();

      t = readTemp_c_x100_signed();

      if (adsOk) {
        CrackMeas cr = readCrackOnce();
        float mm = cr.mm;
        if (isfinite(mm)) {
          if (mm < 0) mm = 0;
          if (mm > 65.535f) mm = 65.535f;
          cr_x1000 = (uint16_t)lroundf(mm * 1000.0f);
          cr_ok = true;
        }
      }

      sensorsPowerOff();
      xSemaphoreGive(gSensMux);
    }

    if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(200)) == pdTRUE) {
      addTempSample_locked(t);
      addCrackSample_locked(cr_ok, cr_x1000);
      webLogSample2_locked(t, cr_ok, cr_x1000);
      xSemaphoreGive(gMux);
    }

    vTaskDelay(periodTicks);
  }
}

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
  LoRaWAN.setDefaultDR(mapDR(cfg.lora_dr));
  enforceTxPolicy();

  Serial.println("[LoRa] join()");
  LoRaWAN.join();
  lastJoinTryMs = millis();
  lastJoinTryUs = esp_timer_get_time();

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
      uint32_t reportPeriodMs = 60000UL * (uint32_t)cfg.minutes;
      if (reportPeriodMs < 60000UL) reportPeriodMs = 60000UL;

      bool due = false;

      uint32_t ws = 0;
      uint32_t m  = 10;

      if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(120)) == pdTRUE) {
        ws = window_start_ms;
        m  = cfg.minutes;
        xSemaphoreGive(gMux);
      } else {
        ws = window_start_ms;
        m  = cfg.minutes;
      }

      reportPeriodMs = 60000UL * m;
      if (reportPeriodMs < 60000UL) reportPeriodMs = 60000UL;

      if ((now - ws) >= reportPeriodMs) due = true;
      if (forceTxOnce) due = true;

      if (due) {
        enforceTxPolicy();

        uint16_t vb = readBattery_mV();

        if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(250)) == pdTRUE) {
          buildPayload18_locked(vb);
          LoRaWAN.send();
          resetMeasWindow_locked();
          forceTxOnce = false;
          xSemaphoreGive(gMux);
        } else {
          Serial.println("[LoRa] payload lock timeout; skip TX");
        }
      }
    }

    LoRaWAN.sleep(CLASS_A);
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// ========================= Field mode (light sleep) ================= //
static void doOneMeasurement_field() {
  int16_t t = INT16_MIN;
  bool cr_ok = false;
  uint16_t cr_x1000 = 0;

  if (gSensMux && xSemaphoreTake(gSensMux, pdMS_TO_TICKS(800)) == pdTRUE) {
    sensorsPowerOn();

    t = readTemp_c_x100_signed();

    if (adsOk) {
      CrackMeas cr = readCrackOnce();
      float mm = cr.mm;
      if (isfinite(mm)) {
        if (mm < 0) mm = 0;
        if (mm > 65.535f) mm = 65.535f;
        cr_x1000 = (uint16_t)lroundf(mm * 1000.0f);
        cr_ok = true;
      }
    }

    sensorsPowerOff();
    xSemaphoreGive(gSensMux);
  }

  if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(200)) == pdTRUE) {
    addTempSample_locked(t);
    addCrackSample_locked(cr_ok, cr_x1000);
    webLogSample2_locked(t, cr_ok, cr_x1000);
    xSemaphoreGive(gMux);
  }
}

static void doOneUplinkIfDue_field() {
  loraInitOnce();

  const uint64_t nowUs = esp_timer_get_time();

  if (!isJoined()) {
    if (nowUs - lastJoinTryUs >= 30ULL * 1000000ULL) {
      Serial.println("[LoRa] join retry");
      LoRaWAN.join();
      enforceTxPolicy();
      lastJoinTryUs = nowUs;
      lastJoinTryMs = millis();
    }
    return;
  }

  // due every cfg.minutes (in samples) OR forced
  uint32_t mins = cfg.minutes;
  if (mins < 1) mins = 1;
  if (mins > MINUTES_MAX) mins = MINUTES_MAX;

  const uint32_t samplesPerTx = (uint32_t)((mins * 60UL) / MEAS_PERIOD_FIXED_S);
  const uint32_t sp = (samplesPerTx < 1) ? 1 : samplesPerTx;

  bool due = false;
  if (forceTxOnce) due = true;
  if ((measCounter % sp) == 0) due = true;

  if (!due) return;

  enforceTxPolicy();

  // OPT: battery read does NOT need sensors power (saves 40ms + ADS init)
  const uint16_t vb = readBattery_mV();

  if (gMux && xSemaphoreTake(gMux, pdMS_TO_TICKS(350)) == pdTRUE) {
    buildPayload18_locked(vb);
    LoRaWAN.send();                 // no extra delay after send
    resetMeasWindow_locked();
    forceTxOnce = false;
    xSemaphoreGive(gMux);
  } else {
    Serial.println("[LoRa] payload lock timeout; skip TX");
  }
}

static void sleepUntilNextMeasurement_field() {
  const uint64_t nowUs = esp_timer_get_time();

  if (nextMeasUs == 0) nextMeasUs = nowUs + (uint64_t)MEAS_PERIOD_FIXED_S * 1000000ULL;

  // Catch-up: if we are late (e.g., long send/RX windows), skip slots
  // so we don't get stuck in near-zero sleeps.
  while ((int64_t)(nextMeasUs - nowUs) <= 1000) {
    nextMeasUs += (uint64_t)MEAS_PERIOD_FIXED_S * 1000000ULL;
  }

  const uint64_t deltaUs = nextMeasUs - nowUs;

  // Put LoRa stack/radio to sleep before MCU light sleep
  LoRaWAN.sleep(CLASS_A);

  esp_sleep_enable_timer_wakeup(deltaUs);
  esp_light_sleep_start();

  // strict schedule: +10s slot, no drift accumulation
  nextMeasUs += (uint64_t)MEAS_PERIOD_FIXED_S * 1000000ULL;
}

// ============================== Setup/Loop ========================= //
static constexpr char AP_SSID_PREFIX[] = "Riss-";

void setup() {
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  // Power domains optimization for sleep
  esp_sleep_pd_config(ESP_PD_DOMAIN_RTC_PERIPH, ESP_PD_OPTION_OFF);

  // Enable light sleep + DVFS (field mode benefits)
  esp_pm_config_t pm_cfg;
  pm_cfg.max_freq_mhz = 80;
  pm_cfg.min_freq_mhz = 10;
  pm_cfg.light_sleep_enable = true;
  (void)esp_pm_configure(&pm_cfg);

  gMux = xSemaphoreCreateMutex();
  gSensMux = xSemaphoreCreateMutex();

  pinMode(PIN_MODE_OUT, OUTPUT);
  digitalWrite(PIN_MODE_OUT, LOW);
  pinMode(PIN_MODE_IN, INPUT_PULLUP);
  delay(5);

  Serial.printf("FW %s build %s %s\n", FW_VER, __DATE__, __TIME__);

  for (int i = 0; i < 5; i++) { apMode |= (digitalRead(PIN_MODE_IN) == LOW); delay(2); }

  analogReadResolution(12);
  loadCfg();

  cfg.meas_period_s = MEAS_PERIOD_FIXED_S;

  appTxDutyCycle = 60000UL * (uint32_t)cfg.minutes;
  if (appTxDutyCycle < 60000UL) appTxDutyCycle = 60000UL;

  if (gMux) {
    xSemaphoreTake(gMux, pdMS_TO_TICKS(200));
    resetMeasWindow_locked();
    xSemaphoreGive(gMux);
  } else {
    window_samples = 60;
    window_start_ms = millis();
  }

  if (apMode) {
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, LOW);   // Vext ON

    WiFi.mode(WIFI_AP);
    WiFi.setTxPower(WIFI_POWER_7dBm);

    bool fsOk = LittleFS.begin(true);
    if (fsOk) attachHttpFS();
    else Serial.println("[FS] mount failed");

    String ssid = String(AP_SSID_PREFIX) + devEuiSuffix6LSB();
    WiFi.softAP(ssid.c_str(), AP_PSK);

    oledSplash();
    oledPhase = OLED_SSID;
    oledPhaseStart = millis();
    oledShowSSID(ssid);

    xTaskCreatePinnedToCore(measTask, "meas", 4096, nullptr, 2, &measTaskH, 1);
    xTaskCreatePinnedToCore(loraTask, "lora", 6144, nullptr, 1, &loraTaskH, 0);
  } else {
    // Field mode: disable WiFi/BT + OLED power off
    WiFi.persistent(false);
    WiFi.disconnect(true, true);
    WiFi.mode(WIFI_OFF);
#if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
    btStop();
#endif
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, HIGH); // Vext OFF

    // Field mode light-sleep scheduler init
    nextMeasUs = esp_timer_get_time() + (uint64_t)MEAS_PERIOD_FIXED_S * 1000000ULL;
    lastJoinTryUs = 0;
    measCounter = 0;

    // Optional: reduce UART overhead in field mode if needed
    // Serial.setDebugOutput(false);
  }
}

void loop() {
  if (apMode) {
    http.handleClient();

    const uint32_t now = millis();

    if (oledPhase == OLED_SSID && (now - oledPhaseStart) >= 1500UL) {
      oledPhase = OLED_SENS;
      oledRefreshMs = 0;
      oledSensorsOnce();
    }

    if (oledPhase == OLED_SENS && (now - oledRefreshMs) >= 1000UL) {
      oledRefreshMs = now;
      oledSensorsOnce();
    }

    delay(5);
    return;
  }

  // ========== Field mode: strict 10s measurement + light sleep ==========
  doOneMeasurement_field();
  measCounter++;

  doOneUplinkIfDue_field();

  sleepUntilNextMeasurement_field();
}
