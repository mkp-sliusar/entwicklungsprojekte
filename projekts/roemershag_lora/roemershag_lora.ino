/*
 * =====================================================================
 *  Project: Heltec ESP32-S3 (LoRa V3) — Crack Monitoring Node
 *  Function: Ratio (CH0/CH1) with low-noise dynamic scaling (no averaging)
 * ---------------------------------------------------------------------
 *  Hardware:
 *    - ADS1115 (I2C1)
 *    - DS18B20 (OneWire)
 *    - SSD1306 OLED
 *  Features:
 *    - AP config page (LittleFS)
 *    - LoRaWAN uplink (8 bytes, compatible format)
 *    - Crack length by ratio: L = L_full * CH0 / CH1
 *      CH0 = sensor (pot), CH1 = reference from same Vee
 *      If ref.mode=manual → CH1 = fixed r1 (one ADC field r1)
 *      If CH1 == 0 → linear fallback
 *  Modes:
 *    AUTO    — dynamic scaling by ADS1115 CH1
 *    MANUAL  — linear map r0→mm0, r1→mm1 on CH0
 * ---------------------------------------------------------------------
 *  LoRaWAN ID conventions:
 *    DevEUI, AppEUI — LSB order end-to-end (UI, storage, prints)
 *    AppKey         — MSB order end-to-end (UI, storage, prints)
 * ---------------------------------------------------------------------
 *  Version:      1.7.2
 *  Date:         2026-03-19
 *  Changes:      unified TX policy for unstable networks: confirmed uplink, NbTrans=1, retries=3
 *                removed AP/field TX divergence; enforced policy at init and before each send
 *                added low-power auto-rejoin watchdog: after 5 consecutive missed network responses
 *                the node performs a controlled reboot and OTAA join without increasing TX cadence
 *  Contributors: Bernd Schinköthe, Roman Sliusar, Evgenij Koloda
 * =====================================================================
 */

// ============================= Includes ============================= //
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

// NOTE: Access to LoRaMac MIB for NbTrans is not available in Heltec build.
// We rely on the confirmed-uplink policy without direct MIB access.

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

// ========================== Frimware version ======================= //
static constexpr const char* FW_VER = "1.7.2";  // sync with header

// ========================== LoRaWAN key slots ======================= //
// The Heltec core expects these globals. Keep LSB/MSB conventions.
uint8_t devEui[8] = { 0 }, appEui[8] = { 0 };  // LSB
uint8_t appKey[16] = { 0 };                    // MSB
uint8_t DevEui[8] = { 0 }, AppEui[8] = { 0 }, AppKey[16] = { 0 };

// ============================== Types ============================== //
/**
 * @brief Result of a single crack sensor read.
 */
struct CrackMeas {
  bool present;    // sensor presence pin indicates installed sensor
  int16_t raw0;    // ADS1115 CH0 raw
  int16_t raw1;    // ADS1115 CH1 raw (reference)
  float mm;        // computed crack opening in mm
  bool auto_used;  // true if AUTO mode mapping was used
};
CrackMeas readCrack();

// =========================== HW constants ========================== //
static constexpr uint8_t OLED_ADDR = 0x3C;
static constexpr int I2C_ADS_SDA = 47;
static constexpr int I2C_ADS_SCL = 48;
static constexpr uint32_t I2C_FREQ_HZ = 400000;

static constexpr int PIN_ONE_WIRE = 4;
static constexpr int PIN_VBAT_ADC = 1;
static constexpr int PIN_ADC_CTRL = 37;
static constexpr int PIN_MODE_OUT = 45;
static constexpr int PIN_MODE_IN = 46;
static constexpr int PIN_CRACK_PRESENT = 3;

static constexpr int PIN_SENS_EN = 7;
static constexpr bool SENS_ACTIVE_HIGH = true;

static constexpr uint8_t ADS1115_ADDR = 0x48;
static constexpr uint8_t DS_RES_BITS = 10;

static constexpr float VBAT_ADC_FACTOR = 1.0f / 4096.0f / 0.210282997855762f;  // board-specific divider and ADC scaling

// ============================ System cfg =========================== //
static constexpr char AP_PSK[] = "12345678";
static constexpr size_t JSON_STATE_DOC = 1700;
static constexpr size_t JSON_CFG_DOC = 1100;
static constexpr uint32_t MS_PER_SEC = 1000UL;
static constexpr uint32_t MARQUEE_PERIOD_MS = 40;

static constexpr uint8_t UPLINK_PORT = 2;
// Kept for reference; policy is unified and not branching on AP/field.
static constexpr uint8_t CONF_TRIALS_AP = 3, CONF_TRIALS_FIELD = 1;

static inline uint32_t now_sec() {
  return millis() / MS_PER_SEC;
}

// ========================== Peripherals ============================ //
SSD1306Wire OLED_Display(OLED_ADDR, I2C_FREQ_HZ, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);
TwoWire I2C_ADS = TwoWire(1);
Adafruit_ADS1115 ads;

OneWire oneWire(PIN_ONE_WIRE);
DallasTemperature sensors(&oneWire);
uint8_t firstDs[8] = { 0 };
bool firstDsFound = false;

bool apMode = false;
Preferences prefs;

enum RefMode : uint8_t { REF_AUTO = 0,
                         REF_MANUAL = 1 };

// ========================= Persisted config ======================== //
/**
 * @brief Non-volatile configuration stored in NVS (Preferences).
 */
struct Cfg {
  uint8_t devEui[8] = { 0 };
  uint8_t appEui[8] = { 0 };
  uint8_t appKey[16] = { 0 };

  uint32_t minutes = 15;  // uplink period in minutes

  int16_t crack_r0 = 5;               // manual mapping raw0
  int16_t crack_r1 = 25480;           // manual mapping raw1
  uint16_t crack_mm0_x100 = 0;        // 0.01 mm units
  uint16_t crack_mm1_x100 = 1000;     // 10.00 mm default
  uint16_t crack_len_mm_x100 = 1000;  // L_full in 0.01 mm

  uint8_t ref_mode = REF_AUTO;  // AUTO vs MANUAL

  uint8_t lora_dr = 3;   // default DR index
  bool lora_adr = true;  // ADR preference

  bool ts_enable = false;  // time sync request enable
  uint16_t ts_hours = 24;  // time sync period hours
} cfg;

// ============================ LoRa state =========================== //
uint16_t userChannelsMask[6] = { 0x00FF, 0, 0, 0, 0, 0 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t loraWanClass = CLASS_A;

bool overTheAirActivation = true;  // OTAA
bool loraWanAdr = true;            // mirrored to cfg.lora_adr where needed

bool isTxConfirmed = false;  // policy applied via enforceTxPolicy()
uint8_t appPort = UPLINK_PORT;
uint8_t confirmedNbTrials = 1;          // stack-level retry count for confirmed
uint32_t appTxDutyCycle = 60000UL * 2;  // unused, see cycle() path

uint8_t nwkSKey[16] = { 0 };  // left for potential ABP compatibility
uint8_t appSKey[16] = { 0 };
uint32_t devAddr = 0;

// ===================== Auto-rejoin watchdog ====================== //
// Energy-neutral strategy: keep the same reporting interval and only
// trigger a recovery after 5 consecutive uplinks without any network
// response (ACK, MAC command, or application downlink). Recovery is
// done by a controlled reboot, which forces a fresh OTAA join on boot.
static constexpr uint8_t REJOIN_MISS_LIMIT = 5;
volatile uint8_t netMissCount = 0;
volatile bool rejoinRebootRequested = false;
volatile uint32_t lastNetRxMs = 0;


// ========================= NVS small helpers ======================= //
static inline void nvsPut(const char* k, const void* p, size_t n) {
  prefs.begin("uhfb", false);
  prefs.putBytes(k, p, n);
  prefs.end();
}
static inline void nvsGet(const char* k, void* p, size_t n) {
  prefs.begin("uhfb", true);
  prefs.getBytes(k, p, n);
  prefs.end();
}

/**
 * @brief Load persisted configuration into RAM.
 */
void loadCfg() {
  prefs.begin("uhfb", true);
  prefs.getBytes("devEui", cfg.devEui, 8);
  prefs.getBytes("appEui", cfg.appEui, 8);
  prefs.getBytes("appKey", cfg.appKey, 16);
  cfg.minutes = prefs.getUInt("minutes", cfg.minutes);
  cfg.crack_len_mm_x100 = prefs.getUShort("cr_len_x100", cfg.crack_len_mm_x100);
  cfg.crack_r0 = prefs.getShort("cr_r0", cfg.crack_r0);
  cfg.crack_r1 = prefs.getShort("cr_r1", cfg.crack_r1);
  cfg.crack_mm0_x100 = prefs.getUShort("cr_mm0", cfg.crack_mm0_x100);
  cfg.crack_mm1_x100 = prefs.getUShort("cr_mm1", cfg.crack_mm1_x100);
  if (cfg.crack_mm1_x100 == 1000) cfg.crack_mm1_x100 = cfg.crack_len_mm_x100;  // keep mm1 aligned to L
  cfg.ref_mode = prefs.getUChar("ref_mode", cfg.ref_mode);
  cfg.lora_dr = prefs.getUChar("lora_dr", cfg.lora_dr);
  cfg.lora_adr = prefs.getBool("lora_adr", cfg.lora_adr);
  cfg.ts_enable = prefs.getBool("ts_en", cfg.ts_enable);
  cfg.ts_hours = prefs.getUShort("ts_h", cfg.ts_hours);
  prefs.end();
}

// ============================ Hex helpers ========================== //
static inline bool parseHexStraight(const String& s, uint8_t* out, size_t n) {
  if (s.length() != n * 2) return false;
  for (size_t i = 0; i < n; i++) {
    char b[3] = { (char)s[2 * i], (char)s[2 * i + 1], 0 };
    out[i] = (uint8_t)strtoul(b, nullptr, 16);
  }
  return true;
}
static inline bool parseHexLSB(const String& s, uint8_t* out, size_t n) {
  return parseHexStraight(s, out, n);
}
static inline bool parseHexMSB_reverse(const String& msb, uint8_t* out, size_t n) {
  if (msb.length() != n * 2) return false;
  for (size_t i = 0; i < n; i++) {
    char b[3] = { (char)msb[2 * i], (char)msb[2 * i + 1], 0 };
    out[n - 1 - i] = (uint8_t)strtoul(b, nullptr, 16);
  }
  return true;
}
String toHex(const uint8_t* v, size_t n) {
  char b[3];
  String s;
  s.reserve(n * 2);
  for (size_t i = 0; i < n; i++) {
    sprintf(b, "%02X", v[i]);
    s += b;
  }
  return s;
}

/**
 * @brief Build DevEUI from ESP32 MAC (EUI-64 format MSB order).
 */
void devEuiFromChipMSB(uint8_t out[8]) {
  uint64_t mac = ESP.getEfuseMac();
  uint8_t m[6];
  for (int i = 0; i < 6; i++) m[i] = (mac >> (8 * (5 - i))) & 0xFF;
  out[0] = m[0];
  out[1] = m[1];
  out[2] = m[2];
  out[3] = 0xFF;
  out[4] = 0xFE;
  out[5] = m[3];
  out[6] = m[4];
  out[7] = m[5];
}
String devEuiSuffix6LSB() {
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
static inline bool crackPresent() {
  pinMode(PIN_CRACK_PRESENT, INPUT_PULLUP);
  return digitalRead(PIN_CRACK_PRESENT) == LOW;
}

/**
 * @brief One-time ADS1115 init for low-noise single-shot use.
 */
void adsInitOnce() {
  ads.setGain(GAIN_ONE);
  ads.setDataRate(RATE_ADS1115_128SPS);
  (void)ads.readADC_SingleEnded(0);
  delay(4);  // ~1 period @128SPS
}

/**
 * @brief Power up sensors and buses, discover DS18B20 if needed.
 */
void sensorsPowerOn() {
  SensorsON();
  delay(12);
  I2C_ADS.begin(I2C_ADS_SDA, I2C_ADS_SCL, I2C_FREQ_HZ);
  if (ads.begin(ADS1115_ADDR, &I2C_ADS)) adsInitOnce();
  else Serial.println("ADS1115 not found");

  if (!firstDsFound) {
    sensors.begin();
    firstDsFound = sensors.getAddress(firstDs, 0);
    if (!firstDsFound) {
      OneWire ow(PIN_ONE_WIRE);
      uint8_t a[8];
      ow.reset_search();
      while (ow.search(a)) {
        if (a[0] == 0x28) {
          memcpy(firstDs, a, 8);
          firstDsFound = true;
          break;
        }
      }
    }
  }
  if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
}

/**
 * @brief Power down sensors and release bus pins to input.
 */
void sensorsPowerOff() {
  I2C_ADS.end();
  pinMode(I2C_ADS_SDA, INPUT);
  pinMode(I2C_ADS_SCL, INPUT);
  pinMode(PIN_ONE_WIRE, INPUT);
  SensorsOFF();
}

/**
 * @brief Read temperature as 0..10000 plus 5000 offset for 0..100°C mapping.
 * @return int16 in range 0..10000 (with 5000 offset during packing), default 5000 if invalid.
 */
int16_t readTemp_c_x100() {
  if (!firstDsFound) return 5000;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200);
  float t = sensors.getTempC(firstDs);
  if (!isfinite(t) || t < -50 || t > 125) return 5000;

  long v = lroundf(t * 100.0f) + 5000;  // 24.25°C -> 2425
  if (v < 0) v = 0;
  if (v > 10000) v = 10000;
  return (int16_t)v;
}

/**
 * @brief Single temperature read, returns NAN if invalid.
 */
float readTempOnceC() {
  if (!firstDsFound) return NAN;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200);
  float t = sensors.getTempC(firstDs);
  if (t <= -127 || t >= 125) return NAN;
  return t;
}

// ============================ Mapping ============================== //
static constexpr int16_t RAW_MIN_STABLE = 6;  // ADC floor threshold
static constexpr int16_t DYN_SPAN_MIN = 50;   // minimal dynamic span

static inline float mapLinear_i32(int32_t x, int32_t in_min, int32_t in_max, float out_min, float out_max) {
  if (in_max <= in_min) return out_min;
  return (float)(x - in_min) * (out_max - out_min) / (float)(in_max - in_min) + out_min;
}

/**
 * @brief Manual mapping CH0 -> mm using two-point calibration.
 */
static inline float mapCrackManual(int M) {
  const float M0 = (float)cfg.crack_r0, M1 = (float)cfg.crack_r1;
  const float mm0 = (float)cfg.crack_mm0_x100 / 100.0f, mm1 = (float)cfg.crack_mm1_x100 / 100.0f;
  if (M1 == M0) return 0.0f;
  float mm = mm0 + (mm1 - mm0) * ((float)M - M0) / (M1 - M0);
  if (mm < 0) mm = 0;
  const float L = (float)cfg.crack_len_mm_x100 / 100.0f;
  if (mm > L) mm = L;
  return mm;
}

/**
 * @brief Low-noise single-shot reads on ADS1115 with mux pre-warm.
 */
static inline int16_t readCH(uint8_t ch) {
  (void)ads.readADC_SingleEnded(ch);  // pre-warm after mux switch
  delayMicroseconds(8000);            // ≥1 period @128SPS
  int16_t v = ads.readADC_SingleEnded(ch);
  return v < 0 ? 0 : v;
}

/**
 * @brief Read crack sensor and compute mm in AUTO or MANUAL mode.
 */
CrackMeas readCrack() {
  CrackMeas r{ false, 0, 0, 0.0f, false };
  r.present = crackPresent();
  if (!r.present) return r;

  int16_t M = readCH(0);
  int16_t R = readCH(1);
  r.raw0 = M;
  r.raw1 = R;

  if (cfg.ref_mode == REF_MANUAL) {
    r.mm = mapCrackManual(M);
    r.auto_used = false;
  } else {
    int16_t dynMax = max<int16_t>(RAW_MIN_STABLE + DYN_SPAN_MIN, R);
    if (dynMax <= RAW_MIN_STABLE) dynMax = RAW_MIN_STABLE + DYN_SPAN_MIN;  // fallback
    const float L = (float)cfg.crack_len_mm_x100 / 100.0f;
    float mm = mapLinear_i32(M, RAW_MIN_STABLE, dynMax, 0.0f, L);
    if (mm < 0) mm = 0;
    if (mm > L) mm = L;
    r.mm = mm;
    r.auto_used = true;
  }
  if (r.mm > 65.535f) r.mm = 65.535f;  // payload limit safety
  return r;
}

// ============================ Battery ============================== //
/**
 * @brief Read battery voltage in millivolts using board-specific factor.
 */
uint16_t readBattery_mV() {
  analogSetAttenuation(ADC_0db);
  pinMode(PIN_ADC_CTRL, OUTPUT);
  digitalWrite(PIN_ADC_CTRL, HIGH);
  delay(2);
  int raw = analogRead(PIN_VBAT_ADC);
  digitalWrite(PIN_ADC_CTRL, LOW);
  return (uint16_t)(VBAT_ADC_FACTOR * raw * 1000.0f);
}

// =========================== LoRa helpers ========================== //
static inline uint8_t mapDR(uint8_t i) {
  switch (i) {
    case 0: return DR_0;
    case 1: return DR_1;
    case 2: return DR_2;
    case 3: return DR_3;
    case 4: return DR_4;
    case 5: return DR_5;
    default: return DR_3;
  }
}
static inline void applyLoraDR() {
  LoRaWAN.setDefaultDR(mapDR(cfg.lora_dr));
}

/**
 * @brief Enforce a unified TX policy for unstable links.
 *        Current policy: confirmed uplink, up to 3 trials, ADR disabled.
 *        Note: Keep functional behavior intact per 1.7.1 change log.
 */
static void enforceTxPolicy() {
  isTxConfirmed = true;   // confirmed uplink
  confirmedNbTrials = 3;  // up to 3 retries decided by the stack
  loraWanAdr = false;     // avoid network ADR requests after sleep
}

// ================ Downlink indicators for diagnostics ============== //
volatile bool lora_has_rx = false;
volatile int16_t lora_last_rssi = 0;
volatile int8_t lora_last_snr = 0;
volatile uint32_t lora_last_rx_ms = 0;

static void scheduleRejoinReboot(const char* reason) {
  Serial.printf("[REJOIN] scheduled after %u missed responses (%s)\n",
                (unsigned)netMissCount, reason ? reason : "unknown");
  rejoinRebootRequested = true;
}

static void onNetworkResponse() {
  netMissCount = 0;
  lora_has_rx = true;
  lastNetRxMs = millis();
}

static void onUplinkAttempt() {
  if (netMissCount < 255) netMissCount++;
  Serial.printf("[TX] waiting for network response, miss=%u/%u\n",
                (unsigned)netMissCount,
                (unsigned)REJOIN_MISS_LIMIT);
  if (netMissCount >= REJOIN_MISS_LIMIT) {
    scheduleRejoinReboot("miss threshold reached");
  }
}

extern "C" void downLinkDataHandle(McpsIndication_t* ind) {
  lora_last_rssi = ind->Rssi;
  lora_last_snr = ind->Snr;
  lora_last_rx_ms = millis();
  onNetworkResponse();
  Serial.printf("[DL] port=%u size=%u rssi=%d snr=%d\n",
                (unsigned)ind->Port,
                (unsigned)ind->BufferSize,
                (int)ind->Rssi,
                (int)ind->Snr);
}

// ============================== OLED =============================== //
struct OledMarquee {
  String text;
  int y = 28;
  int w = 0;
  int x = 0;
  bool active = false;
  uint32_t last = 0;
} apScroll;

/**
 * @brief Horizontal marquee tick for AP status line.
 */
void oledMarqueeTick() {
  if (!apScroll.active) return;
  const uint32_t now = millis();
  if (now - apScroll.last < MARQUEE_PERIOD_MS) return;
  apScroll.last = now;
  OLED_Display.setColor(BLACK);
  OLED_Display.fillRect(0, apScroll.y, 128, 12);
  OLED_Display.setColor(WHITE);
  int x = -apScroll.x;
  OLED_Display.drawString(x, apScroll.y, apScroll.text);
  OLED_Display.drawString(x + apScroll.w + 16, apScroll.y, apScroll.text);
  apScroll.x += 2;
  if (apScroll.x > apScroll.w + 16) apScroll.x = 0;
  OLED_Display.display();
}

/**
 * @brief Show splash with MKP logo at boot.
 */
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
  delay(3000);
}

/**
 * @brief Render AP SSID/IP screen and stop marquee.
 */
static void oledShowSSID(const String& ssid) {
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 0, "Access Point");
  OLED_Display.drawString(0, 16, "SSID: " + ssid);
  OLED_Display.drawString(0, 32, "IP: " + WiFi.softAPIP().toString());
  OLED_Display.display();
  apScroll.active = false;
}

/**
 * @brief One-shot sensor screen for AP mode.
 */
void oledSensorsOnce() {
  // Acquire data before drawing to minimize flicker.
  sensorsPowerOn();
  CrackMeas cr = readCrack();
  float t = readTempOnceC();
  uint16_t bat = readBattery_mV();
  sensorsPowerOff();

  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  int y = 0, dy = 12;

  if (isfinite(t)) OLED_Display.drawString(0, y, String("DS18B20  ") + String(t, 1) + " C");
  else OLED_Display.drawString(0, y, "DS18B20  N/C");
  y += dy;

  OLED_Display.drawString(0, y, String("ADC M   ") + (cr.present ? String(cr.raw0) : "N/C"));
  y += dy;
  OLED_Display.drawString(0, y, String("ADC R   ") + (cr.present ? String(cr.raw1) : "N/C"));
  y += dy;

  if (cr.present) {
    const char* tag = (cfg.ref_mode == REF_MANUAL) ? " (M)" : " (A)";
    OLED_Display.drawString(0, y, String("Crack   ") + String(cr.mm, 3) + " mm" + tag);
  } else OLED_Display.drawString(0, y, "Crack   N/C");
  y += dy;

  OLED_Display.drawString(0, y, String("Battery ") + String(bat) + " mV");
  OLED_Display.display();
}

// ============================== Web API ============================ //
WebServer http(80);

/**
 * @brief Return current IP string depending on Wi-Fi mode.
 */
String ipStr() {
  if (WiFi.getMode() == WIFI_AP) return WiFi.softAPIP().toString();
  if (WiFi.status() == WL_CONNECTED) return WiFi.localIP().toString();
  return "-";
}

/**
 * @brief Basic content type resolution for static files.
 */
String contentTypeFor(const String& p) {
  if (p.endsWith(".html")) return "text/html; charset=utf-8";
  if (p.endsWith(".svg")) return "image/svg+xml";
  if (p.endsWith(".json")) return "application/json; charset=utf-8";
  if (p.endsWith(".css")) return "text/css";
  if (p.endsWith(".js")) return "application/javascript";
  return "application/octet-stream";
}

/**
 * @brief Serve a LittleFS file via HTTP if present.
 */
bool streamFileFS(const char* p) {
  if (!LittleFS.exists(p)) return false;
  File f = LittleFS.open(p, "r");
  if (!f) return false;
  http.streamFile(f, contentTypeFor(p));
  f.close();
  return true;
}

/**
 * @brief GET /api/state — current runtime state and snapshot data.
 */
void api_state() {
  StaticJsonDocument<JSON_STATE_DOC> d;
  d["mode"] = (WiFi.getMode() == WIFI_AP) ? "AP" : "LNS";
  d["ip"] = ipStr();
  d["uptime_s"] = (uint32_t)(millis() / MS_PER_SEC);
  d["heap_free"] = ESP.getFreeHeap();
  d["fw"] = FW_VER;
  d["fw_build"] = String(__DATE__) + " " + String(__TIME__);

  sensorsPowerOn();
  CrackMeas cr = readCrack();
  uint16_t batmV = readBattery_mV();
  float tC = readTempOnceC();
  sensorsPowerOff();

  d["crack_present"] = cr.present;
  d["adc_raw"] = cr.present ? cr.raw0 : 0;
  d["adc_ref"] = cr.present ? cr.raw1 : 0;
  if (cr.present) {
    d["crack_mm"] = cr.mm;
    d["auto"] = cr.auto_used;
  } else {
    d["crack_mm"] = nullptr;
    d["auto"] = nullptr;
  }
  d["batt_mV"] = batmV;
  d["DS18B20_Temp"] = isfinite(tC) ? tC : (double)NAN;

  JsonObject c = d.createNestedObject("cfg");
  c["minutes"] = cfg.minutes;
  c["devEui_lsb"] = toHex(cfg.devEui, 8);
  c["appEui_lsb"] = toHex(cfg.appEui, 8);
  c["appKey_msb"] = toHex(cfg.appKey, 16);
  c["crack_len_x100"] = cfg.crack_len_mm_x100;

  JsonObject cal = c.createNestedObject("crack_cal");
  cal["r0"] = cfg.crack_r0;
  cal["mm0"] = (float)cfg.crack_mm0_x100 / 100.0f;
  cal["r1"] = cfg.crack_r1;
  cal["mm1"] = (float)cfg.crack_mm1_x100 / 100.0f;

  c["dr"] = cfg.lora_dr;
  c["adr"] = cfg.lora_adr;

  JsonObject ts = c.createNestedObject("time_sync");
  ts["enabled"] = cfg.ts_enable;
  ts["hours"] = cfg.ts_hours;

  JsonObject ref = c.createNestedObject("ref");
  ref["mode"] = (cfg.ref_mode == REF_MANUAL) ? "manual" : "auto";

  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}

/**
 * @brief POST /api/send — schedule immediate uplink through state machine.
 */
void api_send() {
  deviceState = DEVICE_STATE_SEND;
  http.send(200, "text/plain", "queued");
}

/**
 * @brief POST /api/reset — restart the MCU.
 */
void api_reset() {
  http.send(200, "text/plain", "restarting");
  delay(200);
  ESP.restart();
}

/**
 * @brief GET /api/config — return current configuration snapshot.
 */
void api_cfg_get() {
  StaticJsonDocument<JSON_CFG_DOC> d;
  d["devEui_lsb"] = toHex(cfg.devEui, 8);
  d["appEui_lsb"] = toHex(cfg.appEui, 8);
  d["appKey_msb"] = toHex(cfg.appKey, 16);
  d["minutes"] = cfg.minutes;
  d["crack_len_x100"] = cfg.crack_len_mm_x100;

  JsonObject cal = d.createNestedObject("crack_cal");
  cal["r0"] = cfg.crack_r0;
  cal["mm0"] = (float)cfg.crack_mm0_x100 / 100.0f;
  cal["r1"] = cfg.crack_r1;
  cal["mm1"] = (float)cfg.crack_mm1_x100 / 100.0f;

  d["dr"] = cfg.lora_dr;
  d["adr"] = cfg.lora_adr;

  JsonObject ts = d.createNestedObject("time_sync");
  ts["enabled"] = cfg.ts_enable;
  ts["hours"] = cfg.ts_hours;

  JsonObject ref = d.createNestedObject("ref");
  ref["mode"] = (cfg.ref_mode == REF_MANUAL) ? "manual" : "auto";

  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}

/**
 * @brief POST /api/config/lora — update LoRa, crack geometry and calibration.
 *        Validates inputs and persists to NVS; restarts MCU after changes.
 */
void api_cfg_lora() {
  if (!http.hasArg("plain")) {
    http.send(400, "text/plain", "bad json");
    return;
  }
  StaticJsonDocument<800> d;
  if (deserializeJson(d, http.arg("plain"))) {
    http.send(400, "text/plain", "bad json");
    return;
  }

  bool ok = true;
  if (d.containsKey("devEui_lsb")) ok &= parseHexLSB((const char*)d["devEui_lsb"], cfg.devEui, 8);
  if (d.containsKey("appEui_lsb")) ok &= parseHexLSB((const char*)d["appEui_lsb"], cfg.appEui, 8);
  if (d.containsKey("appKey_msb")) ok &= parseHexLSB((const char*)d["appKey_msb"], cfg.appKey, 16);

  if (d.containsKey("minutes")) {
    cfg.minutes = d["minutes"].as<uint32_t>();
    prefs.begin("uhfb", false);
    prefs.putUInt("minutes", cfg.minutes);
    prefs.end();
  }

  if (d.containsKey("crack_len_mm")) {
    float L = d["crack_len_mm"].as<float>();
    if (!isfinite(L) || L < 0.01f || L > 1000.0f) {
      http.send(400, "text/plain", "len range");
      return;
    }
    cfg.crack_len_mm_x100 = (uint16_t)lrintf(L * 100.0f);
    if (cfg.crack_mm1_x100 < cfg.crack_len_mm_x100) cfg.crack_mm1_x100 = cfg.crack_len_mm_x100;
    prefs.begin("uhfb", false);
    prefs.putUShort("cr_len_x100", cfg.crack_len_mm_x100);
    prefs.putUShort("cr_mm1", cfg.crack_mm1_x100);
    prefs.end();
  }

  if (d.containsKey("crack_cal")) {
    JsonObject cal = d["crack_cal"].as<JsonObject>();
    bool okc = true;
    if (cal.containsKey("r0")) cfg.crack_r0 = cal["r0"].as<int>();
    if (cal.containsKey("r1")) cfg.crack_r1 = cal["r1"].as<int>();
    if (cal.containsKey("mm0")) {
      float v = cal["mm0"].as<float>();
      okc &= isfinite(v) && v >= 0 && v < 1000.0f;
      if (okc) cfg.crack_mm0_x100 = (uint16_t)lrintf(v * 100.0f);
    }
    if (cal.containsKey("mm1")) {
      float v = cal["mm1"].as<float>();
      okc &= isfinite(v) && v >= 0 && v <= 1000.0f;
      if (okc) cfg.crack_mm1_x100 = (uint16_t)lrintf(v * 100.0f);
    }
    if (!okc || cfg.crack_r0 == cfg.crack_r1) {
      http.send(400, "text/plain", "cal error");
      return;
    }
    prefs.begin("uhfb", false);
    prefs.putShort("cr_r0", cfg.crack_r0);
    prefs.putShort("cr_r1", cfg.crack_r1);
    prefs.putUShort("cr_mm0", cfg.crack_mm0_x100);
    prefs.putUShort("cr_mm1", cfg.crack_mm1_x100);
    prefs.end();
  }

  if (d.containsKey("dr")) {
    int v = d["dr"].as<int>();
    if (v < 0 || v > 5) {
      http.send(400, "text/plain", "dr range");
      return;
    }
    cfg.lora_dr = (uint8_t)v;
    cfg.lora_adr = false;
    prefs.begin("uhfb", false);
    prefs.putUChar("lora_dr", cfg.lora_dr);
    prefs.putBool("lora_adr", cfg.lora_adr);
    prefs.end();
  }
  if (d.containsKey("adr")) {
    cfg.lora_adr = d["adr"].as<bool>();
    prefs.begin("uhfb", false);
    prefs.putBool("lora_adr", cfg.lora_adr);
    prefs.end();
    loraWanAdr = cfg.lora_adr;
  }

  if (d.containsKey("time_sync")) {
    JsonObject ts = d["time_sync"].as<JsonObject>();
    if (ts.containsKey("enabled")) { cfg.ts_enable = ts["enabled"].as<bool>(); }
    if (ts.containsKey("hours")) {
      int h = ts["hours"].as<int>();
      if (h < 1 || h > 168) {
        http.send(400, "text/plain", "ts hours range");
        return;
      }
      cfg.ts_hours = (uint16_t)h;
    }
    prefs.begin("uhfb", false);
    prefs.putBool("ts_en", cfg.ts_enable);
    prefs.putUShort("ts_h", cfg.ts_hours);
    prefs.end();
  }

  if (d.containsKey("ref")) {
    JsonObject rj = d["ref"].as<JsonObject>();
    String m = rj["mode"] | "auto";
    cfg.ref_mode = (m == "manual") ? REF_MANUAL : REF_AUTO;
    prefs.begin("uhfb", false);
    prefs.putUChar("ref_mode", cfg.ref_mode);
    prefs.end();
  }

  if (!ok) {
    http.send(400, "text/plain", "hex error");
    return;
  }
  nvsPut("devEui", cfg.devEui, 8);
  nvsPut("appEui", cfg.appEui, 8);
  nvsPut("appKey", cfg.appKey, 16);

  http.send(200, "text/plain", "ok");
  delay(200);
  ESP.restart();
}

/**
 * @brief GET /api/deveui — report chip and stored DevEUIs.
 */
void api_deveui_get() {
  uint8_t chipMSB[8];
  devEuiFromChipMSB(chipMSB);
  uint8_t chipLSB[8];
  for (int i = 0; i < 8; i++) chipLSB[i] = chipMSB[7 - i];
  StaticJsonDocument<256> d;
  d["chip_devEui_msb"] = toHex(chipMSB, 8);
  d["chip_devEui_lsb"] = toHex(chipLSB, 8);
  d["stored_devEui_lsb"] = toHex(cfg.devEui, 8);
  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}

/**
 * @brief POST /api/deveui — set DevEUI to chip or explicit value.
 */
void api_deveui_post() {
  if (!http.hasArg("plain")) {
    http.send(400, "text/plain", "bad json");
    return;
  }
  StaticJsonDocument<128> d;
  if (deserializeJson(d, http.arg("plain"))) {
    http.send(400, "text/plain", "bad json");
    return;
  }
  if (d.containsKey("use") && String((const char*)d["use"]) == "chip") {
    uint8_t chipMSB[8];
    devEuiFromChipMSB(chipMSB);
    for (int i = 0; i < 8; i++) cfg.devEui[i] = chipMSB[7 - i];
    nvsPut("devEui", cfg.devEui, 8);
    http.send(200, "text/plain", "ok");
    delay(200);
    ESP.restart();
    return;
  }
  if (d.containsKey("devEui_lsb")) {
    String h = d["devEui_lsb"].as<String>();
    if (!parseHexLSB(h, cfg.devEui, 8)) {
      http.send(400, "text/plain", "hex error");
      return;
    }
    nvsPut("devEui", cfg.devEui, 8);
    http.send(200, "text/plain", "ok");
    delay(200);
    ESP.restart();
    return;
  }
  http.send(400, "text/plain", "args");
}

/**
 * @brief Attach HTTP routes and start WebServer; serves SPA index.html on 404.
 */
void attachHttpFS() {
  http.on("/", []() {
    if (!streamFileFS("/index.html")) http.send(404, "text/plain", "index missing");
  });
  http.on("/logo.svg", []() {
    if (!streamFileFS("/logo.svg")) http.send(404, "text/plain", "logo missing");
  });
  http.on("/i18n.json", []() {
    if (!streamFileFS("/i18n.json")) http.send(404, "text/plain", "i18n missing");
  });
  http.on("/api/state", api_state);
  http.on("/api/send", HTTP_POST, api_send);
  http.on("/api/reset", HTTP_POST, api_reset);
  http.on("/api/config", api_cfg_get);
  http.on("/api/config/lora", HTTP_POST, api_cfg_lora);
  http.on("/api/deveui", HTTP_GET, api_deveui_get);
  http.on("/api/deveui", HTTP_POST, api_deveui_post);
  http.onNotFound([]() {
    if (!streamFileFS("/index.html")) http.send(404, "text/plain", "404");
  });
  http.begin();
}

// ========================= Uplink payload (8B) ===================== //
/**
 * @brief Prepare 8-byte payload: T, ADCraw, crack*1000, VBAT.
 *        Layout: 4x uint16 big-endian in appData[0..7].
 */
void prepareTxFrame(uint8_t) {
  sensorsPowerOn();
  CrackMeas cr = readCrack();
  const int16_t t = readTemp_c_x100();  // 0..10000
  const uint16_t vb = readBattery_mV();
  const uint16_t cr1000 = (uint16_t)lrintf(max(0.0f, min(cr.mm, 65.535f)) * 1000.0f);
  const uint16_t adcRaw = cr.present ? (uint16_t)cr.raw0 : 0;
  sensorsPowerOff();

  appDataSize = 8;  // 4 * uint16
  appData[0] = (uint16_t)t >> 8;
  appData[1] = (uint16_t)t;
  appData[2] = adcRaw >> 8;
  appData[3] = adcRaw;
  appData[4] = cr1000 >> 8;
  appData[5] = cr1000;
  appData[6] = vb >> 8;
  appData[7] = vb;
  Serial.printf("TX 8B: T=%d ADC=%u CR=%u VB=%u  HEX=%02X%02X %02X%02X %02X%02X %02X%02X\n",
                (int)t, adcRaw, cr1000, vb,
                appData[0], appData[1], appData[2], appData[3], appData[4], appData[5], appData[6], appData[7]);
}

// ============================== App ================================ //
enum OledPhase { OLED_LOGO,
                 OLED_SSID,
                 OLED_SENS };
OledPhase oledPhase = OLED_LOGO;
uint32_t oledPhaseStart = 0;

/**
 * @brief Application setup: IOs, config, LoRa keys, Wi-Fi/AP, OLED.
 *        Note: Do not force deviceState=DEVICE_STATE_INIT here to avoid re-join loops.
 */
void setup() {
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  pinMode(PIN_MODE_OUT, OUTPUT);
  digitalWrite(PIN_MODE_OUT, LOW);
  pinMode(PIN_MODE_IN, INPUT_PULLUP);
  delay(5);
  pinMode(PIN_CRACK_PRESENT, INPUT_PULLUP);

  Serial.printf("FW %s build %s %s\n", FW_VER, __DATE__, __TIME__);

  for (int i = 0; i < 5; i++) {
    apMode |= (digitalRead(PIN_MODE_IN) == LOW);
    delay(2);
  }

  analogReadResolution(12);
  loadCfg();

  memcpy(devEui, cfg.devEui, 8);
  memcpy(appEui, cfg.appEui, 8);
  memcpy(appKey, cfg.appKey, 16);
  memcpy(DevEui, devEui, 8);
  memcpy(AppEui, appEui, 8);
  memcpy(AppKey, appKey, 16);

  uint8_t chipMSB[8];
  devEuiFromChipMSB(chipMSB);
  uint8_t chipLSB[8];
  for (int i = 0; i < 8; i++) chipLSB[i] = chipMSB[7 - i];
  Serial.printf("DevEUI LSB=%s  AppEUI LSB=%s  AppKey MSB=%s\n",
                toHex(DevEui, 8).c_str(), toHex(AppEui, 8).c_str(), toHex(AppKey, 16).c_str());
  Serial.printf("Chip-MSB=%s  Chip-LSB=%s  Stored-LSB=%s\n",
                toHex(chipMSB, 8).c_str(), toHex(chipLSB, 8).c_str(), toHex(cfg.devEui, 8).c_str());

  if (apMode) {
    // AP mode for configuration
    WiFi.mode(WIFI_AP);
    bool fsOk = LittleFS.begin(true);
    if (fsOk) attachHttpFS();
    else Serial.println("[FS] mount failed");
    WiFi.setTxPower(WIFI_POWER_7dBm);
    String ssid = "Riss-" + devEuiSuffix6LSB();
    WiFi.softAP(ssid.c_str(), AP_PSK);
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, LOW);
    oledSplash();
    oledPhase = OLED_SSID;
    oledPhaseStart = millis();
    oledShowSSID(ssid);
  } else {
    // LNS mode
    WiFi.persistent(false);
    WiFi.disconnect(true, true);
    WiFi.mode(WIFI_OFF);
#if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
    btStop();
#endif
    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, HIGH);
  }

  // IMPORTANT: Keep deviceState unchanged here to avoid forced re-join.
  // deviceState=DEVICE_STATE_INIT; // intentionally disabled to prevent double-send after sleep
}

/**
 * @brief Main loop: HTTP in AP mode, LoRaWAN state machine otherwise.
 */
void loop() {
  static uint32_t appTxDutyCycleCached = 0;
  if (appTxDutyCycleCached == 0) appTxDutyCycleCached = 60000UL * cfg.minutes;

  if (rejoinRebootRequested) {
    Serial.println("[REJOIN] rebooting to force fresh OTAA join");
    delay(100);
    ESP.restart();
  }

  if (apMode) {
    http.handleClient();
    oledMarqueeTick();
    if (oledPhase == OLED_SSID && millis() - oledPhaseStart >= 3000) {
      apScroll.active = false;
      oledPhase = OLED_SENS;
      oledPhaseStart = millis();
      oledSensorsOnce();
    }
    static uint32_t tmr = 0;
    if (oledPhase == OLED_SENS && millis() - tmr > 1000) {
      tmr = millis();
      oledSensorsOnce();
    }
  }

  switch (deviceState) {
    case DEVICE_STATE_INIT:
      // Initialize LoRaWAN and join parameters
      netMissCount = 0;
      rejoinRebootRequested = false;
      memcpy(DevEui, cfg.devEui, 8);
      memcpy(AppEui, cfg.appEui, 8);
      memcpy(AppKey, cfg.appKey, 16);
      loraWanAdr = cfg.lora_adr;
      LoRaWAN.init(CLASS_A, ACTIVE_REGION);
      applyLoraDR();
      enforceTxPolicy();  // ensure policy at init
      Serial.println("[LORA] DEVICE_STATE_INIT");
      deviceState = DEVICE_STATE_JOIN;
      break;

    case DEVICE_STATE_JOIN:
      // Start OTAA join; some stacks reset MIB around join
      Serial.println("[LORA] DEVICE_STATE_JOIN");
      LoRaWAN.join();
      enforceTxPolicy();  // re-apply policy around join
      break;

    case DEVICE_STATE_SEND:
      // Prepare and send uplink once per cycle
      enforceTxPolicy();  // enforce before each TX
      prepareTxFrame(UPLINK_PORT);
      onUplinkAttempt();
      LoRaWAN.send();
      deviceState = DEVICE_STATE_CYCLE;
      break;

    case DEVICE_STATE_CYCLE:
      {
        // Fixed interval scheduling (no minute alignment)
        const uint32_t txDutyCycleTime = 60000UL * cfg.minutes;
        LoRaWAN.cycle(txDutyCycleTime);
        deviceState = DEVICE_STATE_SLEEP;
        break;
      }

    case DEVICE_STATE_SLEEP:
      // Low-power sleep while preserving LoRa session counters
      sensorsPowerOff();
      if (apMode) LoRaWAN.sleep(CLASS_C);
      else LoRaWAN.sleep(CLASS_A);
      break;

    default: deviceState = DEVICE_STATE_INIT; break;  // safe fallback
  }
}
