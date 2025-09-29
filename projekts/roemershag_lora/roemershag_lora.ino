#include "LoRaWan_APP.h"
#ifdef CLASS
#undef CLASS
#endif
#ifdef DR
#undef DR
#endif

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

// ===== OLED =====
SSD1306Wire OLED_Display(0x3c, 400000, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);

// ===== ADS1115 на другій I²C (GPIO47/48) =====
TwoWire I2C_ADS = TwoWire(1);
Adafruit_ADS1115 ads;

// ===== Pins (Heltec V3 / ESP32-S3) =====
#define ONE_WIRE_BUS 4
#define ADC_CRACK 2  // не використовується для crack з ADS
#define VBAT_Read 1
#define ADC_Ctrl 37
#define PIN_MODE_OUT 45
#define PIN_MODE_IN 46

// Нове: детекція наявності crack-сенсора (LOW = підключено)
#define PIN_CRACK_PRES 3

// ===== Керування живленням сенсорів (ADS1115 + DS18B20) =====
#define SENS_EN 7
#define SENS_ACTIVE_HIGH 1
static inline void SensorsON() {
  pinMode(SENS_EN, OUTPUT);
  digitalWrite(SENS_EN, SENS_ACTIVE_HIGH ? HIGH : LOW);
}
static inline void SensorsOFF() {
  digitalWrite(SENS_EN, SENS_ACTIVE_HIGH ? LOW : HIGH);
  pinMode(SENS_EN, INPUT);
}

static inline bool crackPresent() {
  // Внутрішній підтяг до VCC; перемичка/датчик тягне до GND
  pinMode(PIN_CRACK_PRES, INPUT_PULLUP);
  return digitalRead(PIN_CRACK_PRES) == LOW;
}

void sensorsPowerOn() {
  SensorsON();
  delay(12);  // було 3 → 12 мс для стабілізації

  I2C_ADS.begin(47, 48, 400000);
  if (ads.begin(0x48, &I2C_ADS)) {
    ads.setGain(GAIN_ONE);
    ads.setDataRate(RATE_ADS1115_128SPS);
    (void)ads.readADC_SingleEnded(0);  // «холостий» семпл
    delay(2);
  } else {
    Serial.println("ADS1115 not found");
  }
}

void sensorsPowerOff() {
  I2C_ADS.end();
  pinMode(47, INPUT);
  pinMode(48, INPUT);
  pinMode(ONE_WIRE_BUS, INPUT);
  SensorsOFF();
}

// ===== OneWire + DallasTemperature =====
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

// DS18B20
uint8_t firstDs[8] = { 0 };
bool firstDsFound = false;
const uint8_t DS_RES_BITS = 10;  // ~187.5ms

// ===== Режим =====
bool apMode = false;

// ===== NVS конфиг =====
Preferences prefs;
struct Cfg {
  String ssid, wifi_pw;
  uint8_t devEui[8] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
  uint8_t appEui[8] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
  uint8_t appKey[16] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
  uint32_t minutes = 2;
  uint16_t crack_len_mm_x100 = 1000;

  // 2-точкова калібровка: raw r0→mm0, raw r1→mm1 (x100)
  int16_t crack_r0 = 5;
  int16_t crack_r1 = 25480;
  uint16_t crack_mm0_x100 = 0;     // 0.00 мм
  uint16_t crack_mm1_x100 = 1000;  // 10.00 мм (дефолт = crack_len_mm_x100)
} cfg;

void loadCfg() {
  prefs.begin("uhfb", true);
  cfg.minutes = prefs.getUInt("minutes", cfg.minutes);
  cfg.ssid = prefs.getString("ssid", "");
  cfg.wifi_pw = prefs.getString("wifi_pw", "");
  prefs.getBytes("devEui", cfg.devEui, 8);
  prefs.getBytes("appEui", cfg.appEui, 8);
  prefs.getBytes("appKey", cfg.appKey, 16);
  cfg.crack_len_mm_x100 = prefs.getUShort("cr_len_x100", cfg.crack_len_mm_x100);

  cfg.crack_r0 = prefs.getShort("cr_r0", cfg.crack_r0);
  cfg.crack_r1 = prefs.getShort("cr_r1", cfg.crack_r1);
  cfg.crack_mm0_x100 = prefs.getUShort("cr_mm0", cfg.crack_mm0_x100);
  cfg.crack_mm1_x100 = prefs.getUShort("cr_mm1", cfg.crack_mm1_x100);
  if (cfg.crack_mm1_x100 == 1000) cfg.crack_mm1_x100 = cfg.crack_len_mm_x100;
  prefs.end();
}
void saveKVu(const char* k, uint32_t v) {
  prefs.begin("uhfb", false);
  prefs.putUInt(k, v);
  prefs.end();
}
void saveUShort(const char* k, uint16_t v) {
  prefs.begin("uhfb", false);
  prefs.putUShort(k, v);
  prefs.end();
}
void saveShort(const char* k, int16_t v) {
  prefs.begin("uhfb", false);
  prefs.putShort(k, v);
  prefs.end();
}
void saveBytes(const char* k, const uint8_t* d, size_t n) {
  prefs.begin("uhfb", false);
  prefs.putBytes(k, d, n);
  prefs.end();
}

// ===== LoRaWAN (глобалы для Heltec LoRaWan_APP) =====
uint16_t userChannelsMask[6] = { 0x00FF, 0, 0, 0, 0, 0 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;  // REGION_EU868 у Tools
DeviceClass_t loraWanClass = CLASS_A;
bool overTheAirActivation = true;
bool loraWanAdr = true;
bool isTxConfirmed = false;
uint8_t appPort = 2;
uint8_t confirmedNbTrials = 1;
uint32_t appTxDutyCycle = 60000UL * 2;

// <<< глобальные символы для линковки >>>
uint8_t devEui[8] = { 0 };
uint8_t appEui[8] = { 0 };
uint8_t appKey[16] = { 0 };
uint8_t nwkSKey[16] = { 0 };
uint8_t appSKey[16] = { 0 };
uint32_t devAddr = 0;

// ===== Утиліти =====
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
String toHexLSB(const uint8_t* v, size_t n) {
  char b[3];
  String s;
  s.reserve(n * 2);
  for (int i = n - 1; i >= 0; i--) {
    sprintf(b, "%02X", v[i]);
    s += b;
  }
  return s;
}
bool parseHex(const String& s, uint8_t* out, size_t n) {
  if (s.length() != n * 2) return false;
  for (size_t i = 0; i < n; i++) {
    char b[3] = { (char)s[2 * i], (char)s[2 * i + 1], 0 };
    out[i] = (uint8_t)strtoul(b, nullptr, 16);
  }
  return true;
}
void devEuiFromChip(uint8_t out[8]) {
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
String devEuiSuffix6() {
  char buf[7];
  sprintf(buf, "%02X%02X%02X", cfg.devEui[5], cfg.devEui[6], cfg.devEui[7]);
  return String(buf);
}

// ===== DS18B20 =====
bool dsFindFirst(uint8_t addrOut[8]) {
  if (sensors.getAddress(addrOut, 0)) return true;
  OneWire ow(ONE_WIRE_BUS);
  uint8_t addr[8];
  ow.reset_search();
  while (ow.search(addr)) {
    if (addr[0] == 0x28) {
      memcpy(addrOut, addr, 8);
      return true;
    }
  }
  return false;
}

// ===== Калібрування crack: raw -> мм*100 =====
static inline uint16_t mapCrackRawToMMx100(int raw) {
  const int32_t r0 = cfg.crack_r0;
  const int32_t r1 = cfg.crack_r1;
  const int32_t y0 = cfg.crack_mm0_x100;
  const int32_t y1 = cfg.crack_mm1_x100;
  int32_t den = (r1 - r0);
  if (den == 0) return 0;
  int64_t num = (int64_t)(y1 - y0) * (int64_t)(raw - r0);
  int64_t y = (int64_t)y0 + (num + (den > 0 ? den / 2 : -den / 2)) / den;  // з округленням
  if (y < 0) y = 0;
  if (y > 65535) y = 65535;
  return (uint16_t)y;
}

// НОВЕ: float-маппер для точного відображення в тисячних
static inline float mapCrackRawToMM_f(int raw) {
  const float r0 = (float)cfg.crack_r0;
  const float r1 = (float)cfg.crack_r1;
  const float mm0 = (float)cfg.crack_mm0_x100 / 100.0f;
  const float mm1 = (float)cfg.crack_mm1_x100 / 100.0f;
  if (r1 == r0) return 0.0f;
  float mm = mm0 + (mm1 - mm0) * ((float)raw - r0) / (r1 - r0);
  if (mm < 0) mm = 0;
  return mm;
}

// ===== Вимірювання =====
uint16_t readBattery_mV() {
  const float factor = 1.0f / 4096.0f / 0.210282997855762f;  // R1=390k, R2=100k
  analogSetAttenuation(ADC_0db);
  pinMode(ADC_Ctrl, OUTPUT);
  digitalWrite(ADC_Ctrl, HIGH);
  delay(2);
  int raw = analogRead(VBAT_Read);
  digitalWrite(ADC_Ctrl, LOW);
  return (uint16_t)(factor * raw * 1000.0f);
}

// Crack через ADS1115 AIN0 з 2-точковим мапінгом (x100 для сумісності)
uint16_t readCrack_mm_x100(int* rawOut) {
  if (!crackPresent()) {
    if (rawOut) *rawOut = 0;
    return 0;
  }
  int16_t raw = ads.readADC_SingleEnded(0);
  if (raw < 0) raw = 0;  // однополярний сенсор
  if (rawOut) *rawOut = raw;
  return mapCrackRawToMMx100(raw);
}

// Температура для пейлоада
int16_t readTemp_c_x100() {
  if (!firstDsFound) return 5000;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200);
  float tC = sensors.getTempC(firstDs);
  if (tC <= -127.0f || tC >= 125.0f) return 5000;
  int v = (int)(tC * 100.0f) + 5000;
  if (v < 0) v = 0;
  return (int16_t)v;
}

// Разове читання для UI
float readTempOnceC() {
  if (!firstDsFound) return NAN;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200);
  float tC = sensors.getTempC(firstDs);
  if (tC <= -127.0f || tC >= 125.0f) return NAN;
  return tC;
}

// ===== Payload =====
static void prepareTxFrame(uint8_t) {
  sensorsPowerOn();
  if (!firstDsFound) {
    sensors.begin();
    firstDsFound = dsFindFirst(firstDs);
    if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
  } else {
    sensors.setResolution(firstDs, DS_RES_BITS);
  }

  bool present = crackPresent();
  int16_t adcRaw = present ? ads.readADC_SingleEnded(0) : 0;
  if (adcRaw < 0) adcRaw = 0;

  int16_t t = readTemp_c_x100();
  uint16_t vb = readBattery_mV();

  // crack у тисячних (x1000) у 2 байти → макс 65.535 мм
  float mm = present ? mapCrackRawToMM_f(adcRaw) : 0.0f;
  if (mm < 0) mm = 0;
  if (mm > 65.535f) mm = 65.535f;
  uint16_t cr1000 = (uint16_t)lrintf(mm * 1000.0f);

  sensorsPowerOff();

  appDataSize = 8;
  appData[0] = t >> 8;
  appData[1] = t;
  appData[2] = adcRaw >> 8;
  appData[3] = adcRaw;
  appData[4] = cr1000 >> 8;
  appData[5] = cr1000;  // Crack x1000 (0, якщо датчика немає)
  appData[6] = vb >> 8;
  appData[7] = vb;
}

// ===== Web (AP) =====
WebServer http(80);

String ipStr() {
  if (WiFi.getMode() == WIFI_AP) return WiFi.softAPIP().toString();
  if (WiFi.status() == WL_CONNECTED) return WiFi.localIP().toString();
  return "-";
}
String contentTypeFor(const String& path) {
  if (path.endsWith(".html")) return "text/html; charset=utf-8";
  if (path.endsWith(".svg")) return "image/svg+xml";
  if (path.endsWith(".json")) return "application/json; charset=utf-8";
  if (path.endsWith(".css")) return "text/css";
  if (path.endsWith(".js")) return "application/javascript";
  return "application/octet-stream";
}
bool streamFileFS(const char* p) {
  if (!LittleFS.exists(p)) return false;
  File f = LittleFS.open(p, "r");
  if (!f) return false;
  http.streamFile(f, contentTypeFor(p));
  f.close();
  return true;
}

void api_state() {
  StaticJsonDocument<1184> d;
  d["mode"] = (WiFi.getMode() == WIFI_AP) ? "AP" : "LNS";
  d["ip"] = ipStr();
  d["uptime_s"] = (uint32_t)(millis() / 1000);
  d["heap_free"] = ESP.getFreeHeap();

  sensorsPowerOn();
  if (!firstDsFound) {
    sensors.begin();
    firstDsFound = dsFindFirst(firstDs);
    if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
  } else {
    sensors.setResolution(firstDs, DS_RES_BITS);
  }

  bool present = crackPresent();
  int adcRaw = 0;
  uint16_t crack = present ? readCrack_mm_x100(&adcRaw) : 0;
  uint16_t batmV = readBattery_mV();
  float tC = readTempOnceC();
  float crack_mm = present ? mapCrackRawToMM_f(adcRaw) : 0.0f;

  sensorsPowerOff();

  d["crack_present"] = present;
  d["adc_raw"] = present ? adcRaw : 0;
  if (present) {
    d["crack_x100"] = crack;
    d["crack_mm"]   = crack_mm;
  } else {
    d["crack_x100"] = nullptr;
    d["crack_mm"]   = nullptr;
  }
  d["batt_mV"] = batmV;
  if (isfinite(tC)) d["DS18B20_Temp"] = tC;
  else d["DS18B20_Temp"] = nullptr;

  JsonObject c = d.createNestedObject("cfg");
  c["minutes"] = cfg.minutes;
  c["devEui"] = toHex(cfg.devEui, 8);
  c["appEui"] = toHex(cfg.appEui, 8);
  c["appKey"] = toHex(cfg.appKey, 16);
  c["crack_len_x100"] = cfg.crack_len_mm_x100;
  JsonObject cal = c.createNestedObject("crack_cal");
  cal["r0"] = cfg.crack_r0;
  cal["mm0"] = (float)cfg.crack_mm0_x100 / 100.0f;
  cal["r1"] = cfg.crack_r1;
  cal["mm1"] = (float)cfg.crack_mm1_x100 / 100.0f;

  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}

void api_send() {
  deviceState = DEVICE_STATE_SEND;
  http.send(200, "text/plain", "queued");
}
void api_reset() {
  http.send(200, "text/plain", "restarting");
  delay(200);
  ESP.restart();
}

void api_cfg_get() {
  StaticJsonDocument<672> d;
  d["devEui"] = toHex(cfg.devEui, 8);
  d["appEui"] = toHex(cfg.appEui, 8);
  d["appKey"] = toHex(cfg.appKey, 16);
  d["minutes"] = cfg.minutes;
  d["crack_len_x100"] = cfg.crack_len_mm_x100;
  JsonObject cal = d.createNestedObject("crack_cal");
  cal["r0"] = cfg.crack_r0;
  cal["mm0"] = (float)cfg.crack_mm0_x100 / 100.0f;
  cal["r1"] = cfg.crack_r1;
  cal["mm1"] = (float)cfg.crack_mm1_x100 / 100.0f;

  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}

void api_cfg_lora() {
  if (!http.hasArg("plain")) {
    http.send(400, "text/plain", "bad json");
    return;
  }
  StaticJsonDocument<384> d;
  if (deserializeJson(d, http.arg("plain"))) {
    http.send(400, "text/plain", "bad json");
    return;
  }
  bool ok = true;

  if (d.containsKey("devEui")) ok &= parseHex((const char*)d["devEui"], cfg.devEui, 8);
  if (d.containsKey("appEui")) ok &= parseHex((const char*)d["appEui"], cfg.appEui, 8);
  if (d.containsKey("appKey")) ok &= parseHex((const char*)d["appKey"], cfg.appKey, 16);

  if (d.containsKey("minutes")) {
    cfg.minutes = d["minutes"].as<uint32_t>();
    saveKVu("minutes", cfg.minutes);
  }

  if (d.containsKey("crack_len_mm")) {
    float L = d["crack_len_mm"].as<float>();
    if (isfinite(L) && L > 0.01f && L < 1000.0f) {
      uint16_t Lx100 = (uint16_t)(L * 100.0f + 0.5f);
      cfg.crack_len_mm_x100 = Lx100;
      if (cfg.crack_mm1_x100 < Lx100) cfg.crack_mm1_x100 = Lx100;
      saveUShort("cr_len_x100", cfg.crack_len_mm_x100);
    } else {
      http.send(400, "text/plain", "len range");
      return;
    }
  }

  if (d.containsKey("crack_cal")) {
    JsonObject cal = d["crack_cal"].as<JsonObject>();
    bool okc = true;
    if (cal.containsKey("r0")) cfg.crack_r0 = cal["r0"].as<int>();
    if (cal.containsKey("r1")) cfg.crack_r1 = cal["r1"].as<int>();
    if (cal.containsKey("mm0")) {
      float mm0 = cal["mm0"].as<float>();
      if (isfinite(mm0) && mm0 >= 0 && mm0 < 1000.0f) cfg.crack_mm0_x100 = (uint16_t)lrintf(mm0 * 100.0f);
      else okc = false;
    }
    if (cal.containsKey("mm1")) {
      float mm1 = cal["mm1"].as<float>();
      if (isfinite(mm1) && mm1 >= 0 && mm1 <= 1000.0f) cfg.crack_mm1_x100 = (uint16_t)lrintf(mm1 * 100.0f);
      else okc = false;
    }
    if (!okc || cfg.crack_r0 == cfg.crack_r1) {
      http.send(400, "text/plain", "cal error");
      return;
    }

    saveShort("cr_r0", cfg.crack_r0);
    saveShort("cr_r1", cfg.crack_r1);
    saveUShort("cr_mm0", cfg.crack_mm0_x100);
    saveUShort("cr_mm1", cfg.crack_mm1_x100);
  }

  if (!ok) {
    http.send(400, "text/plain", "hex error");
    return;
  }
  saveBytes("devEui", cfg.devEui, 8);
  saveBytes("appEui", cfg.appEui, 8);
  saveBytes("appKey", cfg.appKey, 16);

  http.send(200, "text/plain", "ok");
  delay(200);
  ESP.restart();
}

void api_deveui_get() {
  uint8_t chip[8];
  devEuiFromChip(chip);
  StaticJsonDocument<256> d;
  d["chip_devEui_msb"] = toHex(chip, 8);
  d["chip_devEui_lsb"] = toHexLSB(chip, 8);
  d["stored_devEui_msb"] = toHex(cfg.devEui, 8);
  String out;
  serializeJson(d, out);
  http.send(200, "application/json", out);
}
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
    devEuiFromChip(cfg.devEui);
    saveBytes("devEui", cfg.devEui, 8);
    http.send(200, "text/plain", "ok");
    delay(200);
    ESP.restart();
    return;
  }
  if (d.containsKey("devEui")) {
    String h = d["devEui"].as<String>();
    if (!parseHex(h, cfg.devEui, 8)) {
      http.send(400, "text/plain", "hex error");
      return;
    }
    saveBytes("devEui", cfg.devEui, 8);
    http.send(200, "text/plain", "ok");
    delay(200);
    ESP.restart();
    return;
  }
  http.send(400, "text/plain", "args");
}

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

// ===== OLED =====
void oledBoot(const String& ssid) {
  OLED_Display.init();
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 0, "MARX KRONTAL PARTNER");
  OLED_Display.drawString(0, 14, "UHFB Carport");
  OLED_Display.drawString(0, 28, "AP: " + ssid);
  OLED_Display.display();
}
void oledSensorsOnce() {
  sensorsPowerOn();
  if (!firstDsFound) {
    sensors.begin();
    firstDsFound = dsFindFirst(firstDs);
    if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
  } else {
    sensors.setResolution(firstDs, DS_RES_BITS);
  }

  bool present = crackPresent();
  float t = readTempOnceC();
  int adcRaw = 0;
  uint16_t crack_x100 = present ? readCrack_mm_x100(&adcRaw) : 0;
  uint16_t bat = readBattery_mV();
  sensorsPowerOff();

  float crack_mm = present ? mapCrackRawToMM_f(adcRaw) : NAN;

  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);

  OLED_Display.setColor(BLACK);
  OLED_Display.fillRect(0, 14, 128, 50);
  OLED_Display.setColor(WHITE);

  if (isfinite(t)) OLED_Display.drawString(0, 14, String("DS18B20  ") + String(t, 1) + " C");
  else OLED_Display.drawString(0, 14, "DS18B20  n/a");
  OLED_Display.drawString(0, 26, String("ADC      ") + (present ? String(adcRaw) : String("N/C")));
  if (present) OLED_Display.drawString(0, 38, String("Crack    ") + String(crack_mm, 3) + " mm");
  else OLED_Display.drawString(0, 38, "Crack    N/C");
  OLED_Display.drawString(0, 50, String("Battery  ") + String(bat) + " mV");

  OLED_Display.display();
}

void setup() {
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  pinMode(PIN_MODE_OUT, OUTPUT);
  digitalWrite(PIN_MODE_OUT, LOW);
  pinMode(PIN_MODE_IN, INPUT_PULLUP);
  delay(5);
  pinMode(PIN_CRACK_PRES, INPUT_PULLUP);
  for (int i = 0; i < 5; i++) {
    apMode |= (digitalRead(PIN_MODE_IN) == LOW);
    delay(2);
  }

  analogReadResolution(12);
  loadCfg();
  appTxDutyCycle = 60000UL * cfg.minutes;

  memcpy(devEui, cfg.devEui, sizeof(devEui));
  memcpy(appEui, cfg.appEui, sizeof(appEui));
  memcpy(appKey, cfg.appKey, sizeof(appKey));

  // I2C для OLED
  Wire.begin(SDA_OLED, SCL_OLED, 400000);

  // Ініціалізація сенсорів з живленням
  sensorsPowerOn();
  sensors.begin();
  firstDsFound = dsFindFirst(firstDs);
  if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
  sensorsPowerOff();

  if (apMode) {
    WiFi.mode(WIFI_AP);
    String ssid = "Rissmonitoring-" + devEuiSuffix6();

    bool fsOk = LittleFS.begin(true);
    if (!fsOk) {
      Serial.println("[FS] LittleFS mount failed even after format]");
    } else {
      attachHttpFS();
    }

    WiFi.setTxPower(WIFI_POWER_7dBm);
    WiFi.softAP(ssid.c_str(), "12345678");

    pinMode(Vext, OUTPUT);
    digitalWrite(Vext, LOW);

    OLED_Display.init();
    oledBoot(ssid);
    delay(500);
    oledSensorsOnce();
  } else {
    WiFi.mode(WIFI_OFF);
#if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
    btStop();
#endif
    digitalWrite(Vext, HIGH);
  }

  deviceState = DEVICE_STATE_INIT;
}

void loop() {
  if (apMode) {
    http.handleClient();
    static uint32_t tmr = 0;
    if (millis() - tmr > 5000) {
      tmr = millis();
      oledSensorsOnce();
    }
  }

  switch (deviceState) {
    case DEVICE_STATE_INIT:
      LoRaWAN.init(loraWanClass, loraWanRegion);
      LoRaWAN.setDefaultDR(DR_3);
      deviceState = overTheAirActivation ? DEVICE_STATE_JOIN : DEVICE_STATE_SEND;
      break;

    case DEVICE_STATE_JOIN:
      LoRaWAN.join();
      break;

    case DEVICE_STATE_SEND:
      prepareTxFrame(appPort);
      LoRaWAN.send();
      deviceState = DEVICE_STATE_CYCLE;
      break;

    case DEVICE_STATE_CYCLE:
      txDutyCycleTime = appTxDutyCycle + randr(-APP_TX_DUTYCYCLE_RND, APP_TX_DUTYCYCLE_RND);
      LoRaWAN.cycle(txDutyCycleTime);
      deviceState = DEVICE_STATE_SLEEP;
      break;

    case DEVICE_STATE_SLEEP:
      sensorsPowerOff();
      if (apMode) {
        LoRaWAN.sleep(CLASS_C);
      } else {
        LoRaWAN.sleep(loraWanClass);
      }
      break;

    default:
      deviceState = DEVICE_STATE_INIT;
      break;
  }
}
