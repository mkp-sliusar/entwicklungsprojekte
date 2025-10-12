/*
 * =====================================================================
 *  Heltec ESP32-S3 (LoRa V3) Crack Monitoring Node — ratio (CH0/CH1)
 * ---------------------------------------------------------------------
 *  - ADS1115 (I2C1)
 *  - DS18B20 (OneWire)
 *  - SSD1306 OLED
 *  - AP config page (LittleFS)
 *  - LoRaWAN uplink 8 bytes (compatible format)
 *  - Crack length by ratio: L = Lfull * CH0 / CH1
 *    CH0 = sensor (pot), CH1 = reference from same Vee
 *    If CH1 == 0 → linear map fallback
 * =====================================================================
 *  Version:   1.2.2
 *  Date:      2025-10-10
 *  Contributors: Bernd Schinköthe, Roman Sliusar, Evgenij Koloda
 * =====================================================================
 */

#include "LoRaWan_APP.h"
#ifdef CLASS
#undef CLASS
#endif
#ifdef DR
#undef DR
#endif

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

// ---------- Constants ----------
static constexpr uint8_t  OLED_ADDR            = 0x3C;
static constexpr int      I2C_ADS_SDA          = 47;
static constexpr int      I2C_ADS_SCL          = 48;
static constexpr uint32_t I2C_FREQ_HZ          = 400000;

static constexpr int PIN_ONE_WIRE              = 4;
static constexpr int PIN_ADC_CRACK_UNUSED      = 2;
static constexpr int PIN_VBAT_ADC              = 1;
static constexpr int PIN_ADC_CTRL              = 37;
static constexpr int PIN_MODE_OUT              = 45;
static constexpr int PIN_MODE_IN               = 46;
static constexpr int PIN_CRACK_PRESENT         = 3;

static constexpr int  PIN_SENS_EN              = 7;
static constexpr bool SENS_ACTIVE_HIGH         = true;

static constexpr uint8_t  LORAWAN_DR_DEFAULT   = 3;
static constexpr bool     LORAWAN_ADR_DEFAULT  = true;

static constexpr uint8_t  ADS1115_ADDR         = 0x48;
static constexpr uint8_t  DS_RES_BITS          = 10;

static constexpr float VBAT_ADC_FACTOR         = 1.0f/4096.0f/0.210282997855762f;

static constexpr char   AP_PSK[]               = "12345678";
static constexpr size_t JSON_STATE_DOC         = 1500;
static constexpr size_t JSON_CFG_DOC           = 800;

static constexpr uint32_t MS_PER_SEC           = 1000UL;
static constexpr uint32_t SEC_PER_MIN          = 60UL;

static constexpr uint32_t MARQUEE_PERIOD_MS    = 40;

static constexpr uint8_t  UPLINK_PORT          = 2;
static constexpr uint8_t  CONF_TRIALS_AP       = 3;
static constexpr uint8_t  CONF_TRIALS_FIELD    = 1;

static constexpr uint8_t  RATIO_SAMPLES        = 8;   // averaging

// ---------- Time stubs ----------
static inline bool     sysTimeLooksValid()     { return false; }
static inline uint32_t now_sec()               { return millis()/MS_PER_SEC; }
static inline bool     loraRequestDeviceTime() { return false; }

// ---- fix for Arduino auto-prototype ----
struct CrackRatio;              // fwd
CrackRatio readCrack_ratio();   // fwd

// ---------- OLED ----------
SSD1306Wire OLED_Display(OLED_ADDR, I2C_FREQ_HZ, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);

// ---------- I2C + sensors ----------
TwoWire I2C_ADS = TwoWire(1);
Adafruit_ADS1115 ads;

OneWire oneWire(PIN_ONE_WIRE);
DallasTemperature sensors(&oneWire);
uint8_t firstDs[8] = {0};
bool firstDsFound = false;

// ---------- Mode ----------
bool apMode = false;

// ---------- NVS ----------
Preferences prefs;

struct Cfg {
  String ssid, wifi_pw;
  uint8_t devEui[8]  = {0};   // stored LSB (LoRaMac expects LSB)
  uint8_t appEui[8]  = {0};   // stored LSB
  uint8_t appKey[16] = {0};   // stored MSB
  uint32_t minutes   = 15;

  // Full travel for ratio mode
  uint16_t crack_len_mm_x100 = 1000;  // 10.00 mm

  // Linear fallback calibration
  int16_t  crack_r0          = 5;
  int16_t  crack_r1          = 25480;
  uint16_t crack_mm0_x100    = 0;
  uint16_t crack_mm1_x100    = 1000;

  uint8_t  lora_dr  = LORAWAN_DR_DEFAULT;
  bool     lora_adr = LORAWAN_ADR_DEFAULT;

  bool     ts_enable    = false;
  uint16_t ts_hours     = 24;
  bool     align_minute = true;
} cfg;

void saveKVu(const char* k, uint32_t v){ prefs.begin("uhfb", false); prefs.putUInt(k,v);    prefs.end(); }
void saveUShort(const char* k, uint16_t v){ prefs.begin("uhfb", false); prefs.putUShort(k,v); prefs.end(); }
void saveShort(const char* k, int16_t v){ prefs.begin("uhfb", false); prefs.putShort(k,v);  prefs.end(); }
void saveBytes(const char* k, const uint8_t* d, size_t n){ prefs.begin("uhfb", false); prefs.putBytes(k,d,n); prefs.end(); }
void saveUChar(const char* k, uint8_t v){ prefs.begin("uhfb", false); prefs.putUChar(k,v);  prefs.end(); }
void saveBool(const char* k, bool v){ prefs.begin("uhfb", false); prefs.putBool(k,v);      prefs.end(); }

void loadCfg() {
  prefs.begin("uhfb", true);
  cfg.minutes = prefs.getUInt("minutes", cfg.minutes);
  cfg.ssid    = prefs.getString("ssid", "");
  cfg.wifi_pw = prefs.getString("wifi_pw", "");
  prefs.getBytes("devEui", cfg.devEui, 8);
  prefs.getBytes("appEui", cfg.appEui, 8);
  prefs.getBytes("appKey", cfg.appKey, 16);

  cfg.crack_len_mm_x100 = prefs.getUShort("cr_len_x100", cfg.crack_len_mm_x100);
  cfg.crack_r0          = prefs.getShort ("cr_r0",       cfg.crack_r0);
  cfg.crack_r1          = prefs.getShort ("cr_r1",       cfg.crack_r1);
  cfg.crack_mm0_x100    = prefs.getUShort("cr_mm0",      cfg.crack_mm0_x100);
  cfg.crack_mm1_x100    = prefs.getUShort("cr_mm1",      cfg.crack_mm1_x100);
  if (cfg.crack_mm1_x100 == 1000) cfg.crack_mm1_x100 = cfg.crack_len_mm_x100;

  cfg.lora_dr  = prefs.getUChar ("lora_dr",  cfg.lora_dr);
  cfg.lora_adr = prefs.getBool  ("lora_adr", cfg.lora_adr);

  cfg.ts_enable    = prefs.getBool  ("ts_en",     cfg.ts_enable);
  cfg.ts_hours     = prefs.getUShort("ts_h",      cfg.ts_hours);
  cfg.align_minute = prefs.getBool  ("align_min", cfg.align_minute);
  prefs.end();
}

// ---------- LoRaWAN globals ----------
uint16_t        userChannelsMask[6] = { 0x00FF, 0, 0, 0, 0, 0 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t   loraWanClass  = CLASS_A;
bool overTheAirActivation = true;
bool loraWanAdr = true;
bool isTxConfirmed = false;
uint8_t  appPort = UPLINK_PORT;
uint8_t  confirmedNbTrials = 1;
uint32_t appTxDutyCycle = 60000UL * 2;

uint8_t  devEui[8]  = {0};   // passed into LoRaWAN (LSB)
uint8_t  appEui[8]  = {0};   // (LSB)
uint8_t  appKey[16] = {0};   // (MSB)
uint8_t  nwkSKey[16]= {0};
uint8_t  appSKey[16]= {0};
uint32_t devAddr    = 0;

// ---------- Link metrics ----------
volatile bool     lora_has_rx     = false;
volatile int16_t  lora_last_rssi  = 0;
volatile int8_t   lora_last_snr   = 0;
volatile uint32_t lora_last_rx_ms = 0;

extern "C" void downLinkDataHandle(McpsIndication_t *mcpsIndication) {
  lora_has_rx     = true;
  lora_last_rssi  = mcpsIndication->Rssi;
  lora_last_snr   = mcpsIndication->Snr;
  lora_last_rx_ms = millis();
}

// ---------- OLED marquee ----------
struct OledMarquee {
  String text; int y = 28; int w = 0; int x = 0; bool active = false; uint32_t last = 0;
} apScroll;

void oledMarqueeTick() {
  if (!apScroll.active) return;
  const uint32_t now = millis();
  if (now - apScroll.last < MARQUEE_PERIOD_MS) return;
  apScroll.last = now;

  OLED_Display.setColor(BLACK);
  OLED_Display.fillRect(0, apScroll.y, 128, 12);
  OLED_Display.setColor(WHITE);

  int x = -apScroll.x;
  OLED_Display.drawString(x,                   apScroll.y, apScroll.text);
  OLED_Display.drawString(x + apScroll.w + 16, apScroll.y, apScroll.text);

  apScroll.x += 2;
  if (apScroll.x > apScroll.w + 16) apScroll.x = 0;

  OLED_Display.display();
}

// ---------- Helpers ----------
String toHex(const uint8_t* v, size_t n){ char b[3]; String s; s.reserve(n*2); for(size_t i=0;i<n;i++){ sprintf(b,"%02X",v[i]); s+=b; } return s; }
String toHexLSB(const uint8_t* v, size_t n){ char b[3]; String s; s.reserve(n*2); for(int i=(int)n-1;i>=0;i--){ sprintf(b,"%02X",v[i]); s+=b; } return s; }
bool parseHex(const String& s, uint8_t* out, size_t n){ if(s.length()!=n*2) return false; for(size_t i=0;i<n;i++){ char b[3]={ (char)s[2*i], (char)s[2*i+1], 0 }; out[i]=(uint8_t)strtoul(b,nullptr,16);} return true; }
// parse MSB hex string into LSB array (reverse)
bool parseHexRev(const String& msb, uint8_t* out, size_t n){
  if(msb.length()!=n*2) return false;
  for(size_t i=0;i<n;i++){
    char b[3]={ (char)msb[2*i], (char)msb[2*i+1], 0 };
    out[n-1-i]=(uint8_t)strtoul(b,nullptr,16);
  }
  return true;
}

void devEuiFromChip(uint8_t outMSB[8]) {
  uint64_t mac = ESP.getEfuseMac(); uint8_t m[6];
  for (int i=0;i<6;i++) m[i]=(mac>>(8*(5-i)))&0xFF;
  outMSB[0]=m[0]; outMSB[1]=m[1]; outMSB[2]=m[2]; outMSB[3]=0xFF; outMSB[4]=0xFE; outMSB[5]=m[3]; outMSB[6]=m[4]; outMSB[7]=m[5];
}
String devEuiSuffix6(){ char buf[7]; sprintf(buf,"%02X%02X%02X", cfg.devEui[5], cfg.devEui[6], cfg.devEui[7]); return String(buf); }

// Sensors pwr
static inline void SensorsON(){ pinMode(PIN_SENS_EN, OUTPUT); digitalWrite(PIN_SENS_EN, SENS_ACTIVE_HIGH?HIGH:LOW); }
static inline void SensorsOFF(){ digitalWrite(PIN_SENS_EN, SENS_ACTIVE_HIGH?LOW:HIGH); pinMode(PIN_SENS_EN, INPUT); }
static inline bool crackPresent(){ pinMode(PIN_CRACK_PRESENT, INPUT_PULLUP); return digitalRead(PIN_CRACK_PRESENT)==LOW; }

// ADS helpers
static inline void adsInitOnce(){
  ads.setGain(GAIN_ONE);              // ±4.096 V FS
  ads.setDataRate(RATE_ADS1115_128SPS);
  (void)ads.readADC_SingleEnded(0); delay(2);
}

// Power sequencing
void sensorsPowerOn() {
  SensorsON(); delay(12);
  I2C_ADS.begin(I2C_ADS_SDA, I2C_ADS_SCL, I2C_FREQ_HZ);
  if (ads.begin(ADS1115_ADDR, &I2C_ADS)) adsInitOnce();
  else Serial.println("ADS1115 not found");

  if (!firstDsFound) {
    sensors.begin(); firstDsFound = sensors.getAddress(firstDs, 0);
    if (!firstDsFound) { OneWire ow(PIN_ONE_WIRE); uint8_t addr[8]; ow.reset_search();
      while (ow.search(addr)) { if (addr[0]==0x28){ memcpy(firstDs, addr, 8); firstDsFound=true; break; } } }
  }
  if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
}
void sensorsPowerOff(){ I2C_ADS.end(); pinMode(I2C_ADS_SDA, INPUT); pinMode(I2C_ADS_SCL, INPUT); pinMode(PIN_ONE_WIRE, INPUT); SensorsOFF(); }

// DS18B20
int16_t readTemp_c_x100(){ if(!firstDsFound) return 5000; sensors.requestTemperaturesByAddress(firstDs); delay(200);
  float tC=sensors.getTempC(firstDs); if(tC<=-127.0f||tC>=125.0f) return 5000; int v=(int)(tC*100.0f)+5000; if(v<0)v=0; return (int16_t)v; }
float readTempOnceC(){ if(!firstDsFound) return NAN; sensors.requestTemperaturesByAddress(firstDs); delay(200);
  float tC=sensors.getTempC(firstDs); if(tC<=-127.0f||tC>=125.0f) return NAN; return tC; }

// Crack mapping (fallback)
static inline uint16_t mapCrackRawToMMx100(int raw){
  const int32_t r0=cfg.crack_r0, r1=cfg.crack_r1, y0=cfg.crack_mm0_x100, y1=cfg.crack_mm1_x100;
  int32_t den=(r1-r0); if(den==0) return 0;
  int64_t num=(int64_t)(y1-y0)*(int64_t)(raw-r0);
  int64_t y=(int64_t)y0 + (num + (den>0?den/2:-den/2))/den;
  if(y<0)y=0; if(y>65535)y=65535; return (uint16_t)y;
}
static inline float mapCrackRawToMM_f(int raw){
  const float r0=(float)cfg.crack_r0, r1=(float)cfg.crack_r1;
  const float mm0=(float)cfg.crack_mm0_x100/100.0f, mm1=(float)cfg.crack_mm1_x100/100.0f;
  if(r1==r0) return 0.0f;
  float mm=mm0 + (mm1-mm0)*((float)raw - r0)/(r1 - r0);
  if(mm<0) mm=0; return mm;
}

// ---------- Ratio measurement ----------
struct CrackRatio {
  bool    present;
  int16_t raw_meas;   // CH0
  int16_t raw_ref;    // CH1
  float   mm;         // mm
  bool    used_ratio; // true if ratio used
};

CrackRatio readCrack_ratio(){
  CrackRatio r{false,0,0,0.0f,false};
  r.present = crackPresent();
  if(!r.present) return r;

  int32_t sumM=0, sumR=0;
  for(uint8_t i=0;i<RATIO_SAMPLES;i++){
    int16_t rm = ads.readADC_SingleEnded(0); if(rm<0) rm=0; sumM += rm;
    int16_t rr = ads.readADC_SingleEnded(1); if(rr<0) rr=0; sumR += rr;
  }
  r.raw_meas = (int16_t)(sumM / RATIO_SAMPLES);
  r.raw_ref  = (int16_t)(sumR / RATIO_SAMPLES);

  if(r.raw_ref>0){
    float Lfull = (float)cfg.crack_len_mm_x100 / 100.0f;  // e.g. 10.00 mm
    r.mm = Lfull * (float)r.raw_meas / (float)r.raw_ref;
    r.used_ratio = true;
  } else {
    r.mm = mapCrackRawToMM_f(r.raw_meas);
    r.used_ratio = false;
  }
  if(r.mm < 0) r.mm = 0; if(r.mm > 65.535f) r.mm = 65.535f;
  return r;
}

// Measurements
uint16_t readBattery_mV(){
  analogSetAttenuation(ADC_0db);
  pinMode(PIN_ADC_CTRL, OUTPUT); digitalWrite(PIN_ADC_CTRL, HIGH); delay(2);
  int raw=analogRead(PIN_VBAT_ADC);
  digitalWrite(PIN_ADC_CTRL, LOW);
  return (uint16_t)(VBAT_ADC_FACTOR * raw * 1000.0f);
}

// DR helpers
static inline uint8_t mapDR(uint8_t idx){ switch(idx){ case 0:return DR_0; case 1:return DR_1; case 2:return DR_2; case 3:return DR_3; case 4:return DR_4; case 5:return DR_5; default:return DR_3; } }
static inline void applyLoraDataRate(){ loraWanAdr = cfg.lora_adr; LoRaWAN.setDefaultDR(mapDR(cfg.lora_dr)); }

// ---------- OLED screens ----------
static void oledSplash(){
  OLED_Display.init(); OLED_Display.clear(); OLED_Display.setColor(WHITE);
  const int x=(128 - logoMKP_width)/2; const int y=0;
  OLED_Display.drawXbm(x, y, logoMKP_width, logoMKP_height, (const uint8_t*)logoMKP_bits);
  OLED_Display.setFont(ArialMT_Plain_10); OLED_Display.setTextAlignment(TEXT_ALIGN_CENTER);
  OLED_Display.drawString(64, logoMKP_height + 2, "MARX KRONTAL PARTNER");
  OLED_Display.display(); delay(3000);
}

static void oledShowSSID(const String& ssid){
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 0,  "Access Point");
  OLED_Display.drawString(0, 14, "SSID:");
  OLED_Display.drawString(0, 26, ssid);
  OLED_Display.drawString(0, 40, "IP: " + WiFi.softAPIP().toString());
  OLED_Display.display();

  apScroll.text   = "AP SSID: " + ssid;
  apScroll.w      = OLED_Display.getStringWidth(apScroll.text);
  apScroll.x      = 0;
  apScroll.y      = 52;
  apScroll.active = true;
}

void oledSensorsOnce() {
  sensorsPowerOn();

  CrackRatio cr = readCrack_ratio();
  float t = readTempOnceC();
  uint16_t bat = readBattery_mV();

  sensorsPowerOff();

  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);

  int y = 0, dy = 12;
  if (isfinite(t)) OLED_Display.drawString(0, y,   String("DS18B20  ")+String(t,1)+" C");
  else             OLED_Display.drawString(0, y,   "DS18B20  N/C"); y += dy;

  OLED_Display.drawString(0, y, String("ADC M   ")+(cr.present?String(cr.raw_meas):String("N/C"))); y += dy;
  OLED_Display.drawString(0, y, String("ADC R   ")+(cr.present?String(cr.raw_ref ):String("N/C"))); y += dy;

  if (cr.present) OLED_Display.drawString(0, y, String("Crack   ")+String(cr.mm,3)+" mm"+(cr.used_ratio?" (R)":" (L)"));
  else            OLED_Display.drawString(0, y, "Crack   N/C"); y += dy;

  OLED_Display.drawString(0, y, String("Battery ")+String(bat)+" mV"); y += dy;

  if (lora_has_rx) OLED_Display.drawString(0, y, String("RSSI ")+String(lora_last_rssi)+"  SNR "+String(lora_last_snr));
  else             OLED_Display.drawString(0, y, "RSSI  —");

  OLED_Display.display();
}

// ---------- Web ----------
WebServer http(80);

String ipStr(){ if(WiFi.getMode()==WIFI_AP) return WiFi.softAPIP().toString(); if(WiFi.status()==WL_CONNECTED) return WiFi.localIP().toString(); return "-"; }
String contentTypeFor(const String& path){
  if (path.endsWith(".html")) return "text/html; charset=utf-8";
  if (path.endsWith(".svg"))  return "image/svg+xml";
  if (path.endsWith(".json")) return "application/json; charset=utf-8";
  if (path.endsWith(".css"))  return "text/css";
  if (path.endsWith(".js"))   return "application/javascript";
  return "application/octet-stream";
}
bool streamFileFS(const char* p){ if(!LittleFS.exists(p)) return false; File f=LittleFS.open(p,"r"); if(!f) return false; http.streamFile(f, contentTypeFor(p)); f.close(); return true; }

void api_state() {
  StaticJsonDocument<JSON_STATE_DOC> d;
  d["mode"]      = (WiFi.getMode() == WIFI_AP) ? "AP" : "LNS";
  d["ip"]        = ipStr();
  d["uptime_s"]  = (uint32_t)(millis() / MS_PER_SEC);
  d["heap_free"] = ESP.getFreeHeap();

  sensorsPowerOn();
  CrackRatio cr = readCrack_ratio();
  uint16_t batmV = readBattery_mV();
  float tC = readTempOnceC();
  sensorsPowerOff();

  d["crack_present"] = cr.present;
  d["adc_raw"]       = cr.present ? cr.raw_meas : 0;
  d["adc_ref"]       = cr.present ? cr.raw_ref  : 0;
  if (cr.present) { d["crack_mm"] = cr.mm; d["ratio"] = cr.used_ratio; }
  else            { d["crack_mm"] = nullptr; d["ratio"] = nullptr; }

  d["batt_mV"] = batmV;
  if (isfinite(tC)) d["DS18B20_Temp"] = tC; else d["DS18B20_Temp"] = nullptr;

  JsonObject lj = d.createNestedObject("lora");
  if (lora_has_rx) {
    const uint32_t age = (millis() - lora_last_rx_ms)/MS_PER_SEC;
    lj["rssi"]  = lora_last_rssi;
    lj["snr"]   = lora_last_snr;
    lj["age_s"] = age;
    d["rssi"]     = lora_last_rssi;
    d["snr"]      = lora_last_snr;
    d["dl_age_s"] = age;
  } else {
    lj["rssi"] = lj["snr"] = lj["age_s"] = nullptr;
    d["rssi"] = d["snr"] = d["dl_age_s"] = nullptr;
  }

  JsonObject c = d.createNestedObject("cfg");
  c["minutes"] = cfg.minutes;
  // expose DevEUI/AppEUI in MSB for UI
  c["devEui"]  = toHexLSB(cfg.devEui, 8);
  c["appEui"]  = toHexLSB(cfg.appEui, 8);
  c["appKey"]  = toHex(cfg.appKey, 16);   // MSB as-is
  c["crack_len_x100"] = cfg.crack_len_mm_x100;

  JsonObject cal = c.createNestedObject("crack_cal");
  cal["r0"] = cfg.crack_r0;  cal["mm0"] = (float)cfg.crack_mm0_x100 / 100.0f;
  cal["r1"] = cfg.crack_r1;  cal["mm1"] = (float)cfg.crack_mm1_x100 / 100.0f;

  c["dr"]  = cfg.lora_dr;
  c["adr"] = cfg.lora_adr;

  JsonObject ts = c.createNestedObject("time_sync");
  ts["enabled"] = cfg.ts_enable;
  ts["hours"]   = cfg.ts_hours;
  c["align_minute"] = cfg.align_minute;
  ts["valid"] = sysTimeLooksValid();

  String out; serializeJson(d, out);
  http.send(200, "application/json", out);
}

void api_send()  { deviceState = DEVICE_STATE_SEND; http.send(200, "text/plain", "queued"); }
void api_reset() { http.send(200, "text/plain", "restarting"); delay(200); ESP.restart(); }

void api_cfg_get() {
  StaticJsonDocument<JSON_CFG_DOC> d;
  // expose MSB for UI
  d["devEui"] = toHexLSB(cfg.devEui, 8);
  d["appEui"] = toHexLSB(cfg.appEui, 8);
  d["appKey"] = toHex(cfg.appKey, 16);
  d["minutes"] = cfg.minutes;
  d["crack_len_x100"] = cfg.crack_len_mm_x100;

  JsonObject cal = d.createNestedObject("crack_cal");
  cal["r0"] = cfg.crack_r0;  cal["mm0"] = (float)cfg.crack_mm0_x100 / 100.0f;
  cal["r1"] = cfg.crack_r1;  cal["mm1"] = (float)cfg.crack_mm1_x100 / 100.0f;

  d["dr"]  = cfg.lora_dr;
  d["adr"] = cfg.lora_adr;

  JsonObject ts = d.createNestedObject("time_sync");
  ts["enabled"] = cfg.ts_enable;
  ts["hours"]   = cfg.ts_hours;
  d["align_minute"] = cfg.align_minute;
  ts["valid"] = sysTimeLooksValid();

  String out; serializeJson(d, out);
  http.send(200, "application/json", out);
}

void api_cfg_lora() {
  if (!http.hasArg("plain")) { http.send(400, "text/plain", "bad json"); return; }
  StaticJsonDocument<512> d;
  if (deserializeJson(d, http.arg("plain"))) { http.send(400, "text/plain", "bad json"); return; }
  bool ok = true;

  // UI sends MSB strings → parse into LSB arrays for LoRa stack
  if (d.containsKey("devEui")) ok &= parseHexRev((const char*)d["devEui"], cfg.devEui, 8); // LSB
  if (d.containsKey("appEui")) ok &= parseHexRev((const char*)d["appEui"], cfg.appEui, 8); // LSB
  if (d.containsKey("appKey")) ok &= parseHex   ((const char*)d["appKey"], cfg.appKey,16); // MSB

  if (d.containsKey("minutes")) { cfg.minutes = d["minutes"].as<uint32_t>(); saveKVu("minutes", cfg.minutes); }

  if (d.containsKey("crack_len_mm")) {
    float L = d["crack_len_mm"].as<float>();
    if (isfinite(L) && L>0.01f && L<1000.0f) {
      uint16_t Lx100 = (uint16_t)(L*100.0f + 0.5f);
      cfg.crack_len_mm_x100 = Lx100;
      if (cfg.crack_mm1_x100 < Lx100) cfg.crack_mm1_x100 = Lx100;
      saveUShort("cr_len_x100", cfg.crack_len_mm_x100);
    } else { http.send(400, "text/plain", "len range"); return; }
  }

  if (d.containsKey("crack_cal")) {
    JsonObject cal = d["crack_cal"].as<JsonObject>();
    bool okc = true;
    if (cal.containsKey("r0"))  cfg.crack_r0 = cal["r0"].as<int>();
    if (cal.containsKey("r1"))  cfg.crack_r1 = cal["r1"].as<int>();
    if (cal.containsKey("mm0")) { float mm0 = cal["mm0"].as<float>(); if (isfinite(mm0)&&mm0>=0&&mm0<1000.0f) cfg.crack_mm0_x100 = (uint16_t)lrintf(mm0*100.0f); else okc=false; }
    if (cal.containsKey("mm1")) { float mm1 = cal["mm1"].as<float>(); if (isfinite(mm1)&&mm1>=0&&mm1<=1000.0f) cfg.crack_mm1_x100 = (uint16_t)lrintf(mm1*100.0f); else okc=false; }
    if (!okc || cfg.crack_r0==cfg.crack_r1) { http.send(400, "text/plain", "cal error"); return; }

    saveShort ("cr_r0",  cfg.crack_r0);
    saveShort ("cr_r1",  cfg.crack_r1);
    saveUShort("cr_mm0", cfg.crack_mm0_x100);
    saveUShort("cr_mm1", cfg.crack_mm1_x100);
  }

  if (d.containsKey("dr")) {
    int v = d["dr"].as<int>();
    if (v < 0 || v > 5) { http.send(400, "text/plain", "dr range"); return; }
    cfg.lora_dr  = (uint8_t)v;
    cfg.lora_adr = false;
    saveUChar("lora_dr",  cfg.lora_dr);
    saveBool ("lora_adr", cfg.lora_adr);
  }
  if (d.containsKey("adr")) { cfg.lora_adr = d["adr"].as<bool>(); saveBool("lora_adr", cfg.lora_adr); }

  if (d.containsKey("time_sync")) {
    JsonObject ts = d["time_sync"].as<JsonObject>();
    if (ts.containsKey("enabled")) { cfg.ts_enable = ts["enabled"].as<bool>(); saveBool("ts_en", cfg.ts_enable); }
    if (ts.containsKey("hours"))   { int h = ts["hours"].as<int>(); if (h<1||h>168){ http.send(400,"text/plain","ts hours range"); return; }
                                     cfg.ts_hours = (uint16_t)h; saveUShort("ts_h",  cfg.ts_hours); }
  }
  if (d.containsKey("align_minute")) { cfg.align_minute = d["align_minute"].as<bool>(); saveBool("align_min", cfg.align_minute); }

  if (!ok) { http.send(400, "text/plain", "hex error"); return; }
  saveBytes("devEui", cfg.devEui, 8);
  saveBytes("appEui", cfg.appEui, 8);
  saveBytes("appKey", cfg.appKey, 16);

  http.send(200, "text/plain", "ok");
  delay(200);
  ESP.restart();
}

void api_deveui_get() {
  uint8_t chipMSB[8]; devEuiFromChip(chipMSB);
  StaticJsonDocument<256> d;
  d["chip_devEui_msb"]   = toHex(chipMSB, 8);
  d["chip_devEui_lsb"]   = toHexLSB(chipMSB, 8);
  // show stored as MSB for UI
  d["stored_devEui_msb"] = toHexLSB(cfg.devEui, 8);
  String out; serializeJson(d, out);
  http.send(200, "application/json", out);
}

void api_deveui_post() {
  if (!http.hasArg("plain")) { http.send(400, "text/plain", "bad json"); return; }
  StaticJsonDocument<128> d;
  if (deserializeJson(d, http.arg("plain"))) { http.send(400, "text/plain", "bad json"); return; }
  if (d.containsKey("use") && String((const char*)d["use"])=="chip") {
    uint8_t chipMSB[8]; devEuiFromChip(chipMSB);
    // store as LSB
    for (int i=0;i<8;i++) cfg.devEui[i]=chipMSB[7-i];
    saveBytes("devEui", cfg.devEui, 8);
    http.send(200, "text/plain", "ok"); delay(200); ESP.restart(); return;
  }
  if (d.containsKey("devEui")) {
    String h = d["devEui"].as<String>();  // MSB from UI
    if (!parseHexRev(h, cfg.devEui, 8)) { http.send(400, "text/plain", "hex error"); return; } // store LSB
    saveBytes("devEui", cfg.devEui, 8);
    http.send(200, "text/plain", "ok"); delay(200); ESP.restart(); return;
  }
  http.send(400, "text/plain", "args");
}

void attachHttpFS() {
  http.on("/", [](){ if(!streamFileFS("/index.html")) http.send(404,"text/plain","index missing"); });
  http.on("/logo.svg", [](){ if(!streamFileFS("/logo.svg")) http.send(404,"text/plain","logo missing"); });
  http.on("/i18n.json", [](){ if(!streamFileFS("/i18n.json")) http.send(404,"text/plain","i18n missing"); });
  http.on("/api/state",        api_state);
  http.on("/api/send",  HTTP_POST, api_send);
  http.on("/api/reset", HTTP_POST, api_reset);
  http.on("/api/config",       api_cfg_get);
  http.on("/api/config/lora",  HTTP_POST, api_cfg_lora);
  http.on("/api/deveui",       HTTP_GET,  api_deveui_get);
  http.on("/api/deveui",       HTTP_POST, api_deveui_post);
  http.onNotFound([](){ if(!streamFileFS("/index.html")) http.send(404,"text/plain","404"); });
  http.begin();
}

// ---------- LoRaWAN payload (8 bytes) ----------
void prepareTxFrame(uint8_t port) {
  (void)port;
  sensorsPowerOn();

  CrackRatio cr = readCrack_ratio();
  int16_t  t  = readTemp_c_x100();
  uint16_t vb = readBattery_mV();

  sensorsPowerOff();

  uint16_t cr1000 = (uint16_t)lrintf(cr.mm * 1000.0f);
  int16_t  adcRaw = cr.present ? cr.raw_meas : 0;

  appDataSize = 8;
  appData[0] = t >> 8;        appData[1] = t;
  appData[2] = adcRaw >> 8;   appData[3] = adcRaw;
  appData[4] = cr1000 >> 8;   appData[5] = cr1000;
  appData[6] = vb >> 8;       appData[7] = vb;
}

// ---------- Phases for AP display ----------
enum OledPhase { OLED_LOGO, OLED_SSID, OLED_SENS };
OledPhase oledPhase = OLED_LOGO;
uint32_t  oledPhaseStart = 0;

// ---------- Setup ----------
void setup() {
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  pinMode(PIN_MODE_OUT, OUTPUT); digitalWrite(PIN_MODE_OUT, LOW);
  pinMode(PIN_MODE_IN, INPUT_PULLUP); delay(5);
  pinMode(PIN_CRACK_PRESENT, INPUT_PULLUP);

  for (int i=0;i<5;i++){ apMode |= (digitalRead(PIN_MODE_IN)==LOW); delay(2); }

  analogReadResolution(12);
  loadCfg();
  appTxDutyCycle = 60000UL * cfg.minutes;

  loraWanAdr = cfg.lora_adr;
  memcpy(devEui, cfg.devEui, sizeof(devEui));
  memcpy(appEui, cfg.appEui, sizeof(appEui));
  memcpy(appKey, cfg.appKey, sizeof(appKey));

  if (apMode) {
    isTxConfirmed     = true;
    confirmedNbTrials = CONF_TRIALS_AP;

    WiFi.mode(WIFI_AP);
    String ssid = "Rissmonitoring-" + devEuiSuffix6();

    bool fsOk = LittleFS.begin(true);
    if (!fsOk) Serial.println("[FS] LittleFS mount failed even after format");
    else attachHttpFS();

    WiFi.setTxPower(WIFI_POWER_7dBm);
    WiFi.softAP(ssid.c_str(), AP_PSK);

    pinMode(Vext, OUTPUT); digitalWrite(Vext, LOW);

    oledSplash();                               // 3s
    oledPhase       = OLED_SSID;
    oledPhaseStart  = millis();
    oledShowSSID(ssid);                          // SSID screen

  } else {
    isTxConfirmed     = false;
    confirmedNbTrials = CONF_TRIALS_FIELD;

    WiFi.persistent(false);
    WiFi.disconnect(true, true);
    WiFi.mode(WIFI_OFF);
#if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
    btStop();
#endif
    pinMode(Vext, OUTPUT); digitalWrite(Vext, HIGH);
  }

  deviceState = DEVICE_STATE_INIT;
}

// ---------- Loop ----------
void loop() {
  if (apMode) {
    http.handleClient();
    oledMarqueeTick();

    if (oledPhase == OLED_SSID && millis() - oledPhaseStart >= 3000) {
      apScroll.active = false;
      oledPhase = OLED_SENS;
      oledPhaseStart = millis();
      oledSensorsOnce();
    }

    static uint32_t tmr=0;
    if (oledPhase == OLED_SENS && millis()-tmr>1000) { tmr=millis(); oledSensorsOnce(); }
  }

  switch (deviceState) {
    case DEVICE_STATE_INIT:
      LoRaWAN.init(loraWanClass, loraWanRegion);
      applyLoraDataRate();
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

    case DEVICE_STATE_CYCLE: {
      if (cfg.align_minute) {
        const uint32_t period = cfg.minutes * SEC_PER_MIN;
        const uint32_t nowS   = now_sec();
        uint32_t rem          = nowS % period;
        uint32_t waitSec      = (rem==0) ? period : (period - rem);
        txDutyCycleTime = waitSec * MS_PER_SEC;
      } else {
        txDutyCycleTime = appTxDutyCycle + randr(-APP_TX_DUTYCYCLE_RND, APP_TX_DUTYCYCLE_RND);
      }
      LoRaWAN.cycle(txDutyCycleTime);
      deviceState = DEVICE_STATE_SLEEP;
      break;
    }

    case DEVICE_STATE_SLEEP:
      sensorsPowerOff();
      if (apMode) LoRaWAN.sleep(CLASS_C);
      else        LoRaWAN.sleep(loraWanClass);
      break;

    default:
      deviceState = DEVICE_STATE_INIT;
      break;
  }
}
