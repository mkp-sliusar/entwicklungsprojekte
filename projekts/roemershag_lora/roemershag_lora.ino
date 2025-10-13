/*
 * =====================================================================
 *  Project: Heltec ESP32-S3 (LoRa V3) — Crack Monitoring Node
 *  Function: Ratio (CH0/CH1)
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
 *      If CH1 == 0 → linear fallback-----------------------------
 *  Modes:
 *    AUTO    — dynamic scaling by ADS1115 CH1
 *    MANUAL  — linear map r0→mm0, r1→mm1 on CH0
 * ---------------------------------------------------------------------
 *  LoRaWAN ID conventions:
 *    DevEUI, AppEUI — LSB order end-to-end (UI, storage, prints)
 *    AppKey         — MSB order end-to-end (UI, storage, prints)
 * ---------------------------------------------------------------------
 *  Version:      1.5.0
 *  Date:         2025-10-13
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

// ---------- Heltec LoRaWAN keys expected by core (extern symbols) ----------
uint8_t devEui[8]  = {0};   // LSB
uint8_t appEui[8]  = {0};   // LSB
uint8_t appKey[16] = {0};   // MSB

// Local mirrors for logging clarity
uint8_t DevEui[8]={0}, AppEui[8]={0}, AppKey[16]={0};

// ---------- Types ----------
struct CrackMeas {
  bool    present;
  int16_t raw0;       // ADS CH0
  int16_t raw1;       // ADS CH1
  float   mm;         // mapped value in mm
  bool    auto_used;  // true if AUTO scaling used
};
CrackMeas readCrack();  // explicit prototype for Arduino preprocessor

// ---------- Hardware pins and constants ----------
static constexpr uint8_t  OLED_ADDR   = 0x3C;
static constexpr int      I2C_ADS_SDA = 47;
static constexpr int      I2C_ADS_SCL = 48;
static constexpr uint32_t I2C_FREQ_HZ = 400000;

static constexpr int PIN_ONE_WIRE      = 4;
static constexpr int PIN_VBAT_ADC      = 1;
static constexpr int PIN_ADC_CTRL      = 37;
static constexpr int PIN_MODE_OUT      = 45;
static constexpr int PIN_MODE_IN       = 46;
static constexpr int PIN_CRACK_PRESENT = 3;

static constexpr int  PIN_SENS_EN      = 7;
static constexpr bool SENS_ACTIVE_HIGH = true;

static constexpr uint8_t  ADS1115_ADDR = 0x48;
static constexpr uint8_t  DS_RES_BITS  = 10;

// Calibrated divider factor for battery ADC → volts
static constexpr float VBAT_ADC_FACTOR = 1.0f/4096.0f/0.210282997855762f;

// ---------- System constants ----------
static constexpr char   AP_PSK[]           = "12345678";
static constexpr size_t JSON_STATE_DOC     = 1700;
static constexpr size_t JSON_CFG_DOC       = 1100;
static constexpr uint32_t MS_PER_SEC       = 1000UL;
static constexpr uint32_t SEC_PER_MIN      = 60UL;
static constexpr uint32_t MARQUEE_PERIOD_MS= 40;

static constexpr uint8_t  UPLINK_PORT      = 2;
static constexpr uint8_t  CONF_TRIALS_AP   = 3;
static constexpr uint8_t  CONF_TRIALS_FIELD= 1;

// ---------- Time helper ----------
static inline uint32_t now_sec(){ return millis()/MS_PER_SEC; }

// ---------- Peripherals ----------
SSD1306Wire OLED_Display(OLED_ADDR, I2C_FREQ_HZ, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);
TwoWire I2C_ADS = TwoWire(1);
Adafruit_ADS1115 ads;

OneWire oneWire(PIN_ONE_WIRE);
DallasTemperature sensors(&oneWire);
uint8_t firstDs[8] = {0};
bool firstDsFound=false;

bool apMode=false;
Preferences prefs;

enum RefMode:uint8_t{ REF_AUTO=0, REF_MANUAL=1 };

// ---------- Config persisted in NVS ----------
struct Cfg{
  // LoRa IDs/Key
  uint8_t devEui[8]={0};   // LSB
  uint8_t appEui[8]={0};   // LSB
  uint8_t appKey[16]={0};  // MSB

  // Uplink period
  uint32_t minutes=15;

  // Manual linear calibration for CH0
  int16_t  crack_r0=5;
  int16_t  crack_r1=25480;
  uint16_t crack_mm0_x100=0;
  uint16_t crack_mm1_x100=1000; // 10.00 mm

  // Clamp for both AUTO and MANUAL
  uint16_t crack_len_mm_x100=1000; // 10.00 mm max

  // Reference mode
  uint8_t  ref_mode=REF_AUTO;

  // LoRa link
  uint8_t  lora_dr=3;
  bool     lora_adr=true;

  // Scheduling
  bool     ts_enable=false;   // reserved, not used in this sketch
  uint16_t ts_hours=24;       // reserved
  bool     align_minute=true; // align send to minutes grid
} cfg;

// ---------- Heltec LoRaWAN globals required by their core ----------
uint16_t        userChannelsMask[6] = { 0x00FF, 0, 0, 0, 0, 0 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t   loraWanClass  = CLASS_A;

bool overTheAirActivation = true;
bool loraWanAdr           = true;

bool     isTxConfirmed     = false;
uint8_t  appPort           = UPLINK_PORT;
uint8_t  confirmedNbTrials = 1;
uint32_t appTxDutyCycle    = 60000UL * 2;   // unused by our scheduler

uint8_t  nwkSKey[16] = {0}; // ABP only
uint8_t  appSKey[16] = {0};
uint32_t devAddr     = 0;

// ---------- NVS helpers ----------
static inline void nvsPut(const char* k,const void* p,size_t n){ prefs.begin("uhfb",false); prefs.putBytes(k,p,n); prefs.end(); }
static inline void nvsGet(const char* k,void* p,size_t n){ prefs.begin("uhfb",true); prefs.getBytes(k,p,n); prefs.end(); }

void loadCfg(){
  prefs.begin("uhfb",true);
  prefs.getBytes("devEui", cfg.devEui, 8);
  prefs.getBytes("appEui", cfg.appEui, 8);
  prefs.getBytes("appKey", cfg.appKey, 16);
  cfg.minutes  = prefs.getUInt ("minutes", cfg.minutes);
  cfg.crack_len_mm_x100 = prefs.getUShort("cr_len_x100", cfg.crack_len_mm_x100);
  cfg.crack_r0 = prefs.getShort("cr_r0", cfg.crack_r0);
  cfg.crack_r1 = prefs.getShort("cr_r1", cfg.crack_r1);
  cfg.crack_mm0_x100 = prefs.getUShort("cr_mm0", cfg.crack_mm0_x100);
  cfg.crack_mm1_x100 = prefs.getUShort("cr_mm1", cfg.crack_mm1_x100);
  if(cfg.crack_mm1_x100==1000) cfg.crack_mm1_x100=cfg.crack_len_mm_x100;
  cfg.ref_mode = prefs.getUChar("ref_mode", cfg.ref_mode);
  cfg.lora_dr  = prefs.getUChar("lora_dr",  cfg.lora_dr);
  cfg.lora_adr = prefs.getBool ("lora_adr", cfg.lora_adr);
  cfg.ts_enable    = prefs.getBool  ("ts_en",     cfg.ts_enable);
  cfg.ts_hours     = prefs.getUShort("ts_h",      cfg.ts_hours);
  cfg.align_minute = prefs.getBool  ("align_min", cfg.align_minute);
  prefs.end();
}

// ---------- Hex helpers ----------
// Straight parse: read "AABB..." into out[0]=AA, out[1]=BB, ...
static inline bool parseHexStraight(const String& s,uint8_t* out,size_t n){
  if(s.length()!=n*2) return false;
  for(size_t i=0;i<n;i++){ char b[3]={ (char)s[2*i], (char)s[2*i+1], 0}; out[i]=(uint8_t)strtoul(b,nullptr,16);}
  return true;
}
// For LSB strings where UI already shows LSB order we still parse straight
static inline bool parseHexLSB(const String& s,uint8_t* out,size_t n){ return parseHexStraight(s,out,n); }
// Historical helper that reverses (kept for reference, not used for AppKey)
static inline bool parseHexMSB_reverse(const String& msb,uint8_t* out,size_t n){
  if(msb.length()!=n*2) return false;
  for(size_t i=0;i<n;i++){ char b[3]={ (char)msb[2*i], (char)msb[2*i+1], 0}; out[n-1-i]=(uint8_t)strtoul(b,nullptr,16);}
  return true;
}

String toHex(const uint8_t* v,size_t n){ char b[3]; String s; s.reserve(n*2); for(size_t i=0;i<n;i++){ sprintf(b,"%02X",v[i]); s+=b; } return s; }

void devEuiFromChipMSB(uint8_t out[8]){
  uint64_t mac=ESP.getEfuseMac(); uint8_t m[6];
  for(int i=0;i<6;i++) m[i]=(mac>>(8*(5-i)))&0xFF;
  out[0]=m[0]; out[1]=m[1]; out[2]=m[2]; out[3]=0xFF; out[4]=0xFE; out[5]=m[3]; out[6]=m[4]; out[7]=m[5];
}
String devEuiSuffix6LSB(){ char buf[7]; sprintf(buf,"%02X%02X%02X", cfg.devEui[5], cfg.devEui[6], cfg.devEui[7]); return String(buf); }

// ---------- Power and sensors ----------
static inline void SensorsON(){ pinMode(PIN_SENS_EN,OUTPUT); digitalWrite(PIN_SENS_EN, SENS_ACTIVE_HIGH?HIGH:LOW); }
static inline void SensorsOFF(){ digitalWrite(PIN_SENS_EN, SENS_ACTIVE_HIGH?LOW:HIGH); pinMode(PIN_SENS_EN,INPUT); }
static inline bool crackPresent(){ pinMode(PIN_CRACK_PRESENT,INPUT_PULLUP); return digitalRead(PIN_CRACK_PRESENT)==LOW; }

static inline void adsInitOnce(){ ads.setGain(GAIN_ONE); ads.setDataRate(RATE_ADS1115_860SPS); (void)ads.readADC_SingleEnded(0); delay(1); }

void sensorsPowerOn(){
  SensorsON(); delay(12);
  I2C_ADS.begin(I2C_ADS_SDA, I2C_ADS_SCL, I2C_FREQ_HZ);
  if(ads.begin(ADS1115_ADDR,&I2C_ADS)) adsInitOnce(); else Serial.println("ADS1115 not found");
  if(!firstDsFound){
    sensors.begin(); firstDsFound = sensors.getAddress(firstDs,0);
    if(!firstDsFound){ OneWire ow(PIN_ONE_WIRE); uint8_t a[8]; ow.reset_search(); while(ow.search(a)){ if(a[0]==0x28){ memcpy(firstDs,a,8); firstDsFound=true; break; } } }
  }
  if(firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);
}
void sensorsPowerOff(){ I2C_ADS.end(); pinMode(I2C_ADS_SDA,INPUT); pinMode(I2C_ADS_SCL,INPUT); pinMode(PIN_ONE_WIRE,INPUT); SensorsOFF(); }

int16_t readTemp_c_x100(){
  if(!firstDsFound) return 5000;
  sensors.requestTemperaturesByAddress(firstDs); delay(200);
  float t=sensors.getTempC(firstDs); if(t<=-127||t>=125) return 5000;
  int v=(int)(t*100.0f)+5000; if(v<0)v=0; return (int16_t)v;
}
float readTempOnceC(){
  if(!firstDsFound) return NAN;
  sensors.requestTemperaturesByAddress(firstDs); delay(200);
  float t=sensors.getTempC(firstDs); if(t<=-127||t>=125) return NAN; return t;
}

// ---------- Mapping ----------
static constexpr int16_t RAW_MIN_STABLE = 6;
static constexpr int16_t DYN_SPAN_MIN   = 50;

static inline float mapLinear_i32(int32_t x,int32_t in_min,int32_t in_max,float out_min,float out_max){
  if(in_max<=in_min) return out_min;
  return (float)(x-in_min)*(out_max-out_min)/(float)(in_max-in_min)+out_min;
}

static inline float mapCrackManual(int M){
  const float M0=(float)cfg.crack_r0, M1=(float)cfg.crack_r1;
  const float mm0=(float)cfg.crack_mm0_x100/100.0f, mm1=(float)cfg.crack_mm1_x100/100.0f;
  if(M1==M0) return 0.0f;
  float mm=mm0 + (mm1-mm0)*((float)M - M0)/(M1 - M0);
  if(mm<0) mm=0;
  const float L=(float)cfg.crack_len_mm_x100/100.0f; if(mm>L) mm=L;
  return mm;
}

static inline float mapCrackAuto_byCH1(int M,int R_curr){
  const int16_t dynMax = max((int)RAW_MIN_STABLE + (int)DYN_SPAN_MIN, R_curr);
  const float mm = mapLinear_i32(M, RAW_MIN_STABLE, dynMax, 0.0f, (float)cfg.crack_len_mm_x100/100.0f);
  if(mm<0) return 0.0f;
  float L=(float)cfg.crack_len_mm_x100/100.0f; return mm>L?L:mm;
}

CrackMeas readCrack(){
  CrackMeas r{false,0,0,0.0f,false};
  r.present = crackPresent();
  if(!r.present) return r;
  int16_t M = ads.readADC_SingleEnded(0); if(M<0) M=0;
  int16_t R = ads.readADC_SingleEnded(1); if(R<0) R=0;
  r.raw0=M; r.raw1=R;
  if(cfg.ref_mode==REF_MANUAL){ r.mm=mapCrackManual(M); r.auto_used=false; }
  else                         { r.mm=mapCrackAuto_byCH1(M,R); r.auto_used=true; }
  if(r.mm<0) r.mm=0; if(r.mm>65.535f) r.mm=65.535f;
  return r;
}

// ---------- Battery ----------
uint16_t readBattery_mV(){
  analogSetAttenuation(ADC_0db); // global is fine; can switch to pin-specific if needed
  pinMode(PIN_ADC_CTRL,OUTPUT); digitalWrite(PIN_ADC_CTRL,HIGH); delay(2);
  int raw=analogRead(PIN_VBAT_ADC);
  digitalWrite(PIN_ADC_CTRL,LOW);
  return (uint16_t)(VBAT_ADC_FACTOR*raw*1000.0f);
}

// ---------- LoRa helpers ----------
static inline uint8_t mapDR(uint8_t i){ switch(i){case 0:return DR_0;case 1:return DR_1;case 2:return DR_2;case 3:return DR_3;case 4:return DR_4;case 5:return DR_5;default:return DR_3;} }
static inline void applyLoraDR(){ LoRaWAN.setDefaultDR(mapDR(cfg.lora_dr)); }

// Downlink last-RX markers (kept though not shown on OLED)
volatile bool     lora_has_rx=false;
volatile int16_t  lora_last_rssi=0;
volatile int8_t   lora_last_snr=0;
volatile uint32_t lora_last_rx_ms=0;

extern "C" void downLinkDataHandle(McpsIndication_t *ind){
  lora_has_rx=true; lora_last_rssi=ind->Rssi; lora_last_snr=ind->Snr; lora_last_rx_ms=millis();
}

// ---------- OLED ----------
struct OledMarquee{ String text; int y=28; int w=0; int x=0; bool active=false; uint32_t last=0; } apScroll;

void oledMarqueeTick(){
  if(!apScroll.active) return;
  const uint32_t now=millis(); if(now-apScroll.last<MARQUEE_PERIOD_MS) return; apScroll.last=now;
  OLED_Display.setColor(BLACK); OLED_Display.fillRect(0, apScroll.y,128,12); OLED_Display.setColor(WHITE);
  int x=-apScroll.x; OLED_Display.drawString(x,apScroll.y,apScroll.text); OLED_Display.drawString(x+apScroll.w+16,apScroll.y,apScroll.text);
  apScroll.x+=2; if(apScroll.x>apScroll.w+16) apScroll.x=0; OLED_Display.display();
}

static void oledSplash(){
  OLED_Display.init(); OLED_Display.clear(); OLED_Display.setColor(WHITE);
  const int x=(128-logoMKP_width)/2;
  OLED_Display.drawXbm(x,0,logoMKP_width,logoMKP_height,(const uint8_t*)logoMKP_bits);
  OLED_Display.setFont(ArialMT_Plain_10); OLED_Display.setTextAlignment(TEXT_ALIGN_CENTER);
  OLED_Display.drawString(64,logoMKP_height+2,"MARX KRONTAL PARTNER");
  OLED_Display.display(); delay(3000);
}
static void oledShowSSID(const String& ssid){
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0,0,"Access Point");
  OLED_Display.drawString(0,16,"SSID: " + ssid);
  OLED_Display.drawString(0,32,"IP: " + WiFi.softAPIP().toString());
  OLED_Display.display();
  apScroll.active = false; // no marquee to avoid duplication
}

void oledSensorsOnce(){
  sensorsPowerOn();
  CrackMeas cr=readCrack();
  float t=readTempOnceC();
  uint16_t bat=readBattery_mV();
  sensorsPowerOff();

  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);

  int y=0, dy=12;

  // Temperature
  if(isfinite(t)) OLED_Display.drawString(0,y, String("DS18B20  ")+String(t,1)+" C");
  else            OLED_Display.drawString(0,y,"DS18B20  N/C");
  y += dy;

  // ADC raw
  OLED_Display.drawString(0,y, String("ADC M   ")+(cr.present?String(cr.raw0):"N/C")); y+=dy;
  OLED_Display.drawString(0,y, String("ADC R   ")+(cr.present?String(cr.raw1):"N/C")); y+=dy;

  // Crack value
  if(cr.present){
    const char* tag = (cfg.ref_mode==REF_MANUAL) ? " (M)" : " (A)";
    OLED_Display.drawString(0,y, String("Crack   ")+String(cr.mm,3)+" mm"+tag);
  } else {
    OLED_Display.drawString(0,y,"Crack   N/C");
  }
  y += dy;

  // Battery
  OLED_Display.drawString(0,y, String("Battery ")+String(bat)+" mV");

  // No RSSI/SNR
  OLED_Display.display();
}

// ---------- Web ----------
WebServer http(80);

String ipStr(){
  if(WiFi.getMode()==WIFI_AP) return WiFi.softAPIP().toString();
  if(WiFi.status()==WL_CONNECTED) return WiFi.localIP().toString();
  return "-";
}
String contentTypeFor(const String& p){
  if(p.endsWith(".html")) return "text/html; charset=utf-8";
  if(p.endsWith(".svg"))  return "image/svg+xml";
  if(p.endsWith(".json")) return "application/json; charset=utf-8";
  if(p.endsWith(".css"))  return "text/css";
  if(p.endsWith(".js"))   return "application/javascript";
  return "application/octet-stream";
}
bool streamFileFS(const char* p){
  if(!LittleFS.exists(p)) return false;
  File f=LittleFS.open(p,"r"); if(!f) return false;
  http.streamFile(f,contentTypeFor(p)); f.close(); return true;
}

void api_state(){
  StaticJsonDocument<JSON_STATE_DOC> d;
  d["mode"]=(WiFi.getMode()==WIFI_AP)?"AP":"LNS";
  d["ip"]=ipStr();
  d["uptime_s"]=(uint32_t)(millis()/MS_PER_SEC);
  d["heap_free"]=ESP.getFreeHeap();

  sensorsPowerOn();
  CrackMeas cr=readCrack();
  uint16_t batmV=readBattery_mV();
  float tC=readTempOnceC();
  sensorsPowerOff();

  d["crack_present"]=cr.present;
  d["adc_raw"]=cr.present?cr.raw0:0;
  d["adc_ref"]=cr.present?cr.raw1:0;
  if(cr.present){ d["crack_mm"]=cr.mm; d["auto"]=cr.auto_used; } else { d["crack_mm"]=nullptr; d["auto"]=nullptr; }
  d["batt_mV"]=batmV;
  d["DS18B20_Temp"]= isfinite(tC)?tC:(double)NAN;

  JsonObject c=d.createNestedObject("cfg");
  c["minutes"]=cfg.minutes;
  c["devEui_lsb"]=toHex(cfg.devEui,8);
  c["appEui_lsb"]=toHex(cfg.appEui,8);
  c["appKey_msb"]=toHex(cfg.appKey,16);
  c["crack_len_x100"]=cfg.crack_len_mm_x100;

  JsonObject cal=c.createNestedObject("crack_cal");
  cal["r0"]=cfg.crack_r0; cal["mm0"]=(float)cfg.crack_mm0_x100/100.0f;
  cal["r1"]=cfg.crack_r1; cal["mm1"]=(float)cfg.crack_mm1_x100/100.0f;

  c["dr"]=cfg.lora_dr; c["adr"]=cfg.lora_adr;

  JsonObject ts=c.createNestedObject("time_sync");
  ts["enabled"]=cfg.ts_enable; ts["hours"]=cfg.ts_hours; c["align_minute"]=cfg.align_minute;

  JsonObject ref=c.createNestedObject("ref");
  ref["mode"]=(cfg.ref_mode==REF_MANUAL)?"manual":"auto";

  String out; serializeJson(d,out);
  http.send(200,"application/json",out);
}

void api_send(){ deviceState=DEVICE_STATE_SEND; http.send(200,"text/plain","queued"); }
void api_reset(){ http.send(200,"text/plain","restarting"); delay(200); ESP.restart(); }

void api_cfg_get(){
  StaticJsonDocument<JSON_CFG_DOC> d;
  d["devEui_lsb"]=toHex(cfg.devEui,8);
  d["appEui_lsb"]=toHex(cfg.appEui,8);
  d["appKey_msb"]=toHex(cfg.appKey,16);
  d["minutes"]=cfg.minutes;
  d["crack_len_x100"]=cfg.crack_len_mm_x100;

  JsonObject cal=d.createNestedObject("crack_cal");
  cal["r0"]=cfg.crack_r0; cal["mm0"]=(float)cfg.crack_mm0_x100/100.0f;
  cal["r1"]=cfg.crack_r1; cal["mm1"]=(float)cfg.crack_mm1_x100/100.0f;

  d["dr"]=cfg.lora_dr; d["adr"]=cfg.lora_adr;

  JsonObject ts=d.createNestedObject("time_sync");
  ts["enabled"]=cfg.ts_enable; ts["hours"]=cfg.ts_hours; d["align_minute"]=cfg.align_minute;

  JsonObject ref=d.createNestedObject("ref");
  ref["mode"]=(cfg.ref_mode==REF_MANUAL)?"manual":"auto";

  String out; serializeJson(d,out);
  http.send(200,"application/json",out);
}

void api_cfg_lora(){
  if(!http.hasArg("plain")){ http.send(400,"text/plain","bad json"); return; }
  StaticJsonDocument<800> d;
  if(deserializeJson(d,http.arg("plain"))){ http.send(400,"text/plain","bad json"); return; }

  bool ok=true;

  // DevEUI/AppEUI are given and stored in LSB order. Parse straight.
  if(d.containsKey("devEui_lsb")) ok &= parseHexLSB((const char*)d["devEui_lsb"], cfg.devEui, 8);
  if(d.containsKey("appEui_lsb")) ok &= parseHexLSB((const char*)d["appEui_lsb"], cfg.appEui, 8);

  // AppKey is provided in MSB order and must be kept in the same order in memory. Parse straight, no reversal.
  if(d.containsKey("appKey_msb")) ok &= parseHexStraight((const char*)d["appKey_msb"], cfg.appKey, 16);

  if(d.containsKey("minutes")){ cfg.minutes=d["minutes"].as<uint32_t>(); prefs.begin("uhfb",false); prefs.putUInt("minutes",cfg.minutes); prefs.end(); }

  if(d.containsKey("crack_len_mm")){
    float L=d["crack_len_mm"].as<float>();
    if(!isfinite(L)||L<0.01f||L>1000.0f){ http.send(400,"text/plain","len range"); return; }
    cfg.crack_len_mm_x100=(uint16_t)lrintf(L*100.0f);
    if(cfg.crack_mm1_x100<cfg.crack_len_mm_x100) cfg.crack_mm1_x100=cfg.crack_len_mm_x100;
    prefs.begin("uhfb",false); prefs.putUShort("cr_len_x100",cfg.crack_len_mm_x100); prefs.putUShort("cr_mm1",cfg.crack_mm1_x100); prefs.end();
  }

  if(d.containsKey("crack_cal")){
    JsonObject cal=d["crack_cal"].as<JsonObject>();
    bool okc=true;
    if(cal.containsKey("r0"))  cfg.crack_r0=cal["r0"].as<int>();
    if(cal.containsKey("r1"))  cfg.crack_r1=cal["r1"].as<int>();
    if(cal.containsKey("mm0")){ float v=cal["mm0"].as<float>(); okc&=isfinite(v)&&v>=0&&v<1000.0f; if(okc) cfg.crack_mm0_x100=(uint16_t)lrintf(v*100.0f); }
    if(cal.containsKey("mm1")){ float v=cal["mm1"].as<float>(); okc&=isfinite(v)&&v>=0&&v<=1000.0f; if(okc) cfg.crack_mm1_x100=(uint16_t)lrintf(v*100.0f); }
    if(!okc || cfg.crack_r0==cfg.crack_r1){ http.send(400,"text/plain","cal error"); return; }
    prefs.begin("uhfb",false);
    prefs.putShort("cr_r0",cfg.crack_r0); prefs.putShort("cr_r1",cfg.crack_r1);
    prefs.putUShort("cr_mm0",cfg.crack_mm0_x100); prefs.putUShort("cr_mm1",cfg.crack_mm1_x100);
    prefs.end();
  }

  if(d.containsKey("dr")){
    int v=d["dr"].as<int>(); if(v<0||v>5){ http.send(400,"text/plain","dr range"); return; }
    cfg.lora_dr=(uint8_t)v; cfg.lora_adr=false;
    prefs.begin("uhfb",false); prefs.putUChar("lora_dr",cfg.lora_dr); prefs.putBool("lora_adr",cfg.lora_adr); prefs.end();
  }
  if(d.containsKey("adr")){
    cfg.lora_adr=d["adr"].as<bool>();
    prefs.begin("uhfb",false); prefs.putBool("lora_adr",cfg.lora_adr); prefs.end();
    loraWanAdr = cfg.lora_adr;
  }

  if(d.containsKey("time_sync")){
    JsonObject ts=d["time_sync"].as<JsonObject>();
    if(ts.containsKey("enabled")){ cfg.ts_enable=ts["enabled"].as<bool>(); }
    if(ts.containsKey("hours")){ int h=ts["hours"].as<int>(); if(h<1||h>168){ http.send(400,"text/plain","ts hours range"); return; } cfg.ts_hours=(uint16_t)h; }
    prefs.begin("uhfb",false); prefs.putBool("ts_en",cfg.ts_enable); prefs.putUShort("ts_h",cfg.ts_hours); prefs.end();
  }
  if(d.containsKey("align_minute")){ cfg.align_minute=d["align_minute"].as<bool>(); prefs.begin("uhfb",false); prefs.putBool("align_min",cfg.align_minute); prefs.end(); }

  if(d.containsKey("ref")){
    JsonObject rj=d["ref"].as<JsonObject>(); String m=rj["mode"]|"auto";
    cfg.ref_mode = (m=="manual") ? REF_MANUAL : REF_AUTO;
    prefs.begin("uhfb",false); prefs.putUChar("ref_mode",cfg.ref_mode); prefs.end();
  }

  if(!ok){ http.send(400,"text/plain","hex error"); return; }
  nvsPut("devEui",cfg.devEui,8); nvsPut("appEui",cfg.appEui,8); nvsPut("appKey",cfg.appKey,16);

  http.send(200,"text/plain","ok"); delay(200); ESP.restart();
}

void api_deveui_get(){
  uint8_t chipMSB[8]; devEuiFromChipMSB(chipMSB);
  uint8_t chipLSB[8]; for(int i=0;i<8;i++) chipLSB[i]=chipMSB[7-i];
  StaticJsonDocument<256> d;
  d["chip_devEui_msb"]=toHex(chipMSB,8);
  d["chip_devEui_lsb"]=toHex(chipLSB,8);
  d["stored_devEui_lsb"]=toHex(cfg.devEui,8);
  String out; serializeJson(d,out);
  http.send(200,"application/json",out);
}
void api_deveui_post(){
  if(!http.hasArg("plain")){ http.send(400,"text/plain","bad json"); return; }
  StaticJsonDocument<128> d;
  if(deserializeJson(d,http.arg("plain"))){ http.send(400,"text/plain","bad json"); return; }
  if(d.containsKey("use") && String((const char*)d["use"])=="chip"){
    uint8_t chipMSB[8]; devEuiFromChipMSB(chipMSB);
    for(int i=0;i<8;i++) cfg.devEui[i]=chipMSB[7-i];
    nvsPut("devEui",cfg.devEui,8);
    http.send(200,"text/plain","ok"); delay(200); ESP.restart(); return;
  }
  if(d.containsKey("devEui_lsb")){
    String h=d["devEui_lsb"].as<String>();
    if(!parseHexLSB(h,cfg.devEui,8)){ http.send(400,"text/plain","hex error"); return; }
    nvsPut("devEui",cfg.devEui,8);
    http.send(200,"text/plain","ok"); delay(200); ESP.restart(); return;
  }
  http.send(400,"text/plain","args");
}

void attachHttpFS(){
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

// ---------- Uplink payload (8 bytes) ----------
void prepareTxFrame(uint8_t){
  sensorsPowerOn();
  CrackMeas cr=readCrack();
  int16_t t=readTemp_c_x100();
  uint16_t vb=readBattery_mV();
  sensorsPowerOff();

  uint16_t cr1000=(uint16_t)lrintf(cr.mm*1000.0f);
  int16_t  adcRaw = cr.present ? cr.raw0 : 0;

  appDataSize=8;
  appData[0]=t>>8;      appData[1]=t;
  appData[2]=adcRaw>>8; appData[3]=adcRaw;
  appData[4]=cr1000>>8; appData[5]=cr1000;
  appData[6]=vb>>8;     appData[7]=vb;
}

// ---------- App ----------
enum OledPhase{ OLED_LOGO, OLED_SSID, OLED_SENS };
OledPhase oledPhase=OLED_LOGO; uint32_t oledPhaseStart=0;

void setup(){
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD,SLOW_CLK_TPYE);

  pinMode(PIN_MODE_OUT,OUTPUT); digitalWrite(PIN_MODE_OUT,LOW);
  pinMode(PIN_MODE_IN,INPUT_PULLUP); delay(5);
  pinMode(PIN_CRACK_PRESENT,INPUT_PULLUP);

  for(int i=0;i<5;i++){ apMode |= (digitalRead(PIN_MODE_IN)==LOW); delay(2); }

  analogReadResolution(12);
  loadCfg();

  memcpy(devEui,cfg.devEui,8); memcpy(appEui,cfg.appEui,8); memcpy(appKey,cfg.appKey,16);
  memcpy(DevEui,devEui,8); memcpy(AppEui,appEui,8); memcpy(AppKey,appKey,16);

  uint8_t chipMSB[8]; devEuiFromChipMSB(chipMSB);
  uint8_t chipLSB[8]; for(int i=0;i<8;i++) chipLSB[i]=chipMSB[7-i];
  Serial.printf("DevEUI LSB=%s  AppEUI LSB=%s  AppKey MSB=%s\n",
    toHex(DevEui,8).c_str(), toHex(AppEui,8).c_str(), toHex(AppKey,16).c_str());
  Serial.printf("Chip-MSB=%s  Chip-LSB=%s  Stored-LSB=%s\n",
    toHex(chipMSB,8).c_str(), toHex(chipLSB,8).c_str(), toHex(cfg.devEui,8).c_str());

  if(apMode){
    isTxConfirmed=true; confirmedNbTrials=CONF_TRIALS_AP;
    WiFi.mode(WIFI_AP);
    bool fsOk=LittleFS.begin(true);
    if(fsOk) attachHttpFS(); else Serial.println("[FS] mount failed");
    WiFi.setTxPower(WIFI_POWER_7dBm);
    String ssid="Riss-"+devEuiSuffix6LSB();
    WiFi.softAP(ssid.c_str(),AP_PSK);
    pinMode(Vext,OUTPUT); digitalWrite(Vext,LOW);
    oledSplash(); oledPhase=OLED_SSID; oledPhaseStart=millis(); oledShowSSID(ssid);
  }else{
    isTxConfirmed=false; confirmedNbTrials=CONF_TRIALS_FIELD;
    WiFi.persistent(false); WiFi.disconnect(true,true); WiFi.mode(WIFI_OFF);
#if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
    btStop();
#endif
    pinMode(Vext,OUTPUT); digitalWrite(Vext,HIGH);
  }

  deviceState=DEVICE_STATE_INIT;
}

void loop(){
  static uint32_t appTxDutyCycleCached=0;
  if(appTxDutyCycleCached==0) appTxDutyCycleCached=60000UL*cfg.minutes;

  if(apMode){
    http.handleClient(); oledMarqueeTick();
    if(oledPhase==OLED_SSID && millis()-oledPhaseStart>=3000){ apScroll.active=false; oledPhase=OLED_SENS; oledPhaseStart=millis(); oledSensorsOnce(); }
    static uint32_t tmr=0; if(oledPhase==OLED_SENS && millis()-tmr>1000){ tmr=millis(); oledSensorsOnce(); }
  }

  switch(deviceState){
    case DEVICE_STATE_INIT:
      memcpy(DevEui,cfg.devEui,8); memcpy(AppEui,cfg.appEui,8); memcpy(AppKey,cfg.appKey,16);
      loraWanAdr = cfg.lora_adr;        // keep ADR in sync with config
      LoRaWAN.init(CLASS_A, ACTIVE_REGION);
      applyLoraDR();
      deviceState=DEVICE_STATE_JOIN; break;
    case DEVICE_STATE_JOIN:
      LoRaWAN.join(); break;
    case DEVICE_STATE_SEND:
      prepareTxFrame(UPLINK_PORT);
      LoRaWAN.send();
      deviceState=DEVICE_STATE_CYCLE; break;
    case DEVICE_STATE_CYCLE:{
      uint32_t txDutyCycleTime;
      if(cfg.align_minute){
        const uint32_t period=cfg.minutes*SEC_PER_MIN;
        const uint32_t nowS=now_sec();
        const uint32_t rem=nowS%period;
        const uint32_t waitSec=(rem==0)?period:(period-rem);
        txDutyCycleTime=waitSec*MS_PER_SEC;
      }else{
        txDutyCycleTime=appTxDutyCycleCached + randr(-APP_TX_DUTYCYCLE_RND, APP_TX_DUTYCYCLE_RND);
      }
      LoRaWAN.cycle(txDutyCycleTime);
      deviceState=DEVICE_STATE_SLEEP; break;
    }
    case DEVICE_STATE_SLEEP:
      sensorsPowerOff();
      if(apMode) LoRaWAN.sleep(CLASS_C); else LoRaWAN.sleep(CLASS_A);
      break;
    default: deviceState=DEVICE_STATE_INIT; break;
  }
}
