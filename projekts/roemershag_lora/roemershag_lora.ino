

#include "LoRaWan_APP.h"
#include <WiFi.h>
#include <WebServer.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <LittleFS.h>

// ===== OLED (только в AP) =====
#include "HT_SSD1306Wire.h"
SSD1306Wire OLED_Display(0x3c, 400000, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);

// ===== Pins (Heltec V3 / ESP32-S3) =====
#define ONE_WIRE_BUS 4
#define ADC_CRACK    2
#define VBAT_Read    1
#define ADC_Ctrl     37
#define PIN_MODE_OUT 45
#define PIN_MODE_IN  46

// ===== Vext =====
static inline void VextON(){ pinMode(Vext, OUTPUT); digitalWrite(Vext, LOW); }
static inline void VextOFF(){ pinMode(Vext, OUTPUT); digitalWrite(Vext, HIGH); }

// ===== OneWire + DallasTemperature =====
#include <OneWire.h>
#include <DallasTemperature.h>
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

// Вместо DeviceAddress — обычный 8-байтовый буфер
uint8_t firstDs[8] = {0};
bool firstDsFound = false;
const uint8_t DS_RES_BITS = 10;               // ~187.5ms

// ===== NVS конфиг =====
Preferences prefs;
struct Cfg {
  String ssid, wifi_pw;
  uint8_t devEui[8]  = { 0x63,0x1E,0x56,0x18,0xDF,0x8E,0xB3,0xD4 };
  uint8_t appEui[8]  = { 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 };
  uint8_t appKey[16] = { 0x63,0x6A,0xC0,0x9B,0x24,0x82,0x4A,0x47, 0x30,0x32,0x80,0x58,0xCE,0x63,0x6A,0xDF };
  uint32_t minutes   = 2;
} cfg;

void loadCfg(){
  prefs.begin("uhfb", true);
  cfg.minutes  = prefs.getUInt("minutes", cfg.minutes);
  cfg.ssid     = prefs.getString("ssid", "");
  cfg.wifi_pw  = prefs.getString("wifi_pw", "");
  prefs.getBytes("devEui", cfg.devEui, 8);
  prefs.getBytes("appEui", cfg.appEui, 8);
  prefs.getBytes("appKey", cfg.appKey,16);
  prefs.end();
}
void saveKVu(const char* k, uint32_t v){ prefs.begin("uhfb", false); prefs.putUInt(k, v); prefs.end(); }
void saveBytes(const char* k, const uint8_t* d, size_t n){ prefs.begin("uhfb", false); prefs.putBytes(k, d, n); prefs.end(); }

// ===== LoRaWAN (глобалы, которых требует Heltec LoRaWan_APP) =====
uint16_t userChannelsMask[6]={0x00FF,0,0,0,0,0};
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;   // REGION_EU868 у Tools
DeviceClass_t   loraWanClass  = CLASS_A;
bool overTheAirActivation = true;
bool loraWanAdr           = true;
bool isTxConfirmed        = false;
uint8_t appPort           = 2;
uint8_t confirmedNbTrials = 1;
uint32_t appTxDutyCycle   = 60000UL * 2;

// <<< ВАЖНО: глобальные символы, которые линкуются из LoRaWan_APP.cpp >>>
uint8_t devEui[8]  = {0};
uint8_t appEui[8]  = {0};
uint8_t appKey[16] = {0};
// Для ABP — чтобы линковщик был доволен (при OTAA остаются нулями)
uint8_t  nwkSKey[16] = {0};
uint8_t  appSKey[16] = {0};
uint32_t devAddr     = 0;

// ===== Утилиты =====
String toHex(const uint8_t* v, size_t n){ char b[3]; String s; s.reserve(n*2); for(size_t i=0;i<n;i++){ sprintf(b,"%02X", v[i]); s+=b; } return s; }
String toHexLSB(const uint8_t* v, size_t n){ char b[3]; String s; s.reserve(n*2); for(int i=n-1;i>=0;i--){ sprintf(b,"%02X", v[i]); s+=b; } return s; }
bool parseHex(const String& s, uint8_t* out, size_t n){
  if(s.length()!=n*2) return false;
  for(size_t i=0;i<n;i++){ char b[3]={ (char)s[2*i], (char)s[2*i+1], 0}; out[i]=(uint8_t)strtoul(b,nullptr,16); }
  return true;
}
void devEuiFromChip(uint8_t out[8]){
  uint64_t mac = ESP.getEfuseMac();
  uint8_t m[6]; for(int i=0;i<6;i++) m[i]=(mac>>(8*(5-i)))&0xFF;
  out[0]=m[0]; out[1]=m[1]; out[2]=m[2]; out[3]=0xFF; out[4]=0xFE; out[5]=m[3]; out[6]=m[4]; out[7]=m[5];
}
String devEuiSuffix6(){ char buf[7]; sprintf(buf,"%02X%02X%02X", cfg.devEui[5], cfg.devEui[6], cfg.devEui[7]); return String(buf); }

// ===== DS18B20: найти первый датчик =====
bool dsFindFirst(uint8_t addrOut[8]){
  if (sensors.getAddress(addrOut, 0)) return true; // первый по индексу
  OneWire ow(ONE_WIRE_BUS);
  uint8_t addr[8];
  ow.reset_search();
  while (ow.search(addr)) {
    if (addr[0] == 0x28) { memcpy(addrOut, addr, 8); return true; }
  }
  return false;
}

// ===== Измерения =====
uint16_t readBattery_mV(){
  const float factor = 1.0f/4096.0f/0.210282997855762f; // R1=390k, R2=100k
  analogSetAttenuation(ADC_0db);
  pinMode(ADC_Ctrl, OUTPUT);
  digitalWrite(ADC_Ctrl, HIGH);
  delay(2);
  int raw = analogRead(VBAT_Read);
  digitalWrite(ADC_Ctrl, LOW);
  return (uint16_t)(factor * raw * 1000.0f);
}

uint16_t readCrack_mm_x100(int* rawOut){
  analogSetAttenuation(ADC_11db);
  delay(2);
  int raw = analogRead(ADC_CRACK);
  if(rawOut) *rawOut = raw;
  float mm = (raw * (3.3f/4096.0f)) / 0.33f;
  int v = (int)(mm*100.0f); if(v<0) v=0;
  return (uint16_t)v;
}

// Температура для пейлоада (DS18B20) — БЕЗ VextON/OFF!
int16_t readTemp_c_x100(){
  if (!firstDsFound) return 5000;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200); // 10-бит ~187.5мс
  float tC = sensors.getTempC(firstDs);
  if (tC <= -127.0f || tC >= 125.0f) return 5000;  // ошибка/вне диапазона
  int v = (int)(tC * 100.0f) + 5000;               // сдвиг +5000
  if (v < 0) v = 0;
  return (int16_t)v;
}

// Разовое чтение для UI (float °C) — БЕЗ VextON/OFF!
float readTempOnceC(){
  if (!firstDsFound) return NAN;
  sensors.requestTemperaturesByAddress(firstDs);
  delay(200);
  float tC = sensors.getTempC(firstDs);
  if (tC <= -127.0f || tC >= 125.0f) return NAN;
  return tC;
}

// ===== Payload =====
static void prepareTxFrame(uint8_t){
  int adcRaw=0;
  int16_t t   = readTemp_c_x100();
  uint16_t cr = readCrack_mm_x100(&adcRaw);
  uint16_t vb = readBattery_mV();
  appDataSize = 8;
  appData[0]=t>>8;      appData[1]=t;
  appData[2]=adcRaw>>8; appData[3]=adcRaw;
  appData[4]=cr>>8;     appData[5]=cr;
  appData[6]=vb>>8;     appData[7]=vb;
}

// ===== Web (AP) =====
WebServer http(80);

String ipStr(){
  if(WiFi.getMode()==WIFI_AP) return WiFi.softAPIP().toString();
  if(WiFi.status()==WL_CONNECTED) return WiFi.localIP().toString();
  return "-";
}
String contentTypeFor(const String& path){
  if(path.endsWith(".html")) return "text/html; charset=utf-8";
  if(path.endsWith(".svg"))  return "image/svg+xml";
  if(path.endsWith(".json")) return "application/json; charset=utf-8";
  if(path.endsWith(".css"))  return "text/css";
  if(path.endsWith(".js"))   return "application/javascript";
  return "application/octet-stream";
}
bool streamFileFS(const char* p){
  if(!LittleFS.exists(p)) return false;
  File f = LittleFS.open(p, "r");
  if(!f) return false;
  http.streamFile(f, contentTypeFor(p));
  f.close();
  return true;
}

void api_state(){
  StaticJsonDocument<768> d;
  d["mode"] = (WiFi.getMode()==WIFI_AP) ? "AP" : "LNS";
  d["ip"]= ipStr();
  d["uptime_s"]= (uint32_t)(millis()/1000);
  d["heap_free"]= ESP.getFreeHeap();

  float tC = readTempOnceC();
  if (isfinite(tC)) d["DS18B20_Temp"] = tC;
  else              d["DS18B20_Temp"] = nullptr;

  JsonObject c = d.createNestedObject("cfg");
  c["minutes"]= cfg.minutes;
  c["devEui"]= toHex(cfg.devEui,8);
  c["appEui"]= toHex(cfg.appEui,8);
  c["appKey"]= toHex(cfg.appKey,16);

  String out; serializeJson(d,out);
  http.send(200,"application/json",out);
}
void api_send(){ deviceState = DEVICE_STATE_SEND; http.send(200,"text/plain","queued"); }
void api_reset(){ http.send(200,"text/plain","restarting"); delay(200); ESP.restart(); }

void api_cfg_get(){
  StaticJsonDocument<512> d;
  d["devEui"]= toHex(cfg.devEui,8);
  d["appEui"]= toHex(cfg.appEui,8);
  d["appKey"]= toHex(cfg.appKey,16);
  d["minutes"]= cfg.minutes;
  String out; serializeJson(d,out);
  http.send(200,"application/json",out);
}
void api_cfg_lora(){
  if(!http.hasArg("plain")){ http.send(400,"text/plain","bad json"); return; }
  StaticJsonDocument<256> d; if(deserializeJson(d,http.arg("plain"))){ http.send(400,"text/plain","bad json"); return; }
  bool ok=true;
  if(d.containsKey("devEui")) ok &= parseHex((const char*)d["devEui"], cfg.devEui, 8);
  if(d.containsKey("appEui")) ok &= parseHex((const char*)d["appEui"], cfg.appEui, 8);
  if(d.containsKey("appKey")) ok &= parseHex((const char*)d["appKey"], cfg.appKey,16);
  if(d.containsKey("minutes")){ cfg.minutes = d["minutes"].as<uint32_t>(); saveKVu("minutes",cfg.minutes); }
  if(!ok){ http.send(400,"text/plain","hex error"); return; }
  saveBytes("devEui",cfg.devEui,8); saveBytes("appEui",cfg.appEui,8); saveBytes("appKey",cfg.appKey,16);
  http.send(200,"text/plain","ok"); delay(200); ESP.restart();
}
void api_deveui_get(){
  uint8_t chip[8]; devEuiFromChip(chip);
  StaticJsonDocument<256> d;
  d["chip_devEui_msb"]= toHex(chip,8);
  d["chip_devEui_lsb"]= toHexLSB(chip,8);
  d["stored_devEui_msb"]= toHex(cfg.devEui,8);
  String out; serializeJson(d,out);
  http.send(200,"application/json",out);
}
void api_deveui_post(){
  if(!http.hasArg("plain")){ http.send(400,"text/plain","bad json"); return; }
  StaticJsonDocument<128> d; if(deserializeJson(d,http.arg("plain"))){ http.send(400,"text/plain","bad json"); return; }
  if(d.containsKey("use") && String((const char*)d["use"])=="chip"){
    devEuiFromChip(cfg.devEui); saveBytes("devEui",cfg.devEui,8);
    http.send(200,"text/plain","ok"); delay(200); ESP.restart(); return;
  }
  if(d.containsKey("devEui")){
    String h = d["devEui"].as<String>();
    if(!parseHex(h, cfg.devEui, 8)){ http.send(400,"text/plain","hex error"); return; }
    saveBytes("devEui",cfg.devEui,8);
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

// ===== OLED в AP =====
void oledBoot(const String& ssid){
  OLED_Display.init();
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 0, "MARX KRONTAL PARTNER");
  OLED_Display.drawString(0, 14, "UHFB Carport");
  OLED_Display.drawString(0, 28, "AP: " + ssid);
  OLED_Display.display();
}

// Обновление «нижней части» без полного clear() → без мигания
void oledSensorsOnce(){
  float t = readTempOnceC();
  int adcRaw=0;
  uint16_t crack = readCrack_mm_x100(&adcRaw);
  uint16_t bat = readBattery_mV();

  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);

  // Затираем только область с данными (y: 14..63)
  OLED_Display.setColor(BLACK);
  OLED_Display.fillRect(0, 14, 128, 50);
  OLED_Display.setColor(WHITE);

  if(isfinite(t)) OLED_Display.drawString(0, 14, String("DS18B20  ")+String(t,1)+" C");
  else            OLED_Display.drawString(0, 14, "DS18B20  n/a");
  OLED_Display.drawString(0, 26, String("ADC      ")+String(adcRaw));
  OLED_Display.drawString(0, 38, String("Crack    ")+String(crack/100.0f,2)+" mm");
  OLED_Display.drawString(0, 50, String("Battery  ")+String(bat)+" mV");

  OLED_Display.display();
}

// ===== Режим =====
bool apMode=false;

void setup(){
  Serial.begin(115200);
  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

  pinMode(PIN_MODE_OUT, OUTPUT); digitalWrite(PIN_MODE_OUT, LOW);
  pinMode(PIN_MODE_IN, INPUT_PULLUP); delay(5);
  for(int i=0;i<5;i++){ apMode |= (digitalRead(PIN_MODE_IN)==LOW); delay(2); }

  analogReadResolution(12);
  loadCfg();
  appTxDutyCycle = 60000UL * cfg.minutes;

  // Синхронизируем NVS-конфиг в глобалы, которые ждёт библиотека
  memcpy(devEui,  cfg.devEui,  sizeof(devEui));
  memcpy(appEui,  cfg.appEui,  sizeof(appEui));
  memcpy(appKey,  cfg.appKey,  sizeof(appKey));

  // OneWire + Dallas init
  sensors.begin();
  firstDsFound = dsFindFirst(firstDs);
  if (firstDsFound) sensors.setResolution(firstDs, DS_RES_BITS);

  if(apMode){
    WiFi.mode(WIFI_AP);
    String ssid = "Rissmonitoring-" + devEuiSuffix6();

    bool fsOk = LittleFS.begin(true);   // true = формат при ошибке
    if (!fsOk) {
      Serial.println("[FS] LittleFS mount failed even after format");
    } else {
      attachHttpFS();
    }

    // Понижаем мощность AP, чтобы убрать пиковые просадки (опционально)
    WiFi.setTxPower(WIFI_POWER_7dBm);
    WiFi.softAP(ssid.c_str(), "12345678");

    // ДЕРЖИМ Vext ПОСТОЯННО ВКЛ в AP, чтобы OLED не мигал
    VextON(); delay(5);

    OLED_Display.init();
    oledBoot(ssid); delay(500);
    oledSensorsOnce();
  }else{
    WiFi.mode(WIFI_OFF);
    #if defined(CONFIG_BT_ENABLED) && CONFIG_BT_ENABLED
      btStop();
    #endif
    VextOFF();
  }

  // Явно стартуем стейт-машину
  deviceState = DEVICE_STATE_INIT;
}

void loop(){
  if(apMode){
    http.handleClient();
    static uint32_t tmr=0;
    if(millis()-tmr>5000){ // обновляем раз в 5с без полного clear()
      tmr=millis();
      oledSensorsOnce();
    }
  }

  switch (deviceState) {
    case DEVICE_STATE_INIT:
      // КАНОНИЧЕСКАЯ инициализация Heltec в INIT
      LoRaWAN.init(loraWanClass, loraWanRegion);
      LoRaWAN.setDefaultDR(3);
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
      if (apMode) {
        LoRaWAN.sleep(CLASS_C);
        break;
      } else {
        // Обычное поведение в LNS-режиме
        LoRaWAN.sleep(loraWanClass);
        break;
      }
      //LoRaWAN.sleep(loraWanClass);
      //break;

    default:
      deviceState = DEVICE_STATE_INIT;
      break;
  }
}
