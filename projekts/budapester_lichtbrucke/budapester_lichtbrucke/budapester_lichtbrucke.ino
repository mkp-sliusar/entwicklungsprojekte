#include <WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_NeoPixel.h>
#include <WebServer.h>
#include <ArduinoJson.h>
#include <LittleFS.h>
#include <ESPmDNS.h>
#include <DNSServer.h>
#include <vector>
#include <algorithm>
#include <ctype.h>

// ===== Wi-Fi (fallback) =====
const char* ssid1 = "Test";
const char* password1 = "1234567890";
const char* ssid2 = "AERO-MK-Guest";
const char* password2 = "%Gast#2020$MK!";
const char* ssid3 = "test";
const char* password3 = "12345678";

// ===== AP для налаштувань =====
const char* AP_SSID = "budapester_lichtbrucke";
const char* AP_PASS = "lichtbrucke";

// ===== MQTT =====
const char* mqtt_server   = "mqtt.marxkrontal.com";
const int   mqtt_port     = 1883;
const char* mqtt_user     = "test-monitoring";
const char* mqtt_password = "+aR7gr*G&v4]SPS6\\q,W";
const char* mqtt_topic    = "test-monitoring/Lichtbrucke";

static const uint16_t MQTT_BUFFER_BYTES   = 256;
static const uint16_t MQTT_KEEPALIVE_SEC  = 30;
static const uint16_t MQTT_SOCKET_TO_SEC  = 15;

// ===== LED (NeoPixel) =====
#define LED_PIN   2
#define NUM_LEDS  122
Adafruit_NeoPixel strip(NUM_LEDS, LED_PIN, NEO_GRB + NEO_KHZ800);

// ===== Стан LED =====
struct ActiveLED { int channel; float db; uint8_t brightness; unsigned long lastUpdate; bool active; };
ActiveLED activeLeds[NUM_LEDS];

// ===== Правила кольорів =====
struct LedRule { int maxDb; uint32_t color; };
std::vector<LedRule> gRules;

// ===== Зберігання профілів Wi-Fi =====
struct WiFiProfile { String ssid; String pass; };

// ===== Нетворк/сервіси =====
WiFiClient espClient;
PubSubClient client(espClient);
WebServer server(80);
DNSServer dnsServer;
const byte DNS_PORT = 53;
bool httpStarted = false;
bool mdnsStarted = false;

// ===== Таймери/ліміти =====
static const wifi_power_t WIFI_TX_MAX = WIFI_POWER_19_5dBm;
static const uint16_t LED_UPDATE_MS          = 100;
static const uint16_t LED_FADE_STEP_MS       = 150;
static const uint8_t  LED_FADE_DEC           = 5;
static const uint16_t WIFI_CHECK_MS          = 5000;
static const uint16_t MQTT_RATE_WINDOW_MS    = 1000;
static const uint16_t MQTT_RATE_LIMIT_MSG_S  = 400;
static const uint16_t WIFI_CONNECT_TIMEOUT_MS= 10000;
static const uint8_t  WIFI_MAX_ATTEMPTS_SSID = 3;

// ===== Вікно автоналаштування AP =====
static const unsigned long AP_WINDOW_MS = 10UL * 60UL * 1000UL;
unsigned long apWindowStart = 0;
bool apWindowOpen = false;

unsigned int  mqttMessageCount   = 0;
unsigned long lastMqttCountTime  = 0;
unsigned long lastLedUpdate      = 0;
unsigned long lastWiFiCheck      = 0;
unsigned long lastMqttConnTry    = 0;
volatile unsigned int mqttDropped = 0;

// ===== LittleFS =====
const char* WIFI_FILE   = "/wifi.json";
const char* LEDMAP_FILE = "/ledmap.json";

// ===== Утиліти =====
static inline uint32_t wheel(Adafruit_NeoPixel& s, uint8_t pos){
  pos = 255 - pos;
  if (pos < 85)  return s.Color(255 - pos * 3, 0, pos * 3);
  if (pos < 170){ pos -= 85; return s.Color(0, pos * 3, 255 - pos * 3); }
  pos -= 170;     return s.Color(pos * 3, 255 - pos * 3, 0);
}
void rainbowBounce(uint8_t frameDelay=6, uint8_t passes=1){
  for(uint8_t p=0;p<passes;p++){
    for(int start=0; start<NUM_LEDS+10; start++){
      strip.clear();
      for(int i=0;i<10;i++){ int pos=start-i; if(pos>=0 && pos<NUM_LEDS){ uint8_t hue=(i*256/10)&0xFF; strip.setPixelColor(pos,wheel(strip,hue)); } }
      strip.show(); delay(frameDelay);
    }
    for(int start=NUM_LEDS-1; start>=-10; start--){
      strip.clear();
      for(int i=0;i<10;i++){ int pos=start+i; if(pos>=0 && pos<NUM_LEDS){ uint8_t hue=(i*256/10)&0xFF; strip.setPixelColor(pos,wheel(strip,hue)); } }
      strip.show(); delay(frameDelay);
    }
  }
  strip.clear(); strip.show();
}
void indicateWiFiAttempt(int idx){ for(int i=0;i<=idx && i<NUM_LEDS;i++) strip.setPixelColor(i, strip.Color(0,0,255)); strip.show(); }
void indicateWiFiSuccess(int idx){ for(int i=0;i<=idx && i<NUM_LEDS;i++) strip.setPixelColor(i, strip.Color(0,255,0)); strip.show(); delay(150); }
void indicateMQTT(){ for(int i=0;i<5 && i<NUM_LEDS;i++) strip.setPixelColor(i, strip.Color(255,165,0)); strip.show(); delay(120); }
void indicateSystemReady(){ rainbowBounce(6,1); }

// ===== Прототипи =====
bool loadWiFiProfiles(std::vector<WiFiProfile>& out, String& last);
void saveWiFiProfiles(const std::vector<WiFiProfile>& arr, const String& last);
bool loadLedMap(DynamicJsonDocument& out);
void saveLedMap(const DynamicJsonDocument& doc);
void try_connect_known(const std::vector<WiFiProfile>& plist);
void setup_web_routes();
void start_http_server_if_needed();
void start_mdns_if_needed();
void start_ap();
void manage_ap_window();
void loadRulesFromFS();

// ===== Парсер кольору "#RRGGBB" =====
static uint32_t parseHexColor(const String& hex){
  if(hex.length() < 7) return strip.Color(0,255,0);
  auto v=[](char c)->uint8_t{
    if(c>='0'&&c<='9') return c-'0';
    c = (char)toupper((unsigned char)c);
    if(c>='A'&&c<='F') return 10 + (c-'A');
    return 0;
  };
  auto toByte=[&](char hi, char lo)->uint8_t{ return (v(hi)<<4)|v(lo); };
  uint8_t r=toByte(hex[1],hex[2]), g=toByte(hex[3],hex[4]), b=toByte(hex[5],hex[6]);
  return strip.Color(r,g,b);
}

// ===== Колір за dB з урахуванням яскравості =====
static uint32_t colorForDb(float db, uint8_t brightness){
  if(gRules.empty()) return strip.Color(0, brightness, 0);
  for(const auto& r: gRules){
    if(db <= r.maxDb){
      uint8_t R=(r.color>>16)&0xFF, G=(r.color>>8)&0xFF, B=r.color&0xFF;
      auto scale=[&](uint8_t x)->uint8_t{ return (uint16_t)x*brightness/255; };
      return strip.Color(scale(R), scale(G), scale(B));
    }
  }
  const auto& last = gRules.back();
  uint8_t R=(last.color>>16)&0xFF, G=(last.color>>8)&0xFF, B=last.color&0xFF;
  auto scale=[&](uint8_t x)->uint8_t{ return (uint16_t)x*brightness/255; };
  return strip.Color(scale(R), scale(G), scale(B));
}

// ===== Wi-Fi профілі =====
bool loadWiFiProfiles(std::vector<WiFiProfile>& out, String& last){
  out.clear(); last="";
  if(!LittleFS.exists(WIFI_FILE)) return false;
  File f=LittleFS.open(WIFI_FILE,"r"); if(!f) return false;
  DynamicJsonDocument doc(2048); DeserializationError e = deserializeJson(doc, f); f.close();
  if(e) return false;
  JsonArray arr = doc["profiles"].as<JsonArray>();
  if(arr){
    for(JsonObject o: arr){
      WiFiProfile p; p.ssid=o["ssid"].as<String>(); p.pass=o["pass"].as<String>();
      if(p.ssid.length()) out.push_back(p);
    }
  }
  if(doc["last"].is<const char*>()) last = doc["last"].as<String>();
  return !out.empty();
}
void saveWiFiProfiles(const std::vector<WiFiProfile>& arr, const String& last){
  DynamicJsonDocument doc(2048);
  JsonArray a = doc.createNestedArray("profiles");
  for(const auto &p: arr){ JsonObject o=a.createNestedObject(); o["ssid"]=p.ssid; o["pass"]=p.pass; }
  if(last.length()) doc["last"]=last;
  File f=LittleFS.open(WIFI_FILE,"w"); serializeJson(doc,f); f.close();
}

// ===== LED map файли ↔ кеш правил =====
bool loadLedMap(DynamicJsonDocument& out){
  out.clear();
  if(!LittleFS.exists(LEDMAP_FILE)) return false;
  File f=LittleFS.open(LEDMAP_FILE,"r"); if(!f) return false;
  DeserializationError e=deserializeJson(out,f); f.close();
  return !e;
}
void saveLedMap(const DynamicJsonDocument& doc){
  File f=LittleFS.open(LEDMAP_FILE,"w"); serializeJson(doc,f); f.close();
}
void loadRulesFromFS(){
  gRules.clear();
  DynamicJsonDocument doc(4096);
  if(!loadLedMap(doc)){
    JsonArray a = doc.createNestedArray("rules");
    JsonObject r1=a.createNestedObject(); r1["max"]=90;  r1["color"]="#00ff00";
    JsonObject r2=a.createNestedObject(); r2["max"]=100; r2["color"]="#ffff00";
    JsonObject r3=a.createNestedObject(); r3["max"]=110; r3["color"]="#ff0000";
  }
  JsonArray arr = doc["rules"].as<JsonArray>();
  if(arr){
    for(JsonObject o: arr){
      int m = o["max"] | o["db"] | 0;
      String c = o["color"] | "#00ff00";
      gRules.push_back({m, parseHexColor(c)});
    }
    std::sort(gRules.begin(), gRules.end(), [](const LedRule&a, const LedRule&b){return a.maxDb<b.maxDb;});
  }
  if(gRules.empty()){
    gRules = {{90,strip.Color(0,255,0)}, {100,strip.Color(255,255,0)}, {110,strip.Color(255,0,0)}};
  }
}

// ===== STA підключення з fallback =====
void try_connect_known(const std::vector<WiFiProfile>& plist){
  WiFi.mode(WIFI_AP_STA);
  WiFi.setSleep(false);
  WiFi.setTxPower(WIFI_TX_MAX);

  // попереднє скан-очищення
  WiFi.scanDelete();
  int networkCount = WiFi.scanNetworks();

  for(size_t i=0;i<plist.size();++i){
    bool present=false;
    for(int j=0;j<networkCount;j++) if(WiFi.SSID(j)==plist[i].ssid){ present=true; break; }
    if(!present) continue;

    indicateWiFiAttempt((int)i);
    WiFi.begin(plist[i].ssid.c_str(), plist[i].pass.c_str());
    for(int attempt=1; attempt<=WIFI_MAX_ATTEMPTS_SSID; attempt++){
      unsigned long t0=millis();
      while(WiFi.status()!=WL_CONNECTED && millis()-t0<WIFI_CONNECT_TIMEOUT_MS){ delay(250); yield(); }
      if(WiFi.status()==WL_CONNECTED){ indicateWiFiSuccess((int)i); return; }
      WiFi.disconnect(); delay(200);
    }
  }
}

// ===== AP + Captive Portal =====
void start_ap(){
  WiFi.mode(WIFI_AP_STA);
  WiFi.setSleep(false);
  if(WiFi.softAP(AP_SSID, AP_PASS)){
    dnsServer.start(DNS_PORT, "*", WiFi.softAPIP());
    apWindowStart = millis();
    apWindowOpen  = true;
  }
}
void manage_ap_window(){
  if(!apWindowOpen) return;
  if(millis() - apWindowStart < AP_WINDOW_MS) return;

  if(WiFi.softAPgetStationNum() == 0){
    dnsServer.stop();
    WiFi.softAPdisconnect(true);
    WiFi.mode(WIFI_STA);
  }
  apWindowOpen = false;
}

// ===== mDNS/HTTP =====
void start_mdns_if_needed(){
  if(mdnsStarted) return;
  if(WiFi.getMode()==WIFI_AP || WiFi.getMode()==WIFI_AP_STA || WiFi.status()==WL_CONNECTED){
    if(MDNS.begin("lichtbrucke")){
      MDNS.addService("http","tcp",80);
      mdnsStarted = true;
    }
  }
}
void start_http_server_if_needed(){
  if(httpStarted) return;
  if(WiFi.getMode()==WIFI_AP || WiFi.getMode()==WIFI_AP_STA || WiFi.status()==WL_CONNECTED){
    server.begin();
    httpStarted = true;
  }
}

// ===== MQTT =====
void mqtt_subscribe(){ client.subscribe(mqtt_topic); }
void reconnect_mqtt(){
  unsigned long now=millis(); if(now-lastMqttConnTry<1000) return; lastMqttConnTry=now;
  char clientId[40]; snprintf(clientId,sizeof(clientId),"ESP32-%06X",(uint32_t)(ESP.getEfuseMac()&0xFFFFFF));
  if(client.connect(clientId, mqtt_user, mqtt_password)){ mqtt_subscribe(); indicateMQTT(); indicateSystemReady(); }
}
void callback(char* topic, byte* payload, unsigned int length){
  mqttMessageCount++; if(length>64){ mqttDropped++; return; }
  char msg[65]; memcpy(msg,payload,length); msg[length]='\0';
  char* comma=strchr(msg,','); if(!comma) return;
  *comma='\0'; int channel=atoi(msg)-1; float db=atof(comma+1);
  if(channel>=0 && channel<NUM_LEDS){
    int brightness = constrain(map((int)db, 88, 111, 50, 255), 0, 255);
    activeLeds[channel]={channel,db,(uint8_t)brightness,millis(),true};
  }
}

// ===== LED update =====
void updateLEDs(){
  strip.clear(); unsigned long now=millis();
  for(int i=0;i<NUM_LEDS;i++){
    if(!activeLeds[i].active) continue;
    uint8_t b=activeLeds[i].brightness;
    uint32_t color = colorForDb(activeLeds[i].db, b);
    strip.setPixelColor(i,color);
    if(now - activeLeds[i].lastUpdate > LED_FADE_STEP_MS){
      if(activeLeds[i].brightness>LED_FADE_DEC) activeLeds[i].brightness-=LED_FADE_DEC;
      else activeLeds[i].active=false;
      activeLeds[i].lastUpdate=now;
    }
  }
  strip.show();
}

// ===== Health =====
void checkSystemHealth(){
  unsigned long now=millis();
  if(now-lastMqttCountTime>MQTT_RATE_WINDOW_MS){
    if(mqttMessageCount>MQTT_RATE_LIMIT_MSG_S){
      Serial.printf("MQTT rate %u/s, dropped %u\n", mqttMessageCount, mqttDropped);
    }
    mqttMessageCount=0; lastMqttCountTime=now;
  }
  if(now-lastWiFiCheck>WIFI_CHECK_MS){
    if(WiFi.status()!=WL_CONNECTED){ WiFi.reconnect(); WiFi.setTxPower(WIFI_TX_MAX); }
    lastWiFiCheck=now;
  }
}

// ===== Web =====
String currentState(){
  if(WiFi.status()==WL_CONNECTED) return "connected";
  if(WiFi.getMode()==WIFI_AP || WiFi.getMode()==WIFI_AP_STA) return "ap";
  return "disconnected";
}
void sendJSON(DynamicJsonDocument& doc){ String s; serializeJson(doc,s); server.send(200,"application/json",s); }
void sendError(int code, const String& msg){ server.send(code,"text/plain",msg); }
void handle_static(const String& path, const char* ctype){
  if(!LittleFS.exists(path)){ server.send(404,"text/plain","Not found"); return; }
  File f=LittleFS.open(path,"r"); server.streamFile(f,ctype); f.close();
}

void setup_web_routes(){
  server.on("/",  [](){ handle_static("/index.html","text/html"); });
  server.on("/logo.svg", [](){ handle_static("/logo.svg","image/svg+xml"); });

  // Captive-check endpoints
  server.on("/connecttest.txt", [](){ server.send(200,"text/plain","Microsoft Connect Test"); });
  server.on("/redirect", [](){ server.sendHeader("Location","/"); server.send(302,"text/plain",""); });
  server.on("/ncsi.txt", [](){ server.send(200,"text/plain","Microsoft NCSI"); });
  server.on("/generate_204", [](){ server.sendHeader("Location","/"); server.send(302,"text/plain",""); });
  server.on("/hotspot-detect.html", [](){
    server.send(200,"text/html","<html><head><title>Success</title></head><body><a href=\"/\">open</a></body></html>");
  });

  // Async scan, надійний у AP_STA
  server.on("/api/wifi/scan", HTTP_GET, [](){
    DynamicJsonDocument doc(4096);
    JsonArray a = doc.createNestedArray("nets");

    WiFi.scanDelete();
    WiFi.scanNetworks(true, true); // async, show_hidden

    unsigned long t0 = millis();
    int16_t res;
    while((res = WiFi.scanComplete()) == WIFI_SCAN_RUNNING && millis()-t0 < 7000){
      delay(100);
    }
    int n = (res >= 0) ? res : WiFi.scanNetworks(); // fallback

    for(int i=0;i<n;i++){
      JsonObject o=a.createNestedObject();
      o["ssid"] = WiFi.SSID(i);
      o["rssi"] = WiFi.RSSI(i);
      o["auth"] = WiFi.encryptionType(i);
    }
    WiFi.scanDelete();
    sendJSON(doc);
  });

  server.on("/api/wifi/status", HTTP_GET, [](){
    DynamicJsonDocument doc(256);
    doc["state"]=currentState();
    doc["ssid"]= (WiFi.status()==WL_CONNECTED? WiFi.SSID() : "");
    doc["ip"]=   (WiFi.status()==WL_CONNECTED? WiFi.localIP().toString()
                  : (WiFi.getMode()==WIFI_AP || WiFi.getMode()==WIFI_AP_STA? WiFi.softAPIP().toString() : ""));
    doc["rssi"]= (WiFi.status()==WL_CONNECTED? WiFi.RSSI() : 0);
    doc["ap_clients"]= WiFi.softAPgetStationNum();
    sendJSON(doc);
  });

  server.on("/api/wifi/saved", HTTP_GET, [](){
    DynamicJsonDocument doc(2048);
    std::vector<WiFiProfile> arr; String last; loadWiFiProfiles(arr,last);
    JsonArray a = doc.createNestedArray("profiles");
    for(const auto &p: arr){ JsonObject o=a.createNestedObject(); o["ssid"]=p.ssid; }
    sendJSON(doc);
  });

  server.on("/api/wifi/connect", HTTP_POST, [](){
    if(!server.hasArg("plain")){ sendError(400,"no body"); return; }
    DynamicJsonDocument body(512); if(deserializeJson(body, server.arg("plain"))) { sendError(400,"bad json"); return; }
    String ssid = body["ssid"]|""; String pass = body["pass"]|""; if(!ssid.length()){ sendError(400,"ssid required"); return; }

    std::vector<WiFiProfile> arr; String last; loadWiFiProfiles(arr,last);
    bool found=false; for(auto &p: arr){ if(p.ssid==ssid){ p.pass=pass; found=true; break; } }
    if(!found) arr.push_back({ssid,pass});
    saveWiFiProfiles(arr, ssid);

    WiFi.begin(ssid.c_str(), pass.c_str());
    server.send(200,"text/plain","OK");
  });

  server.on("/api/wifi/forget", HTTP_POST, [](){
    if(!server.hasArg("plain")){ sendError(400,"no body"); return; }
    DynamicJsonDocument body(256); if(deserializeJson(body, server.arg("plain"))) { sendError(400,"bad json"); return; }
    String ssid=body["ssid"]|""; if(!ssid.length()){ sendError(400,"ssid required"); return; }
    std::vector<WiFiProfile> arr; String last; loadWiFiProfiles(arr,last);
    std::vector<WiFiProfile> res; for(const auto &p: arr){ if(p.ssid!=ssid) res.push_back(p); }
    if(last==ssid) last="";
    saveWiFiProfiles(res,last);
    server.send(200,"text/plain","OK");
  });

  server.on("/api/ledmap", HTTP_GET, [](){
    DynamicJsonDocument doc(2048);
    if(!loadLedMap(doc)){
      doc["rules"] = JsonArray();
      JsonArray a=doc["rules"].as<JsonArray>();
      JsonObject r1=a.createNestedObject(); r1["max"]=90;  r1["color"]="#00ff00";
      JsonObject r2=a.createNestedObject(); r2["max"]=100; r2["color"]="#ffff00";
      JsonObject r3=a.createNestedObject(); r3["max"]=110; r3["color"]="#ff0000";
    }
    sendJSON(doc);
  });

  server.on("/api/ledmap", HTTP_POST, [](){
    if(!server.hasArg("plain")){ sendError(400,"no body"); return; }
    DynamicJsonDocument doc(4096); auto err=deserializeJson(doc, server.arg("plain"));
    if(err){ sendError(400,"bad json"); return; }
    saveLedMap(doc);
    loadRulesFromFS();
    server.send(200,"text/plain","OK");
  });

  server.onNotFound([](){
    if(server.uri().startsWith("/api/")){
      server.send(404,"text/plain","Not found");
    }else{
      server.sendHeader("Location","/");
      server.send(302,"text/plain","");
    }
  });
}

// ===== Setup / Loop =====
void setup(){
  Serial.begin(19200);

  strip.begin(); strip.show();
  LittleFS.begin(true);
  loadRulesFromFS();

  start_ap();                 // AP + DNS captive
  setup_web_routes();
  start_http_server_if_needed();
  start_mdns_if_needed();

  std::vector<WiFiProfile> list; String last;
  if(loadWiFiProfiles(list,last) && !list.empty()) try_connect_known(list);
  if(WiFi.status()!=WL_CONNECTED){
    std::vector<WiFiProfile> fb = { {ssid1,password1}, {ssid2,password2}, {ssid3,password3} };
    try_connect_known(fb);
  }

  client.setServer(mqtt_server,mqtt_port);
  client.setCallback(callback);
  client.setKeepAlive(MQTT_KEEPALIVE_SEC);
  client.setSocketTimeout(MQTT_SOCKET_TO_SEC);
  client.setBufferSize(MQTT_BUFFER_BYTES);

  for(int i=0;i<NUM_LEDS;i++) activeLeds[i].active=false;
  unsigned long now=millis(); lastLedUpdate=now; lastMqttCountTime=now; lastWiFiCheck=now;

  reconnect_mqtt();
}

void loop(){
  dnsServer.processNextRequest();

  if(!httpStarted || !mdnsStarted){
    start_http_server_if_needed();
    start_mdns_if_needed();
  }
  manage_ap_window();

  server.handleClient();

  if(WiFi.status()==WL_CONNECTED){
    if(!client.connected()) reconnect_mqtt();
    client.loop();
  }

  if(millis()-lastLedUpdate>=LED_UPDATE_MS){ updateLEDs(); lastLedUpdate=millis(); }
  checkSystemHealth();
  yield();
}
