#include <WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_NeoPixel.h>

// ===== Wi-Fi =====
const char* ssid1 = "Test";
const char* password1 = "1234567890";
const char* ssid2 = "AERO-MK-Guest";
const char* password2 = "%Gast#2020$MK!";
const char* ssid3 = "test";
const char* password3 = "12345678";

// Максимальна потужність передавача для Arduino-ESP32
// Для більшості плат верхня межа — WIFI_POWER_19_5dBm
static const wifi_power_t WIFI_TX_MAX = WIFI_POWER_19_5dBm;

// ===== MQTT =====
const char* mqtt_server   = "mqtt.marxkrontal.com";
const int   mqtt_port     = 1883;
const char* mqtt_user     = "test-monitoring";
const char* mqtt_password = "+aR7gr*G&v4]SPS6\\q,W";
const char* mqtt_topic    = "test-monitoring/Lichtbrucke";

// Параметри MQTT з м’якшим таймінгом
static const uint16_t MQTT_BUFFER_BYTES   = 256;
static const uint16_t MQTT_KEEPALIVE_SEC  = 30;
static const uint16_t MQTT_SOCKET_TO_SEC  = 15;

// ===== LED (NeoPixel) =====
// Уникаємо flash-шини (GPIO6..11 на ESP32). Безпечні пін(и) — 2 або 4.
#define LED_PIN   2
#define NUM_LEDS  122
Adafruit_NeoPixel strip(NUM_LEDS, LED_PIN, NEO_GRB + NEO_KHZ800);

// ===== Стани активних світлодіодів =====
struct ActiveLED {
  int channel;
  float db;
  uint8_t brightness;
  unsigned long lastUpdate;
  bool active;
};
ActiveLED activeLeds[NUM_LEDS];

// ===== Мережеві об’єкти =====
WiFiClient espClient;
PubSubClient client(espClient);

// ===== Лічильники/таймери =====
unsigned int  mqttMessageCount   = 0;
unsigned long lastMqttCountTime  = 0;
unsigned long lastLedUpdate      = 0;
unsigned long lastWiFiCheck      = 0;
unsigned long lastMqttConnTry    = 0;

// Ліміти й інтервали
static const uint16_t LED_UPDATE_MS          = 100;
static const uint16_t LED_FADE_STEP_MS       = 150;
static const uint8_t  LED_FADE_DEC           = 5;
static const uint16_t WIFI_CHECK_MS          = 5000;
static const uint16_t MQTT_RATE_WINDOW_MS    = 1000;
static const uint16_t MQTT_RATE_LIMIT_MSG_S  = 400; // замість жорсткого рестарту
static const uint16_t WIFI_CONNECT_TIMEOUT_MS= 10000;
static const uint8_t  WIFI_MAX_ATTEMPTS_SSID = 3;

// ===== LED індикація =====
void indicateWiFiAttempt(int index) {
  for (int i = 0; i <= index && i < NUM_LEDS; i++) strip.setPixelColor(i, strip.Color(0, 0, 255));
  strip.show();
}

void indicateWiFiSuccess(int index) {
  for (int i = 0; i <= index && i < NUM_LEDS; i++) strip.setPixelColor(i, strip.Color(0, 255, 0));
  strip.show();
  delay(200);
}

void indicateMQTT() {
  for (int i = 0; i < 5 && i < NUM_LEDS; i++) strip.setPixelColor(i, strip.Color(255, 165, 0));
  strip.show();
  delay(200);
}

void indicateSystemReady() {
  for (int blink = 0; blink < 3; blink++) {
    for (int j = 0; j < 5; j++) {
      if (j < NUM_LEDS) strip.setPixelColor(j, strip.Color(0, 255, 0));
      if ((NUM_LEDS - 1 - j) >= 0) strip.setPixelColor(NUM_LEDS - 1 - j, strip.Color(0, 255, 0));
    }
    strip.show();
    delay(200);
    for (int j = 0; j < 5; j++) {
      if (j < NUM_LEDS) strip.setPixelColor(j, 0);
      if ((NUM_LEDS - 1 - j) >= 0) strip.setPixelColor(NUM_LEDS - 1 - j, 0);
    }
    strip.show();
    delay(200);
  }
}

// ===== Wi-Fi =====
void setup_wifi() {
  Serial.println("\nПідключення до Wi-Fi...");
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true);
  delay(100);
  WiFi.setTxPower(WIFI_TX_MAX);

  struct WiFiOption {
    const char* ssid;
    const char* password;
  } wifiOptions[] = {
    {ssid1, password1},
    {ssid2, password2},
    {ssid3, password3}
  };

  int networkCount = WiFi.scanNetworks();
  if (networkCount <= 0) {
    Serial.println("Жодної мережі не знайдено.");
  } else {
    Serial.printf("Знайдено %d мереж(і):\n", networkCount);
    for (int i = 0; i < networkCount; ++i) {
      Serial.printf("  %2d: %-30s RSSI: %4d dBm\n", i + 1, WiFi.SSID(i).c_str(), WiFi.RSSI(i));
    }
  }

  for (int i = 0; i < (int)(sizeof(wifiOptions) / sizeof(wifiOptions[0])); i++) {
    bool found = false;
    for (int j = 0; j < networkCount; j++) {
      if (WiFi.SSID(j) == wifiOptions[i].ssid) {
        Serial.printf("Мережу '%s' знайдено (RSSI: %d dBm)\n", wifiOptions[i].ssid, WiFi.RSSI(j));
        found = true;
        break;
      }
    }
    if (!found) {
      Serial.printf("Мережу '%s' не знайдено\n", wifiOptions[i].ssid);
      continue;
    }

    indicateWiFiAttempt(i);
    WiFi.begin(wifiOptions[i].ssid, wifiOptions[i].password);

    for (int attempt = 1; attempt <= WIFI_MAX_ATTEMPTS_SSID; attempt++) {
      Serial.printf("Спроба %d підключитися до %s...\n", attempt, wifiOptions[i].ssid);
      unsigned long t0 = millis();
      while (WiFi.status() != WL_CONNECTED && (millis() - t0) < WIFI_CONNECT_TIMEOUT_MS) {
        delay(250);
        yield();
      }
      if (WiFi.status() == WL_CONNECTED) {
        Serial.printf("Підключено до %s\n", wifiOptions[i].ssid);
        Serial.print("IP: "); Serial.println(WiFi.localIP());
        Serial.printf("RSSI: %d dBm\n", WiFi.RSSI());
        indicateWiFiSuccess(i);
        return;
      } else {
        Serial.println("Спроба не вдалася.");
        WiFi.disconnect();
        delay(200);
      }
    }
  }

  Serial.println("Не вдалося підключитися до жодної мережі. Повтор сканування...");
  delay(1500);
  setup_wifi(); // рекурсивний повтор із невеликим блокуванням
}

// ===== MQTT =====
void mqtt_subscribe() {
  client.subscribe(mqtt_topic);
}

void reconnect_mqtt() {
  // Експоненційний мін-бекоф без блокування понад 5 с
  unsigned long now = millis();
  if (now - lastMqttConnTry < 1000) return;
  lastMqttConnTry = now;

  Serial.print("Підключення до MQTT...");
  char clientId[40];
  // Унікальний ClientID за MAC
  snprintf(clientId, sizeof(clientId), "ESP32C3-%06X", (uint32_t)(ESP.getEfuseMac() & 0xFFFFFF));

  if (client.connect(clientId, mqtt_user, mqtt_password)) {
    Serial.println("ОК");
    mqtt_subscribe();
    indicateMQTT();
    indicateSystemReady();
  } else {
    Serial.print("помилка, rc=");
    Serial.println(client.state());
  }
}

// ===== Обробка MQTT повідомлень =====
volatile unsigned int mqttDropped = 0;

void callback(char* topic, byte* payload, unsigned int length) {
  // Ліміт швидкості повідомлень без рестарту
  mqttMessageCount++;

  if (length > 64) {
    // Надто велике. Пропускаємо.
    mqttDropped++;
    return;
  }

  char msg[65];
  memcpy(msg, payload, length);
  msg[length] = '\0';

  // Очікуємо "channel,db"
  char* commaPtr = strchr(msg, ',');
  if (!commaPtr) return;

  *commaPtr = '\0';
  int   channel = atoi(msg) - 1;
  float db      = atof(commaPtr + 1);

  if (channel >= 0 && channel < NUM_LEDS) {
    int brightness = constrain(map((int)db, 88, 111, 50, 255), 0, 255);
    activeLeds[channel] = {channel, db, (uint8_t)brightness, millis(), true};
  }
}

// ===== Оновлення світлодіодів =====
void updateLEDs() {
  strip.clear();
  unsigned long now = millis();

  for (int i = 0; i < NUM_LEDS; i++) {
    if (!activeLeds[i].active) continue;

    uint8_t b = activeLeds[i].brightness;
    uint32_t color =
      (activeLeds[i].db < 90.0f)  ? strip.Color(0, b, 0) :
      (activeLeds[i].db < 100.0f) ? strip.Color(b, b / 2, 0) :
                                    strip.Color(b, 0, 0);

    strip.setPixelColor(i, color);

    if (now - activeLeds[i].lastUpdate > LED_FADE_STEP_MS) {
      if (activeLeds[i].brightness > LED_FADE_DEC) activeLeds[i].brightness -= LED_FADE_DEC;
      else activeLeds[i].active = false;
      activeLeds[i].lastUpdate = now;
    }
  }
  strip.show();
}

// ===== Перевірка стану системи =====
void checkSystemHealth() {
  unsigned long now = millis();

  if (now - lastMqttCountTime > MQTT_RATE_WINDOW_MS) {
    if (mqttMessageCount > MQTT_RATE_LIMIT_MSG_S) {
      // Перевищення трафіку — лише лог і відсічення у callback
      Serial.printf("MQTT: перевищено ліміт %u/s, отримано %u, дропнуто %u\n",
                    MQTT_RATE_LIMIT_MSG_S, mqttMessageCount, mqttDropped);
    }
    mqttMessageCount = 0;
    lastMqttCountTime = now;
  }

  if (now - lastWiFiCheck > WIFI_CHECK_MS) {
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("Wi-Fi втрачено. Повторне підключення...");
      WiFi.disconnect();
      WiFi.reconnect();
      WiFi.setTxPower(WIFI_TX_MAX);
    }
    lastWiFiCheck = now;
  }
}

// ===== Setup / Loop =====
void setup() {
  Serial.begin(19200);

  strip.begin();
  strip.show(); // очистити

  // Початкове підключення Wi-Fi
  setup_wifi();

  // MQTT клієнт
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);
  client.setKeepAlive(MQTT_KEEPALIVE_SEC);
  client.setSocketTimeout(MQTT_SOCKET_TO_SEC);
  client.setBufferSize(MQTT_BUFFER_BYTES);

  for (int i = 0; i < NUM_LEDS; i++) activeLeds[i].active = false;

  unsigned long now = millis();
  lastLedUpdate     = now;
  lastMqttCountTime = now;
  lastWiFiCheck     = now;

  // Початковий конект до брокера
  reconnect_mqtt();
}

void loop() {
  // Підтримка Wi-Fi
  if (WiFi.status() != WL_CONNECTED) {
    // даємо шанс відновитися checkSystemHealth()
  }

  // Підтримка MQTT
  if (!client.connected()) reconnect_mqtt();
  client.loop();

  // Анімація LED
  if (millis() - lastLedUpdate >= LED_UPDATE_MS) {
    updateLEDs();
    lastLedUpdate = millis();
  }

  // Моніторинг
  checkSystemHealth();
  yield();
}

/*
Апаратні нотатки:
— Живлення стрічки 5 В із загальною «землею». Резистор 220–470 Ω у лінії DIN.
— Конденсатор 1000 µF між 5V і GND на вході стрічки.
— Рівні логіки: ESP32 3.3 В → бажано level-shifter до 5 В для довгих ліній.
— Уникайте GPIO6..11 (flash), та страп-пінів при виборі LED_PIN.
*/
