# Roemershag LoRa

Rissmonitoring-Projekt für den Standort Roemershag.  
Basierend auf **Heltec WiFi LoRa 32 V3 (ESP32-S3)** mit einem **DS18B20**-Sensor.

## Payload-Format

- `T_c_x100+5000` — Temperatur vom DS18B20 (°C ×100, Offset +5000)  
- `ADC_RAW` — Rohwert des ADC  
- `CRACK_mm_x100` — Rissbreite in 1/100 mm  
- `VBAT_mV` — Batteriespannung in mV  

## Funktionen
- LoRaWAN OTAA (EU868)  
- AP-Modus mit Weboberfläche (Konfiguration und Monitoring)  
- Konfiguration in NVS gespeichert  
- OLED-Display mit sanften Updates (kein Flackern)  
- Datenerfassung: DS18B20, Risssensor, Batteriespannung  

## Struktur
roemershag_lora/
├─ roemershag_lora.ino # Hauptsketch
└─ data/ # LittleFS-Dateien (index.html, i18n.json, usw.)

## Nutzung
1. Öffnen Sie `roemershag_lora.ino` in Arduino IDE oder PlatformIO.  
2. Erforderliche Bibliotheken installieren:
   - Heltec `LoRaWan_APP`  
   - DallasTemperature + OneWire  
   - ArduinoJson  
   - SSD1306Wire (für OLED)  
3. Sketch auf das Board hochladen.  
4. Inhalt des `data`-Ordners in LittleFS hochladen.  

## Lizenz
MKP GMBH
