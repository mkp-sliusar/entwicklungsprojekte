#define TINY_GSM_MODEM_SIM7080
#define TINY_GSM_RX_BUFFER 1024
#define XPOWERS_CHIP_AXP2101

#include <Arduino.h>
#include <ArduinoJson.h>
#include <DallasTemperature.h>
#include <OneWire.h>
#include <PubSubClient.h>
#include <SPI.h>
#include <TinyGsmClient.h>
#include <Wire.h>
#include <XPowersLib.h>
#include <esp_sleep.h>
#include <math.h>

#define SerialAT Serial1

static constexpr uint8_t PIN_ADS_SCLK = 12;
static constexpr uint8_t PIN_ADS_MISO = 11;
static constexpr uint8_t PIN_ADS_MOSI = 13;
static constexpr uint8_t PIN_ADS_CS = 10;
static constexpr uint8_t PIN_ADS_DRDY = 8;
// Standalone LilyGO test; MultiConnect v17 J2 pin 8 is routed to GPIO1.
static constexpr uint8_t PIN_DS18B20 = 9;
static constexpr uint8_t PIN_INA_SDA = 47;
static constexpr uint8_t PIN_INA_SCL = 48;
static constexpr uint8_t PIN_J7_24V_ENABLE = 14;

static constexpr uint8_t CAM_PMU_SDA = 15;
static constexpr uint8_t CAM_PMU_SCL = 7;
static constexpr uint8_t CAM_MODEM_TX = 5;
static constexpr uint8_t CAM_MODEM_RX = 4;
static constexpr uint8_t CAM_MODEM_RING = 3;
static constexpr uint8_t CAM_MODEM_DTR = 42;
static constexpr uint8_t CAM_MODEM_PWRKEY = 41;
static constexpr uint8_t H795_MODEM_TX = 4;
static constexpr uint8_t H795_MODEM_RX = 5;
static constexpr uint8_t H795_MODEM_RING = 6;
static constexpr uint8_t H795_MODEM_DTR = 7;
static constexpr uint8_t H795_MODEM_POWER_SAVE = 42;
static constexpr uint8_t H795_MODEM_PWRKEY = 46;
static constexpr uint32_t MODEM_BAUD = 115200;
static constexpr uint16_t MODEM_POWER_ON_PULSE_MS = 1000;
static constexpr uint16_t CAM_MODEM_INITIAL_PULSE_MS = 500;

static constexpr uint8_t DS18B20_INIT_ATTEMPTS = 3;
static constexpr uint8_t DS18B20_READ_ATTEMPTS = 2;
static constexpr uint16_t DS18B20_RETRY_DELAY_MS = 100;

//static constexpr char CELLULAR_APN[] = "iotsim.melita.io";
static constexpr char CELLULAR_APN[] = "internet.m2mportal.de"; 
static constexpr char CELLULAR_USER[] = "";
static constexpr char CELLULAR_PASSWORD[] = "";
static constexpr char MQTT_SERVER[] = "mqtt.marxkrontal.com";
static constexpr uint16_t MQTT_PORT = 1883;
static constexpr char MQTT_USER[] = "funksystemiot";
static constexpr char MQTT_PASSWORD[] = "4bx7aijN_mb?znh2T`T\"";
static constexpr char MQTT_TOPIC[] = "test/funksystemiot/nbiot/07152_05/1081ACA0ED30";

static constexpr uint8_t ADS_CMD_POWERDOWN = 0x02;
static constexpr uint8_t ADS_CMD_RESET = 0x06;
static constexpr uint8_t ADS_CMD_START = 0x08;
static constexpr uint8_t ADS_CMD_RDATA = 0x10;
static constexpr uint8_t ADS_CMD_RREG = 0x20;
static constexpr uint8_t ADS_CMD_WREG = 0x40;
static constexpr uint8_t ADS_MUX_AIN2_AIN3 = 0x50;
static constexpr uint8_t ADS_AVERAGE_SAMPLES = 10;
static constexpr uint8_t ADS_DISCARD_SAMPLES = 1;
static constexpr uint8_t INA226_ADDRESS = 0x40;
static constexpr float INA226_SHUNT_OHMS = 0.010f;

SPIClass adsSPI(HSPI);
SPISettings adsSPISettings(500000, MSBFIRST, SPI_MODE1);
OneWire oneWire(PIN_DS18B20);
DallasTemperature temperatureSensors(&oneWire);
TwoWire sensorI2C(1);
TinyGsm modem(SerialAT);
TinyGsmClient gsmClient(modem);
PubSubClient mqtt(gsmClient);
XPowersPMU PMU;

uint8_t modemTxPin = H795_MODEM_TX;
uint8_t modemRxPin = H795_MODEM_RX;
uint8_t modemRingPin = H795_MODEM_RING;
uint8_t modemDtrPin = H795_MODEM_DTR;
uint8_t modemPwrKeyPin = H795_MODEM_PWRKEY;
bool camPmuReady = false;

struct Measurements {
	bool temperatureValid = false;
	float temperatureC = NAN;
	bool wegValid = false;
	int32_t wegRaw = 0;
	float wegVoltageV = NAN;
	bool inaValid = false;
	float inaBusVoltageV = NAN;
	float inaShuntVoltageMv = NAN;
	float inaCurrentMa = NAN;
	float inaPowerW = NAN;
	uint32_t measuredAtMs = 0;
};

Measurements cached;
bool sensorTestEnabled = false;
bool adsAvailable = false;
bool dsAvailable = false;
bool inaAvailable = false;
bool modemReady = false;
bool cellularReady = false;

bool publishMeasurements();

bool initializeCamPowerManager() {
	if (!PMU.begin(Wire, AXP2101_SLAVE_ADDRESS, CAM_PMU_SDA, CAM_PMU_SCL)) {
		Wire.end();
		return false;
	}

	camPmuReady = true;
	modemTxPin = CAM_MODEM_TX;
	modemRxPin = CAM_MODEM_RX;
	modemRingPin = CAM_MODEM_RING;
	modemDtrPin = CAM_MODEM_DTR;
	modemPwrKeyPin = CAM_MODEM_PWRKEY;

	PMU.setChargeTargetVoltage(XPOWERS_AXP2101_CHG_VOL_4V2);
	PMU.setChargingLedMode(XPOWERS_CHG_LED_CTRL_CHG);
	PMU.enableTemperatureMeasure();
	PMU.enableBattDetection();
	PMU.enableVbusVoltageMeasure();
	PMU.enableBattVoltageMeasure();
	PMU.enableSystemVoltageMeasure();

	PMU.disableDC2();
	PMU.disableDC4();
	PMU.disableDC5();
	PMU.disableALDO1();
	PMU.disableALDO2();
	PMU.disableALDO3();
	PMU.disableALDO4();
	PMU.disableBLDO2();
	PMU.disableCPUSLDO();
	PMU.disableDLDO1();
	PMU.disableDLDO2();

	if (esp_sleep_get_wakeup_cause() == ESP_SLEEP_WAKEUP_UNDEFINED) {
		PMU.disableDC3();
		delay(200);
	}

	PMU.setBLDO1Voltage(3300);
	PMU.enableBLDO1();
	PMU.setDC3Voltage(3000);
	PMU.setBLDO2Voltage(3300);
	PMU.enableBLDO2();
	PMU.setALDO3Voltage(3300);
	PMU.enableALDO3();
	PMU.disableTSPinMeasure();
	return true;
}

void powerDownModemRail() {
	digitalWrite(modemPwrKeyPin, LOW);
	if (camPmuReady) {
		PMU.disableDC3();
		delay(200);
	} else {
		digitalWrite(H795_MODEM_POWER_SAVE, LOW);
	}
}

void adsSelect() {
	adsSPI.beginTransaction(adsSPISettings);
	digitalWrite(PIN_ADS_CS, LOW);
	delayMicroseconds(2);
}

void adsDeselect() {
	delayMicroseconds(2);
	digitalWrite(PIN_ADS_CS, HIGH);
	adsSPI.endTransaction();
}

void adsCommand(uint8_t command) {
	adsSelect();
	adsSPI.transfer(command);
	adsDeselect();
}

void adsPowerDown() {
	adsCommand(ADS_CMD_POWERDOWN);
}

void disableAdsInterface() {
	adsSPI.end();
	pinMode(PIN_ADS_CS, INPUT);
	pinMode(PIN_ADS_SCLK, INPUT);
	pinMode(PIN_ADS_MOSI, INPUT);
	pinMode(PIN_ADS_MISO, INPUT);
	pinMode(PIN_ADS_DRDY, INPUT);
}

void adsWriteRegister(uint8_t address, uint8_t value) {
	adsSelect();
	adsSPI.transfer(ADS_CMD_WREG | ((address & 0x03) << 2));
	adsSPI.transfer(value);
	adsDeselect();
}

uint8_t adsReadRegister(uint8_t address) {
	adsSelect();
	adsSPI.transfer(ADS_CMD_RREG | ((address & 0x03) << 2));
	const uint8_t value = adsSPI.transfer(0xFF);
	adsDeselect();
	return value;
}

bool waitAdsReady(uint32_t timeoutMs) {
	const uint32_t started = millis();
	while (digitalRead(PIN_ADS_DRDY) == HIGH) {
		if (millis() - started >= timeoutMs) return false;
		delay(1);
	}
	return true;
}

int32_t adsReadRaw() {
	adsSelect();
	adsSPI.transfer(ADS_CMD_RDATA);

	uint32_t raw = 0;
	raw |= static_cast<uint32_t>(adsSPI.transfer(0xFF)) << 16;
	raw |= static_cast<uint32_t>(adsSPI.transfer(0xFF)) << 8;
	raw |= static_cast<uint32_t>(adsSPI.transfer(0xFF));
	adsDeselect();

	if (raw & 0x00800000UL) raw |= 0xFF000000UL;
	return static_cast<int32_t>(raw);
}

bool adsSingleConversion(int32_t& raw) {
	adsCommand(ADS_CMD_START);
	if (!waitAdsReady(500)) return false;
	raw = adsReadRaw();
	return true;
}

void configureWegChannel() {
	adsWriteRegister(0, ADS_MUX_AIN2_AIN3 | 0x01);
	adsWriteRegister(1, 0x00);
	adsWriteRegister(2, 0xC0);
	adsWriteRegister(3, 0x00);
	delayMicroseconds(100);
}

bool initializeADS1220() {
	pinMode(PIN_ADS_CS, OUTPUT);
	digitalWrite(PIN_ADS_CS, HIGH);
	pinMode(PIN_ADS_DRDY, INPUT_PULLUP);
	adsSPI.begin(PIN_ADS_SCLK, PIN_ADS_MISO, PIN_ADS_MOSI, PIN_ADS_CS);
	adsCommand(ADS_CMD_RESET);
	delay(10);

	adsWriteRegister(0, 0x10);
	delay(2);
	const uint8_t readback = adsReadRegister(0);
	Serial.printf("[ADS1220] REG0 readback: 0x%02X\n", readback);
	if (readback != 0x10) {
		disableAdsInterface();
		return false;
	}

	configureWegChannel();
	return true;
}

bool readWegsensor(int32_t& raw, float& voltageV) {
	if (!adsAvailable) return false;
	configureWegChannel();

	for (uint8_t i = 0; i < ADS_DISCARD_SAMPLES; ++i) {
		int32_t discarded = 0;
		if (!adsSingleConversion(discarded)) return false;
	}

	int64_t sum = 0;
	for (uint8_t i = 0; i < ADS_AVERAGE_SAMPLES; ++i) {
		int32_t sample = 0;
		if (!adsSingleConversion(sample)) return false;
		sum += sample;
	}

	raw = static_cast<int32_t>(sum / ADS_AVERAGE_SAMPLES);
	voltageV = static_cast<float>(raw) * 3.300f / 8388608.0f;
	adsPowerDown();
	return isfinite(voltageV);
}

void reportOneWireLine() {
	pinMode(PIN_DS18B20, INPUT);
	analogSetPinAttenuation(PIN_DS18B20, ADC_11db);
	const uint32_t lineMillivolts = analogReadMilliVolts(PIN_DS18B20);
	pinMode(PIN_DS18B20, INPUT_PULLUP);
	delay(1);
	Serial.printf("[DS18B20] GPIO%u line=%lu mV, logic=%u, sensor 3V3=always ON\n",
								static_cast<unsigned>(PIN_DS18B20),
								static_cast<unsigned long>(lineMillivolts),
								digitalRead(PIN_DS18B20));
}

bool initializeDS18B20() {
	oneWire.begin(PIN_DS18B20);
	pinMode(PIN_DS18B20, INPUT_PULLUP);
	delay(1);
	reportOneWireLine();
	temperatureSensors.begin();

	for (uint8_t attempt = 0; attempt < DS18B20_INIT_ATTEMPTS; ++attempt) {
		const uint8_t count = temperatureSensors.getDeviceCount();
		Serial.printf("[DS18B20] devices: %u (attempt %u)\n",
									count, static_cast<unsigned>(attempt + 1));
		if (count > 0) {
			temperatureSensors.setResolution(10);
			temperatureSensors.setWaitForConversion(true);
			return true;
		}

		if (attempt + 1 < DS18B20_INIT_ATTEMPTS) {
			delay(DS18B20_RETRY_DELAY_MS);
			oneWire.begin(PIN_DS18B20);
			pinMode(PIN_DS18B20, INPUT_PULLUP);
			delay(1);
			temperatureSensors.begin();
		}
	}

	Serial.printf("[DS18B20] no devices on GPIO%u; check DATA wiring, VDD, and pull-up\n",
							static_cast<unsigned>(PIN_DS18B20));
	return false;
}

bool readTemperature(float& temperatureC) {
	if (!dsAvailable) return false;

	for (uint8_t attempt = 0; attempt < DS18B20_READ_ATTEMPTS; ++attempt) {
		temperatureSensors.requestTemperatures();
		const float reading = temperatureSensors.getTempCByIndex(0);
		if (reading != DEVICE_DISCONNECTED_C && isfinite(reading)) {
			temperatureC = reading;
			return true;
		}

		Serial.printf("[DS18B20] invalid conversion %.2f C (attempt %u)\n",
									reading, static_cast<unsigned>(attempt + 1));
		if (attempt + 1 < DS18B20_READ_ATTEMPTS) {
			delay(DS18B20_RETRY_DELAY_MS);
		}
	}

	return false;
}

bool inaReadRegister(uint8_t reg, uint16_t& value) {
	sensorI2C.beginTransmission(INA226_ADDRESS);
	sensorI2C.write(reg);
	if (sensorI2C.endTransmission(false) != 0) return false;
	if (sensorI2C.requestFrom(INA226_ADDRESS, static_cast<uint8_t>(2)) != 2) return false;

	value = static_cast<uint16_t>(sensorI2C.read()) << 8;
	value |= static_cast<uint16_t>(sensorI2C.read());
	return true;
}

bool inaWriteRegister(uint8_t reg, uint16_t value) {
	sensorI2C.beginTransmission(INA226_ADDRESS);
	sensorI2C.write(reg);
	sensorI2C.write(static_cast<uint8_t>(value >> 8));
	sensorI2C.write(static_cast<uint8_t>(value & 0xFF));
	return sensorI2C.endTransmission() == 0;
}

bool initializeINA226() {
	sensorI2C.begin(PIN_INA_SDA, PIN_INA_SCL, 100000);
	sensorI2C.setTimeOut(30);

	uint16_t manufacturer = 0;
	uint16_t dieId = 0;
	const bool present =
		inaReadRegister(0xFE, manufacturer) &&
		inaReadRegister(0xFF, dieId) &&
		manufacturer == 0x5449;

	if (present) {
		inaWriteRegister(0x00, 0x4127);
		Serial.printf("[INA226] connected, die=0x%04X\n", dieId);
	} else {
		Serial.println("[INA226] not detected at 0x40");
	}
	return present;
}

bool readINA226(float& busVoltageV, float& shuntVoltageMv,
								float& currentMa, float& powerW) {
	if (!inaAvailable) return false;

	uint16_t rawBus = 0;
	uint16_t rawShunt = 0;
	if (!inaReadRegister(0x02, rawBus) || !inaReadRegister(0x01, rawShunt)) {
		inaAvailable = false;
		return false;
	}

	Serial.printf("[INA226] raw bus=0x%04X raw shunt=0x%04X\n", rawBus, rawShunt);
	busVoltageV = static_cast<float>(rawBus) * 0.00125f;
	shuntVoltageMv = static_cast<float>(static_cast<int16_t>(rawShunt)) * 0.0025f;
	const float currentA = (shuntVoltageMv / 1000.0f) / INA226_SHUNT_OHMS;
	currentMa = currentA * 1000.0f;
	powerW = busVoltageV * currentA;
	return true;
}

String mqttClientId() {
	char value[24];
	const char* profile = camPmuReady ? "CAM" : "H795";
	const uint32_t suffix = static_cast<uint32_t>(ESP.getEfuseMac() & 0xFFFFFFULL);
	snprintf(value, sizeof(value), "MC-%s-%06lX", profile, static_cast<unsigned long>(suffix));
	return String(value);
}

bool publishMeasurements() {
	if (!mqtt.connected()) {
		Serial.println("[MQTT] not connected; send M first");
		return false;
	}

	JsonDocument doc;
	doc["controller_id"] = mqttClientId();
	doc["board"] = camPmuReady ? "LilyGo-T-SIM7080G-S3-PMU" : "T-SIM7080G-S3-Standard";
	doc["timestamp_ms"] = cached.measuredAtMs;
	doc["temperature_valid"] = cached.temperatureValid;
	if (cached.temperatureValid) doc["temperature_c"] = cached.temperatureC;
	else doc["temperature_c"] = nullptr;
	doc["weg_valid"] = cached.wegValid;
	if (cached.wegValid) {
		doc["weg_raw"] = cached.wegRaw;
		doc["weg_voltage_v"] = cached.wegVoltageV;
	} else {
		doc["weg_raw"] = nullptr;
		doc["weg_voltage_v"] = nullptr;
	}
	doc["ina_valid"] = cached.inaValid;
	if (cached.inaValid) {
		doc["ina_bus_v"] = cached.inaBusVoltageV;
		doc["ina_shunt_mv"] = cached.inaShuntVoltageMv;
		doc["ina_current_ma"] = cached.inaCurrentMa;
		doc["ina_power_w"] = cached.inaPowerW;
	}
	if (modemReady) doc["signal_quality"] = modem.getSignalQuality();

	String payload;
	serializeJson(doc, payload);
	const bool sent = mqtt.publish(MQTT_TOPIC, payload.c_str(), false);
	Serial.printf("[MQTT] publish %s (%u bytes)\n",
								sent ? "OK" : "FAILED",
								static_cast<unsigned>(payload.length()));
	return sent;
}

void readAllSensors() {
	if (!sensorTestEnabled) {
		Serial.println("[TEST] sensor acquisition is stopped; send P first");
		return;
	}

	Measurements next;
	next.temperatureValid = readTemperature(next.temperatureC);
	next.wegValid = readWegsensor(next.wegRaw, next.wegVoltageV);
	next.inaValid = readINA226(next.inaBusVoltageV, next.inaShuntVoltageMv,
														 next.inaCurrentMa, next.inaPowerW);
	next.measuredAtMs = millis();
	cached = next;

	if (cached.temperatureValid) {
		Serial.printf("[DS18B20] %.2f C\n", cached.temperatureC);
	} else {
		Serial.println("[DS18B20] no valid reading");
	}
	if (cached.wegValid) {
		Serial.printf("[ADS1220] raw=%ld voltage_est=%.6f V\n",
									static_cast<long>(cached.wegRaw), cached.wegVoltageV);
	} else {
		adsAvailable = false;
		Serial.println("[ADS1220] no valid reading; check SPI wiring and GPIO8 DRDY");
	}
	if (cached.inaValid) {
		Serial.printf("[INA226] bus=%.4f V shunt=%.4f mV current=%.2f mA power=%.4f W\n",
									cached.inaBusVoltageV, cached.inaShuntVoltageMv,
									cached.inaCurrentMa, cached.inaPowerW);
	} else {
		Serial.println("[INA226] no valid reading; check I2C wiring and RTS/CTS pads");
	}

	if (mqtt.connected()) publishMeasurements();
}

void powerExternalSensorsOn() {
	if (sensorTestEnabled) {
		readAllSensors();
		return;
	}

	sensorTestEnabled = true;

	dsAvailable = initializeDS18B20();
	inaAvailable = initializeINA226();
	adsAvailable = initializeADS1220();
	Serial.printf("[INIT] ADS1220=%s DS18B20=%s INA226=%s\n",
								adsAvailable ? "OK" : "NOT FOUND",
								dsAvailable ? "OK" : "NOT FOUND",
								inaAvailable ? "OK" : "NOT FOUND");
	readAllSensors();
}

void powerExternalSensorsOff() {
	if (sensorTestEnabled) {
		if (inaAvailable) inaWriteRegister(0x00, 0x4120);
		if (adsAvailable) adsPowerDown();
		disableAdsInterface();
		sensorI2C.end();
		pinMode(PIN_INA_SDA, INPUT);
		pinMode(PIN_INA_SCL, INPUT);
		pinMode(PIN_DS18B20, INPUT);
	}

	digitalWrite(PIN_J7_24V_ENABLE, LOW);
	sensorTestEnabled = false;
	adsAvailable = false;
	dsAvailable = false;
	inaAvailable = false;
	Serial.println("[SENSORS] acquisition OFF; sensor 3V3 remains ON; J7 24 V OFF");
}

bool waitForModem(uint8_t attempts) {
	for (uint8_t attempt = 0; attempt < attempts; ++attempt) {
		if (modem.testAT(1000)) return true;
		Serial.print('.');
	}
	Serial.println();
	return false;
}

void pulseModemPowerKey(uint16_t pulseMs, uint16_t settleMs) {
	digitalWrite(modemPwrKeyPin, LOW);
	delay(100);
	digitalWrite(modemPwrKeyPin, HIGH);
	delay(pulseMs);
	digitalWrite(modemPwrKeyPin, LOW);
	if (settleMs > 0) delay(settleMs);
}

bool waitForModemStartup(uint32_t timeoutMs) {
	const uint32_t started = millis();
	uint8_t retryCount = 0;

	while (millis() - started < timeoutMs) {
		if (modem.testAT(1000)) return true;
		Serial.print('.');

		if (++retryCount >= 16 && millis() - started < timeoutMs) {
			Serial.printf("\n[MODEM] retrying %s PWRKEY\n",
									camPmuReady ? "CAM" : "H795");
			pulseModemPowerKey(MODEM_POWER_ON_PULSE_MS, 0);
			retryCount = 0;
		}
	}

	return modem.testAT(1000);
}

bool configureNbIotMode() {
	if (camPmuReady) {
		modem.sendAT("+CFUN=0");
		if (modem.waitResponse(20000L) != 1) {
			Serial.println("[MODEM] failed to disable RF before NB-IoT setup");
			return false;
		}

		if (!modem.setNetworkMode(2)) {
			Serial.println("[MODEM] failed to select automatic network mode");
			return false;
		}
		if (!modem.setPreferredMode(2)) {
			Serial.println("[MODEM] failed to select NB-IoT preferred mode");
			return false;
		}

		modem.sendAT("+CGDCONT=1,\"IP\",\"", CELLULAR_APN, "\"");
		if (modem.waitResponse(5000L) != 1) {
			Serial.println("[MODEM] failed to configure APN context");
			return false;
		}
		modem.sendAT("+CNCFG=0,1,\"", CELLULAR_APN, "\"");
		if (modem.waitResponse(5000L) != 1) {
			Serial.println("[MODEM] failed to configure NB-IoT data APN");
			return false;
		}

		modem.sendAT("+CFUN=1");
		if (modem.waitResponse(20000L) != 1) {
			Serial.println("[MODEM] failed to enable RF after NB-IoT setup");
			return false;
		}
		Serial.printf("[MODEM] automatic/NB-IoT mode and APN configured: %s\n",
								CELLULAR_APN);
		return true;
	}

	String response;
	bool changed = false;

	modem.sendAT("+CNMP?");
	const bool cnmpRead = modem.waitResponse(1500, response) == 1;
	if (!cnmpRead || response.indexOf("+CNMP: 38") < 0) {
		modem.sendAT("+CNMP=38");
		if (modem.waitResponse(3000) != 1) {
			Serial.println("[MODEM] failed to set LTE-only mode");
			return false;
		}
		changed = true;
	}

	response = "";
	modem.sendAT("+CMNB?");
	const bool cmnbRead = modem.waitResponse(1500, response) == 1;
	if (!cmnbRead || response.indexOf("+CMNB: 1") < 0) {
		modem.sendAT("+CMNB=1");
		if (modem.waitResponse(3000) != 1) {
			Serial.println("[MODEM] failed to select NB-IoT mode");
			return false;
		}
		changed = true;
	}

	if (changed) {
		Serial.println("[MODEM] applying network mode with restart");
		if (!modem.restart()) return false;
		delay(10000);
		if (!waitForModem(10)) return false;
	}
	return true;
}

bool startModem() {
	pinMode(modemPwrKeyPin, OUTPUT);
	digitalWrite(modemPwrKeyPin, LOW);
	pinMode(modemDtrPin, OUTPUT);
	digitalWrite(modemDtrPin, LOW);
	pinMode(modemRingPin, INPUT);

	if (camPmuReady) {
		Serial.println("[MODEM] enabling AXP2101 DC3 at 3.0V");
		PMU.setDC3Voltage(3000);
		PMU.enableDC3();
		delay(200);
		pulseModemPowerKey(CAM_MODEM_INITIAL_PULSE_MS, 2000);
	} else {
		pinMode(H795_MODEM_POWER_SAVE, OUTPUT);
		digitalWrite(H795_MODEM_POWER_SAVE, HIGH);
		delay(50);
	}

	SerialAT.begin(MODEM_BAUD, SERIAL_8N1, modemRxPin, modemTxPin);
	if (!camPmuReady && !modem.testAT(1000)) {
		Serial.println("[MODEM] PS=HIGH; pulsing H795 PWRKEY");
		pulseModemPowerKey(MODEM_POWER_ON_PULSE_MS, 3000);
	}

	if (!waitForModemStartup(30000UL)) {
		powerDownModemRail();
		Serial.println("[MODEM] no AT response; check PMU/DC3, UART, SIM, antenna, PWRKEY");
		return false;
	}
	if (!modem.init()) {
		powerDownModemRail();
		Serial.println("[MODEM] init failed");
		return false;
	}

	modemReady = true;
	Serial.println("[MODEM] SIM7080 initialized");
	return true;
}

bool connectMqtt() {
	mqtt.setServer(MQTT_SERVER, MQTT_PORT);
	mqtt.setKeepAlive(60);
	mqtt.setSocketTimeout(30);
	mqtt.setBufferSize(512);

	const String clientId = mqttClientId();
	bool connected = false;
	if (MQTT_USER[0] == '\0' && MQTT_PASSWORD[0] == '\0') {
		connected = mqtt.connect(clientId.c_str());
	} else {
		connected = mqtt.connect(clientId.c_str(), MQTT_USER, MQTT_PASSWORD);
	}

	Serial.printf("[MQTT] %s, state=%d, client=%s\n",
								connected ? "connected" : "connection failed",
								mqtt.state(), clientId.c_str());
	return connected;
}

void testBrokerTcp() {
	if (!modemReady || !modem.isGprsConnected()) {
		Serial.println("[NET] modem packet data is not connected; send M first");
		return;
	}

	const String localIp = modem.getLocalIP();
	Serial.printf("[NET] local IP=%s; testing %s:%u\n",
								localIp.c_str(), MQTT_SERVER, MQTT_PORT);

	gsmClient.stop();
	const bool connected = gsmClient.connect(MQTT_SERVER, MQTT_PORT);
	Serial.printf("[NET] broker TCP %s\n", connected ? "reachable" : "failed");
	if (connected) gsmClient.stop();
}

bool connectModemAndMqtt() {
	if (!modemReady && !startModem()) return false;

	const SimStatus simStatus = modem.getSimStatus(5000L);
	Serial.printf("[MODEM] SIM status=%d\n", static_cast<int>(simStatus));
	if (simStatus != SIM_READY) {
		Serial.println("[MODEM] SIM is not ready; check insertion and SIM PIN");
		return false;
	}
	if (!configureNbIotMode()) return false;

	String registrationResponse;
	modem.sendAT("+CEREG?");
	const bool registrationRead = modem.waitResponse(2000, registrationResponse) == 1;
	Serial.printf("[MODEM] CEREG=%s\n",
								registrationRead ? registrationResponse.c_str() : "no response");

	bool networkConnected = modem.isNetworkConnected();
	if (!networkConnected) {
		Serial.println("[MODEM] waiting up to 120 seconds for NB-IoT registration");
		const uint32_t registrationStarted = millis();
		uint32_t lastRegistrationLog = 0;
		while (!networkConnected && millis() - registrationStarted < 120000UL) {
			const SIM70xxRegStatus registration = modem.getRegistrationStatus();
			networkConnected = registration == REG_OK_HOME ||
										 registration == REG_OK_ROAMING;
			if (networkConnected) break;
			if (millis() - lastRegistrationLog >= 5000UL) {
				Serial.printf("[MODEM] registration status=%d, CSQ=%d\n",
										static_cast<int>(registration),
										modem.getSignalQuality());
				lastRegistrationLog = millis();
			}
			delay(1000);
		}
	}
	if (!networkConnected) {
		registrationResponse = "";
		modem.sendAT("+CEREG?");
		const bool finalRegistrationRead =
			modem.waitResponse(2000, registrationResponse) == 1;
		Serial.printf("[MODEM] registration after wait: %s\n",
								finalRegistrationRead ? registrationResponse.c_str() : "no response");
		Serial.println("[MODEM] not registered; skipping GPRS/MQTT attach");
		return false;
	}

	const String operatorName = modem.getOperator();
	const int signalQuality = modem.getSignalQuality();
	Serial.printf("[MODEM] operator=%s, CSQ=%d, registered=%s\n",
								operatorName.c_str(), signalQuality,
								networkConnected ? "yes" : "no");

	if (!modem.isGprsConnected() &&
			!modem.gprsConnect(CELLULAR_APN, CELLULAR_USER, CELLULAR_PASSWORD)) {
		Serial.println("[MODEM] gprsConnect failed; check SIM registration, APN, and plan");
		return false;
	}
	cellularReady = true;
	Serial.println("[MODEM] packet data connected");

	if (!connectMqtt()) return false;
	if (cached.measuredAtMs > 0) publishMeasurements();
	return true;
}

void disconnectModem() {
	if (mqtt.connected()) mqtt.disconnect();
	if (modemReady && modem.isGprsConnected()) modem.gprsDisconnect();
	if (modemReady) modem.poweroff();
	SerialAT.end();
	powerDownModemRail();
	modemReady = false;
	cellularReady = false;
	Serial.println("[MODEM] disconnected and powered off");
}

void printHelp() {
	Serial.println("Commands: P=start sensor test; R=sample; O=stop sensor test; L=DS18B20 DATA voltage; M=NB-IoT + MQTT; U=publish; X=disconnect; ?=help");
	Serial.println("T=direct broker TCP diagnostic (after M)");
	Serial.println("v17: sensor 3V3 is always ON; GPIO14 controls J7 24 V and is held LOW.");
	Serial.println("MQTT auth fields are blank; set them locally if the broker requires authentication.");
}

void setup() {
	digitalWrite(PIN_J7_24V_ENABLE, LOW);
	pinMode(PIN_J7_24V_ENABLE, OUTPUT);
	digitalWrite(PIN_J7_24V_ENABLE, LOW);
	analogReadResolution(12);

	Serial.begin(115200);
	const uint32_t serialStarted = millis();
	while (!Serial && millis() - serialStarted < 5000) delay(10);

	Serial.println();
	if (initializeCamPowerManager()) {
		Serial.println("[BOARD] CAM-SIM7080G profile; AXP2101 detected on GPIO15/7");
	} else {
		Serial.println("[BOARD] no AXP2101 on GPIO15/7; using H795 Standard profile");
	}
	pinMode(modemPwrKeyPin, OUTPUT);
	digitalWrite(modemPwrKeyPin, LOW);
	pinMode(modemDtrPin, OUTPUT);
	digitalWrite(modemDtrPin, LOW);
	pinMode(modemRingPin, INPUT);
	if (!camPmuReady) {
		pinMode(H795_MODEM_POWER_SAVE, OUTPUT);
		digitalWrite(H795_MODEM_POWER_SAVE, LOW);
	}
	Serial.println("MultiConnect T-SIM7080G sensor + NB-IoT/MQTT test");
	Serial.printf("APN=%s MQTT=%s:%u topic=%s\n",
								CELLULAR_APN, MQTT_SERVER, MQTT_PORT, MQTT_TOPIC);
	printHelp();
}

void loop() {
	if (mqtt.connected()) mqtt.loop();

	while (Serial.available() > 0) {
		const char command = static_cast<char>(toupper(Serial.read()));
		if (command == 'P') {
			powerExternalSensorsOn();
		} else if (command == 'R') {
			readAllSensors();
		} else if (command == 'O') {
			powerExternalSensorsOff();
		} else if (command == 'L') {
			reportOneWireLine();
		} else if (command == 'M') {
			connectModemAndMqtt();
		} else if (command == 'T') {
			testBrokerTcp();
		} else if (command == 'U') {
			publishMeasurements();
		} else if (command == 'X') {
			disconnectModem();
		} else if (command == '?') {
			printHelp();
		}
	}
}
