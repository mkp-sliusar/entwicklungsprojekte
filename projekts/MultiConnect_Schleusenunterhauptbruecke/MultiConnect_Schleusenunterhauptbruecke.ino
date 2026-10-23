#define TINY_GSM_MODEM_SIM7080
#define TINY_GSM_RX_BUFFER 1024
#define XPOWERS_CHIP_AXP2101

#include <Arduino.h>
#include <ArduinoJson.h>
#include <DallasTemperature.h>
#include <DNSServer.h>
#include <LittleFS.h>
#include <OneWire.h>
#include <PubSubClient.h>
#include <Preferences.h>
#include <SPI.h>
#include <TinyGsmClient.h>
#include <Wire.h>
#include <WebServer.h>
#include <WiFi.h>
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
static constexpr uint8_t PIN_AP_MODE = 2;
static constexpr uint16_t AP_MODE_THRESHOLD_MV = 350;
static constexpr char AP_PASSWORD[] = "12345678";
static constexpr char NVS_NAMESPACE[] = "mclilygo";
static constexpr uint8_t BATTERY_TYPE_LIPO_18650 = 0;
static constexpr uint8_t BATTERY_TYPE_SAFT_36V = 1;

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
static constexpr char MQTT_TOPIC_PREFIX[] = "test/funksystemiot/nbiot/07152_05";
static constexpr char LEGACY_DEFAULT_MQTT_TOPIC[] = "test/funksystemiot/nbiot/07152_05/1081ACA0ED30";

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
	float wegPositionMm = NAN;
	bool inaValid = false;
	float inaBusVoltageV = NAN;
	float inaShuntVoltageMv = NAN;
	float inaCurrentMa = NAN;
	float inaPowerW = NAN;
	bool batteryValid = false;
	uint16_t batteryMillivolts = 0;
	float batteryVoltageV = NAN;
	uint8_t batteryPercent = 0;
	uint32_t measuredAtMs = 0;
};

	uint16_t measurementIntervalMinutes = 1;
	uint16_t sendIntervalCycles = 15;
	String cellularUser = CELLULAR_USER;
	String cellularPassword = CELLULAR_PASSWORD;
	String mqttServer = MQTT_SERVER;
	String mqttUser = MQTT_USER;
	String mqttPassword = MQTT_PASSWORD;
	String mqttTopic;
	uint16_t mqttPort = MQTT_PORT;
	uint16_t measurementIntervalMinutes = 1;
	uint16_t sendIntervalMinutes = 15;
	bool lowPowerEnabled = true;
	bool autoRedirectEnabled = false;
	uint8_t batteryType = BATTERY_TYPE_LIPO_18650;
	int32_t wegRaw0 = 0;
	int32_t wegRaw1 = 8388607;
	float wegMm0 = 0.0f;
	float wegMm1 = 10.0f;
};

Measurements cached;
AppConfig config;
Preferences preferences;
WebServer server(80);
DNSServer dnsServer;
bool sensorTestEnabled = false;
	const uint32_t legacySendMinutes = preferences.getUInt(
		"sendInterval", preferences.getUInt("interval", 15UL));
	const uint32_t migratedSendCycles =
		(legacySendMinutes + config.measurementIntervalMinutes - 1UL) /
		config.measurementIntervalMinutes;
	config.sendIntervalCycles = constrain(
		preferences.getUInt("sendCycles", migratedSendCycles), 1UL, 1440UL);
bool modemReady = false;
bool cellularReady = false;
bool apMode = false;
bool littleFsReady = false;
bool dnsReady = false;
volatile bool mqttTaskBusy = false;
volatile uint8_t mqttTaskResult = 0;
volatile bool mqttLastPublishSuccess = false;
volatile int lastSignalQuality = -1;
bool mqttTaskMeasureBeforePublish = true;
bool j7OutputEnabled = false;
uint32_t nextMeasurementMs = 0;
uint32_t nextSendMs = 0;

bool publishMeasurements();
String controllerId();
String defaultMqttTopic();

uint32_t intervalToMilliseconds(uint16_t minutes) {
	return static_cast<uint32_t>(minutes) * 60000UL;
}

bool deadlineReached(uint32_t deadlineMs) {
	return static_cast<int32_t>(millis() - deadlineMs) >= 0;
}

	preferences.putUInt("sendCycles", config.sendIntervalCycles);
	preferences.begin(NVS_NAMESPACE, true);
	config.apn = preferences.getString("apn", config.apn);
	config.cellularUser = preferences.getString("gprsUser", config.cellularUser);
	config.cellularPassword = preferences.getString("gprsPass", config.cellularPassword);
	config.mqttServer = preferences.getString("mqttHost", config.mqttServer);
	config.mqttUser = preferences.getString("mqttUser", config.mqttUser);
	config.mqttPassword = preferences.getString("mqttPass", config.mqttPassword);
	config.mqttTopic = preferences.getString("mqttTopic", "");
	if (config.mqttTopic.isEmpty() || config.mqttTopic == LEGACY_DEFAULT_MQTT_TOPIC) {
		config.mqttTopic = defaultMqttTopic();
	}
	config.mqttPort = preferences.getUShort("mqttPort", config.mqttPort);
	config.measurementIntervalMinutes = constrain(
		preferences.getUInt("measureInterval", config.measurementIntervalMinutes), 1UL, 1440UL);
	config.sendIntervalMinutes = constrain(
		preferences.getUInt("sendInterval", preferences.getUInt("interval", config.sendIntervalMinutes)),
		1UL, 1440UL);
	config.lowPowerEnabled = preferences.getBool("lowPower", config.lowPowerEnabled);
	config.autoRedirectEnabled = preferences.getBool("autoRedirect", config.autoRedirectEnabled);
	config.batteryType = constrain(preferences.getUChar("batteryType", config.batteryType),
											  BATTERY_TYPE_LIPO_18650, BATTERY_TYPE_SAFT_36V);
	config.wegRaw0 = preferences.getInt("wegRaw0", config.wegRaw0);
	config.wegRaw1 = preferences.getInt("wegRaw1", config.wegRaw1);
	config.wegMm0 = preferences.getFloat("wegMm0", config.wegMm0);
	config.wegMm1 = preferences.getFloat("wegMm1", config.wegMm1);
	preferences.end();
	if (config.wegRaw0 == config.wegRaw1) {
		config.wegRaw0 = 0;
		config.wegRaw1 = 8388607;
	}
}

void saveConfig() {
	preferences.begin(NVS_NAMESPACE, false);
	preferences.putString("apn", config.apn);
	preferences.putString("gprsUser", config.cellularUser);
	preferences.putString("gprsPass", config.cellularPassword);
	preferences.putString("mqttHost", config.mqttServer);
	preferences.putString("mqttUser", config.mqttUser);
	preferences.putString("mqttPass", config.mqttPassword);
	preferences.putString("mqttTopic", config.mqttTopic);
	preferences.putUShort("mqttPort", config.mqttPort);
	preferences.putUInt("measureInterval", config.measurementIntervalMinutes);
	preferences.putUInt("sendInterval", config.sendIntervalMinutes);
	preferences.putBool("lowPower", config.lowPowerEnabled);
	preferences.putBool("autoRedirect", config.autoRedirectEnabled);
	preferences.putUChar("batteryType", config.batteryType);
	preferences.putInt("wegRaw0", config.wegRaw0);
	preferences.putInt("wegRaw1", config.wegRaw1);
	preferences.putFloat("wegMm0", config.wegMm0);
	preferences.putFloat("wegMm1", config.wegMm1);
	preferences.end();
}

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

bool calculatePositionMm(int32_t raw, float& positionMm) {
	const int32_t rawDelta = config.wegRaw1 - config.wegRaw0;
	if (rawDelta == 0) return false;

	const float ratio = static_cast<float>(raw - config.wegRaw0) /
						static_cast<float>(rawDelta);
	positionMm = config.wegMm0 + ratio * (config.wegMm1 - config.wegMm0);
	positionMm = constrain(positionMm,
								min(config.wegMm0, config.wegMm1),
								max(config.wegMm0, config.wegMm1));
	return isfinite(positionMm);
}

uint8_t batteryPercentFromMillivolts(uint16_t millivolts) {
	struct Point {
		uint16_t millivolts;
		uint8_t percent;
	};

	static constexpr Point lipoCurve[] = {
		{ 3200, 0 }, { 3300, 5 }, { 3500, 10 }, { 3600, 20 },
		{ 3700, 35 }, { 3750, 50 }, { 3800, 60 }, { 3900, 75 },
		{ 4000, 85 }, { 4100, 95 }, { 4200, 100 }
	};
	static constexpr Point saftCurve[] = {
		{ 2800, 0 }, { 3000, 5 }, { 3200, 15 }, { 3300, 25 },
		{ 3400, 40 }, { 3500, 60 }, { 3550, 75 }, { 3600, 95 },
		{ 3650, 100 }
	};

	const Point* curve = config.batteryType == BATTERY_TYPE_SAFT_36V ? saftCurve : lipoCurve;
	const size_t count = config.batteryType == BATTERY_TYPE_SAFT_36V
		? sizeof(saftCurve) / sizeof(saftCurve[0])
		: sizeof(lipoCurve) / sizeof(lipoCurve[0]);
	if (millivolts <= curve[0].millivolts) return curve[0].percent;
	if (millivolts >= curve[count - 1].millivolts) return curve[count - 1].percent;
	for (size_t index = 1; index < count; ++index) {
		if (millivolts <= curve[index].millivolts) {
			const uint16_t lowMv = curve[index - 1].millivolts;
			const uint16_t highMv = curve[index].millivolts;
			const uint8_t lowPercent = curve[index - 1].percent;
			const uint8_t highPercent = curve[index].percent;
			return static_cast<uint8_t>(lowPercent +
				(static_cast<uint32_t>(millivolts - lowMv) *
				 (highPercent - lowPercent)) / (highMv - lowMv));
		}
	}
	return 0;
}

bool readInternalBattery(uint16_t& millivolts, float& voltageV, uint8_t& percent) {
	if (!camPmuReady || !PMU.isBatteryConnect()) return false;
	millivolts = PMU.getBattVoltage();
	if (millivolts < 2000 || millivolts > 5000) return false;
	voltageV = static_cast<float>(millivolts) / 1000.0f;
	percent = batteryPercentFromMillivolts(millivolts);
	return true;
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
		doc["position_mm"] = cached.wegPositionMm;
		doc["weg_voltage_v"] = cached.wegVoltageV;
	} else {
		doc["weg_raw"] = nullptr;
		doc["position_mm"] = nullptr;
		doc["weg_voltage_v"] = nullptr;
	}
	doc["battery_valid"] = cached.batteryValid;
	if (cached.batteryValid) {
		doc["battery_voltage_v"] = cached.batteryVoltageV;
		doc["battery_percent"] = cached.batteryPercent;
	} else {
		doc["battery_voltage_v"] = nullptr;
		doc["battery_percent"] = nullptr;
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
	const bool sent = mqtt.publish(config.mqttTopic.c_str(), payload.c_str(), false);
	mqttLastPublishSuccess = sent;
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
	if (next.wegValid) {
		next.wegValid = calculatePositionMm(next.wegRaw, next.wegPositionMm);
	}
	next.inaValid = readINA226(next.inaBusVoltageV, next.inaShuntVoltageMv,
														 next.inaCurrentMa, next.inaPowerW);
	next.batteryValid = readInternalBattery(next.batteryMillivolts,
											 next.batteryVoltageV, next.batteryPercent);
	next.measuredAtMs = millis();
	cached = next;

	if (cached.temperatureValid) {
		Serial.printf("[DS18B20] %.2f C\n", cached.temperatureC);
	} else {
		Serial.println("[DS18B20] no valid reading");
	}
	if (cached.wegValid) {
		Serial.printf("[ADS1220] position=%.3f mm raw=%ld voltage=%.6f V\n",
									cached.wegPositionMm,
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
	if (cached.batteryValid) {
		Serial.printf("[BATTERY] %u mV / %.3f V / %u %%\n",
									cached.batteryMillivolts,
									cached.batteryVoltageV,
									cached.batteryPercent);
	}

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

	sensorTestEnabled = false;
	adsAvailable = false;
	dsAvailable = false;
	inaAvailable = false;
	Serial.printf("[SENSORS] acquisition OFF; sensor 3.3 V remains ON; J7 24 V %s\n",
							j7OutputEnabled ? "ON" : "OFF");
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

		modem.sendAT("+CGDCONT=1,\"IP\",\"", config.apn.c_str(), "\"");
		if (modem.waitResponse(5000L) != 1) {
			Serial.println("[MODEM] failed to configure APN context");
			return false;
		}
		modem.sendAT("+CNCFG=0,1,\"", config.apn.c_str(), "\"");
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
								config.apn.c_str());
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
	mqtt.setServer(config.mqttServer.c_str(), config.mqttPort);
	mqtt.setKeepAlive(60);
	mqtt.setSocketTimeout(30);
	mqtt.setBufferSize(512);

	const String clientId = mqttClientId();
	bool connected = false;
	if (config.mqttUser.isEmpty() && config.mqttPassword.isEmpty()) {
		connected = mqtt.connect(clientId.c_str());
	} else {
		connected = mqtt.connect(clientId.c_str(), config.mqttUser.c_str(), config.mqttPassword.c_str());
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
								localIp.c_str(), config.mqttServer.c_str(), config.mqttPort);

	gsmClient.stop();
	const bool connected = gsmClient.connect(config.mqttServer.c_str(), config.mqttPort);
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
			lastSignalQuality = modem.getSignalQuality();
			networkConnected = registration == REG_OK_HOME ||
										 registration == REG_OK_ROAMING;
			if (networkConnected) break;
			if (millis() - lastRegistrationLog >= 5000UL) {
				Serial.printf("[MODEM] registration status=%d, CSQ=%d\n",
										static_cast<int>(registration),
									lastSignalQuality);
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
	lastSignalQuality = signalQuality;
	Serial.printf("[MODEM] operator=%s, CSQ=%d, registered=%s\n",
								operatorName.c_str(), signalQuality,
								networkConnected ? "yes" : "no");

	if (!modem.isGprsConnected() &&
			!modem.gprsConnect(config.apn.c_str(), config.cellularUser.c_str(),
										config.cellularPassword.c_str())) {
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

String apSsid() {
	char suffix[7];
	snprintf(suffix, sizeof(suffix), "%06lX",
						static_cast<unsigned long>(ESP.getEfuseMac() & 0xFFFFFFULL));
	return String("MC-LilyGO-") + suffix;
}

String controllerId() {
	char value[13];
	snprintf(value, sizeof(value), "%012llX",
					static_cast<unsigned long long>(ESP.getEfuseMac() & 0xFFFFFFFFFFFFULL));
	return String(value);
}

String defaultMqttTopic() {
	return String(MQTT_TOPIC_PREFIX) + "/" + controllerId();
}

const char* mqttTaskStatusName() {
	switch (mqttTaskResult) {
		case 1: return "connecting";
		case 2: return "published";
		case 3: return "failed";
		default: return "idle";
	}
}

bool streamLittleFsFile(const char* path, const char* contentType) {
	if (!littleFsReady || !LittleFS.exists(path)) return false;
	File file = LittleFS.open(path, "r");
	if (!file) return false;
	server.streamFile(file, contentType);
	file.close();
	return true;
}

void sendJson(const JsonDocument& document, int status = 200) {
	String output;
	serializeJson(document, output);
	server.send(status, "application/json", output);
}

void apiState() {
	JsonDocument document;
	document["name"] = "MKP MultiConnect";
	document["fw"] = "1.1.0-LilyGO";
	document["board"] = camPmuReady ? "LilyGo-T-SIM7080G-S3-PMU" : "T-SIM7080G-S3-Standard";
	document["controller_id"] = controllerId();
	document["mode"] = apMode ? "AP" : "FIELD";
	document["ip"] = apMode ? WiFi.softAPIP().toString() : String("-");
	document["uptime_s"] = millis() / 1000UL;
	document["heap_free"] = ESP.getFreeHeap();
	document["measurement_age_s"] = cached.measuredAtMs > 0
		? (millis() - cached.measuredAtMs) / 1000UL : 0;
	document["timestamp_ms"] = cached.measuredAtMs;
	document["sensor_power_enabled"] = true;
	document["sensor_power_always_on"] = true;
	document["j7_24v_enabled"] = j7OutputEnabled;
	document["mqtt_busy"] = mqttTaskBusy;
	document["mqtt_status"] = mqttTaskStatusName();
	document["mqtt_last_publish_ok"] = mqttLastPublishSuccess;
	document["modem_ready"] = modemReady;
	document["cellular_ready"] = cellularReady;
	document["gprs_password_set"] = !config.cellularPassword.isEmpty();
	document["mqtt_password_set"] = !config.mqttPassword.isEmpty();
	document["temperature_valid"] = cached.temperatureValid;
	if (cached.temperatureValid) document["temperature_c"] = cached.temperatureC;
	document["weg_valid"] = cached.wegValid;
	document["weg_raw"] = cached.wegRaw;
	if (cached.wegValid) {
		document["position_mm"] = cached.wegPositionMm;
		document["weg_voltage_v"] = cached.wegVoltageV;
	}
	document["ina_connected"] = inaAvailable;
	document["ina_valid"] = cached.inaValid;
	if (cached.inaValid) {
		document["ina_bus_v"] = cached.inaBusVoltageV;
		document["ina_shunt_mv"] = cached.inaShuntVoltageMv;
		document["ina_current_ma"] = cached.inaCurrentMa;
		document["ina_power_w"] = cached.inaPowerW;
	}
	document["battery_valid"] = cached.batteryValid;
	if (cached.batteryValid) {
		document["battery_voltage_v"] = cached.batteryVoltageV;
		document["battery_percent"] = cached.batteryPercent;
		document["battery_mv"] = cached.batteryMillivolts;
	}
	if (lastSignalQuality >= 0) document["signal_quality"] = lastSignalQuality;

	JsonObject settings = document["cfg"].to<JsonObject>();
	settings["apn"] = config.apn;
	settings["gprs_user"] = config.cellularUser;
	settings["mqtt_server"] = config.mqttServer;
	settings["mqtt_port"] = config.mqttPort;
	settings["mqtt_user"] = config.mqttUser;
	settings["mqtt_topic"] = config.mqttTopic;
	settings["measurement_interval"] = config.measurementIntervalMinutes;
	settings["send_interval"] = config.sendIntervalMinutes;
	settings["low_power"] = config.lowPowerEnabled;
	settings["auto_redirect"] = config.autoRedirectEnabled;
	settings["battery_type"] = config.batteryType == BATTERY_TYPE_SAFT_36V
		? "saft-36v" : "lipo-18650";
	JsonObject calibration = settings["weg_cal"].to<JsonObject>();
	calibration["raw0"] = config.wegRaw0;
	calibration["raw1"] = config.wegRaw1;
	calibration["mm0"] = config.wegMm0;
	calibration["mm1"] = config.wegMm1;
	sendJson(document);
}

void apiSaveConfig() {
	if (mqttTaskBusy) {
		server.send(409, "text/plain", "MQTT operation is active");
		return;
	}
	if (!server.hasArg("plain")) {
		server.send(400, "text/plain", "missing body");
		return;
	}
	JsonDocument request;
	if (deserializeJson(request, server.arg("plain"))) {
		server.send(400, "text/plain", "invalid json");
		return;
	}
	AppConfig updated = config;
	if (request["apn"].is<const char*>()) updated.apn = request["apn"].as<String>();
	if (request["gprs_user"].is<const char*>()) updated.cellularUser = request["gprs_user"].as<String>();
	if (request["gprs_password"].is<const char*>()) {
		const String password = request["gprs_password"].as<String>();
		if (!password.isEmpty()) updated.cellularPassword = password;
	}
	if (request["mqtt_server"].is<const char*>()) updated.mqttServer = request["mqtt_server"].as<String>();
	if (request["mqtt_user"].is<const char*>()) updated.mqttUser = request["mqtt_user"].as<String>();
	if (request["mqtt_topic"].is<const char*>()) updated.mqttTopic = request["mqtt_topic"].as<String>();
	if (request["mqtt_password"].is<const char*>()) {
		const String password = request["mqtt_password"].as<String>();
		if (!password.isEmpty()) updated.mqttPassword = password;
	}
	if (request["clear_mqtt_password"] == true) updated.mqttPassword = "";
	if (request["mqtt_port"].is<uint16_t>()) updated.mqttPort = request["mqtt_port"].as<uint16_t>();
	if (request["measurement_interval"].is<uint32_t>()) {
		updated.measurementIntervalMinutes = request["measurement_interval"].as<uint32_t>();
	}
	if (request["send_interval"].is<uint32_t>()) {
		updated.sendIntervalMinutes = request["send_interval"].as<uint32_t>();
	} else if (request["interval"].is<uint32_t>()) {
		updated.sendIntervalMinutes = request["interval"].as<uint32_t>();
	}
	if (request["low_power"].is<bool>()) updated.lowPowerEnabled = request["low_power"].as<bool>();
	if (request["auto_redirect"].is<bool>()) updated.autoRedirectEnabled = request["auto_redirect"].as<bool>();
	if (request["battery_type"].is<const char*>()) {
		const String type = request["battery_type"].as<String>();
		if (type == "lipo-18650") updated.batteryType = BATTERY_TYPE_LIPO_18650;
		else if (type == "saft-36v") updated.batteryType = BATTERY_TYPE_SAFT_36V;
		else {
			server.send(400, "text/plain", "invalid battery type");
			return;
		}
	}
	if (request["weg_cal"].is<JsonObjectConst>()) {
		JsonObjectConst calibration = request["weg_cal"].as<JsonObjectConst>();
		if (!calibration["raw0"].isNull()) updated.wegRaw0 = calibration["raw0"].as<int32_t>();
		if (!calibration["raw1"].isNull()) updated.wegRaw1 = calibration["raw1"].as<int32_t>();
		if (!calibration["mm0"].isNull()) updated.wegMm0 = calibration["mm0"].as<float>();
		if (!calibration["mm1"].isNull()) updated.wegMm1 = calibration["mm1"].as<float>();
	}
	if (updated.apn.isEmpty() || updated.mqttServer.isEmpty() || updated.mqttTopic.isEmpty() ||
		updated.mqttPort == 0 || updated.measurementIntervalMinutes < 1 ||
		updated.measurementIntervalMinutes > 1440 || updated.sendIntervalMinutes < 1 ||
		updated.sendIntervalMinutes > 1440 ||
		updated.wegRaw0 == updated.wegRaw1 || !isfinite(updated.wegMm0) || !isfinite(updated.wegMm1)) {
		server.send(400, "text/plain", "invalid configuration");
		return;
	}
	config = updated;
	saveConfig();
	nextMeasurementMs = millis() + intervalToMilliseconds(config.measurementIntervalMinutes);
	nextSendMs = millis() + intervalToMilliseconds(config.sendIntervalMinutes);
	JsonDocument response;
	response["saved"] = true;
	sendJson(response);
}

void apiSetJ7Output() {
	if (!server.hasArg("plain")) {
		server.send(400, "text/plain", "missing body");
		return;
	}
	JsonDocument request;
	if (deserializeJson(request, server.arg("plain")) || !request["enabled"].is<bool>()) {
		server.send(400, "text/plain", "invalid j7 output request");
		return;
	}
	j7OutputEnabled = request["enabled"].as<bool>();
	digitalWrite(PIN_J7_24V_ENABLE, j7OutputEnabled ? HIGH : LOW);
	Serial.printf("[J7] switched 24 V output %s on GPIO%u\n",
							j7OutputEnabled ? "ON" : "OFF", PIN_J7_24V_ENABLE);
	JsonDocument response;
	response["enabled"] = j7OutputEnabled;
	sendJson(response);
}

void apiMeasureNow() {
	if (mqttTaskBusy) {
		server.send(409, "text/plain", "MQTT operation is active");
		return;
	}
	if (sensorTestEnabled) readAllSensors();
	else powerExternalSensorsOn();
	nextMeasurementMs = millis() + intervalToMilliseconds(config.measurementIntervalMinutes);
	JsonDocument response;
	response["measured_at_ms"] = cached.measuredAtMs;
	sendJson(response, 202);
}

void mqttPublishWorker(void*) {
	if (mqttTaskMeasureBeforePublish) {
		if (sensorTestEnabled) readAllSensors();
		else powerExternalSensorsOn();
		nextMeasurementMs = millis() + intervalToMilliseconds(config.measurementIntervalMinutes);
	}
	const bool connected = connectModemAndMqtt();
	mqttTaskResult = connected && mqttLastPublishSuccess ? 2 : 3;
	disconnectModem();
	if (!apMode && config.lowPowerEnabled) powerExternalSensorsOff();
	if (mqttTaskMeasureBeforePublish) {
		nextSendMs = millis() + intervalToMilliseconds(config.sendIntervalMinutes);
	}
	mqttTaskBusy = false;
	vTaskDelete(nullptr);
}

bool startMqttPublishTask(const char* taskName, bool measureBeforePublish = true) {
	if (mqttTaskBusy) return false;
	mqttTaskBusy = true;
	mqttTaskResult = 1;
	mqttLastPublishSuccess = false;
	mqttTaskMeasureBeforePublish = measureBeforePublish;
	if (xTaskCreatePinnedToCore(mqttPublishWorker, taskName, 8192,
										nullptr, 1, nullptr, 0) != pdPASS) {
		mqttTaskBusy = false;
		mqttTaskResult = 3;
		return false;
	}
	return true;
}

void apiMqttTest() {
	if (mqttTaskBusy) {
		server.send(409, "text/plain", "MQTT operation is active");
		return;
	}
	if (!startMqttPublishTask("mqttPublish")) {
		server.send(503, "text/plain", "could not start MQTT task");
		return;
	}
	JsonDocument response;
	response["status"] = "queued";
	sendJson(response, 202);
}

void apiReboot() {
	server.send(200, "text/plain", "restarting");
	delay(200);
	ESP.restart();
}

void redirectToCaptivePortal() {
	if (server.uri().startsWith("/api/") || !config.autoRedirectEnabled) {
		server.send(404, "text/plain", "Not found");
		return;
	}
	server.sendHeader("Cache-Control", "no-store");
	server.sendHeader("Location", String("http://") + WiFi.softAPIP().toString() + "/", true);
	server.send(302, "text/plain", "Redirecting to MultiConnect");
}

void attachWebRoutes() {
	server.on("/", HTTP_GET, []() {
		server.sendHeader("Cache-Control", "no-store");
		if (!streamLittleFsFile("/index.html", "text/html")) server.send(404, "text/plain", "index.html missing");
	});
	server.on("/i18n.json", HTTP_GET, []() {
		if (!streamLittleFsFile("/i18n.json", "application/json")) server.send(404, "text/plain", "i18n.json missing");
	});
	server.on("/logo.svg", HTTP_GET, []() {
		if (!streamLittleFsFile("/logo.svg", "image/svg+xml")) server.send(404, "text/plain", "logo.svg missing");
	});
	server.on("/api/state", HTTP_GET, apiState);
	server.on("/api/config", HTTP_POST, apiSaveConfig);
	server.on("/api/j7-24v", HTTP_POST, apiSetJ7Output);
	server.on("/api/measure", HTTP_POST, apiMeasureNow);
	server.on("/api/mqtt/test", HTTP_POST, apiMqttTest);
	server.on("/api/reboot", HTTP_POST, apiReboot);
	server.on("/generate_204", HTTP_ANY, redirectToCaptivePortal);
	server.on("/gen_204", HTTP_ANY, redirectToCaptivePortal);
	server.on("/hotspot-detect.html", HTTP_ANY, redirectToCaptivePortal);
	server.on("/connecttest.txt", HTTP_ANY, redirectToCaptivePortal);
	server.on("/ncsi.txt", HTTP_ANY, redirectToCaptivePortal);
	server.onNotFound(redirectToCaptivePortal);
	server.begin();
}

void startAccessPoint() {
	WiFi.mode(WIFI_AP);
	WiFi.setSleep(false);
	WiFi.setTxPower(WIFI_POWER_7dBm);
	const String ssid = apSsid();
	WiFi.softAP(ssid.c_str(), AP_PASSWORD);
	dnsReady = dnsServer.start(53, "*", WiFi.softAPIP());
	littleFsReady = LittleFS.begin(false);
	if (littleFsReady) attachWebRoutes();
	Serial.printf("[AP] SSID=%s IP=%s DNS=%s FS=%s\n", ssid.c_str(),
						WiFi.softAPIP().toString().c_str(), dnsReady ? "ready" : "failed",
						littleFsReady ? "ready" : "failed");
}

bool selectApMode() {
	pinMode(PIN_AP_MODE, INPUT_PULLUP);
	delay(50);
	uint32_t voltageSumMillivolts = 0;
	for (uint8_t sample = 0; sample < 8; ++sample) {
		voltageSumMillivolts += analogReadMilliVolts(PIN_AP_MODE);
		delay(2);
	}
	const uint32_t modePinMillivolts = voltageSumMillivolts / 8;
	const bool selected = modePinMillivolts >= AP_MODE_THRESHOLD_MV;
	Serial.printf("[MODE] GPIO2=%lu mV -> %s mode\n",
						static_cast<unsigned long>(modePinMillivolts), selected ? "AP" : "FIELD");
	return selected;
}

void printHelp() {
	Serial.println("Commands: P=start sampling; R=sample; O=stop sampling; L=DATA voltage; M=MQTT test; U=publish cached; X=disconnect; ?=help");
	Serial.println("Sensor 3V3 is always on; GPIO14 remains LOW and J7 24 V stays off. O stops sampling only.");
}

void processSerialCommands() {
	while (Serial.available() > 0) {
		const char command = static_cast<char>(toupper(Serial.read()));
		if (command == '?') {
			printHelp();
		} else if (command == 'L') {
			reportOneWireLine();
		} else if (command == 'P' || command == 'R') {
			if (mqttTaskBusy) {
				Serial.println("[TEST] MQTT operation is active");
			} else {
				powerExternalSensorsOn();
			}
		} else if (command == 'O') {
			if (!mqttTaskBusy) powerExternalSensorsOff();
		} else if (command == 'M') {
			if (!startMqttPublishTask("serialMqtt")) {
				Serial.println("[MQTT] task already active or could not start");
			}
		} else if (command == 'U') {
			publishMeasurements();
		} else if (command == 'X') {
			if (!mqttTaskBusy) disconnectModem();
		}
	}
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
	loadConfig();
	pinMode(modemPwrKeyPin, OUTPUT);
	digitalWrite(modemPwrKeyPin, LOW);
	pinMode(modemDtrPin, OUTPUT);
	digitalWrite(modemDtrPin, LOW);
	pinMode(modemRingPin, INPUT);
	if (!camPmuReady) {
		pinMode(H795_MODEM_POWER_SAVE, OUTPUT);
		digitalWrite(H795_MODEM_POWER_SAVE, LOW);
	}
	apMode = selectApMode();
	Serial.println("MKP MultiConnect LilyGO controller");
	Serial.printf("APN=%s MQTT=%s:%u topic=%s measure=%u min send=%u min\n",
								config.apn.c_str(), config.mqttServer.c_str(),
							config.mqttPort, config.mqttTopic.c_str(),
							config.measurementIntervalMinutes,
							config.sendIntervalMinutes);
	printHelp();
	if (apMode) {
		startAccessPoint();
		powerExternalSensorsOn();
		nextMeasurementMs = millis() + intervalToMilliseconds(config.measurementIntervalMinutes);
		nextSendMs = millis() + intervalToMilliseconds(config.sendIntervalMinutes);
		Serial.println("[MODE] AP configuration/live monitoring");
	} else {
		WiFi.mode(WIFI_OFF);
		powerExternalSensorsOff();
		nextMeasurementMs = millis();
		nextSendMs = millis();
		Serial.println("[MODE] FIELD MQTT telemetry");
	}
}

void loop() {
	processSerialCommands();
	if (apMode) {
		if (dnsReady) dnsServer.processNextRequest();
		if (littleFsReady) server.handleClient();
		if (!mqttTaskBusy && sensorTestEnabled && deadlineReached(nextMeasurementMs)) {
			readAllSensors();
			nextMeasurementMs = millis() + intervalToMilliseconds(config.measurementIntervalMinutes);
		}
		if (!mqttTaskBusy && deadlineReached(nextSendMs)) {
			if (startMqttPublishTask("apMqtt", false)) {
				nextSendMs = millis() + intervalToMilliseconds(config.sendIntervalMinutes);
			} else {
				nextSendMs = millis() + 5000UL;
			}
		}
		delay(2);
		return;
	}

	if (!mqttTaskBusy && deadlineReached(nextMeasurementMs)) {
		if (sensorTestEnabled) readAllSensors();
		else powerExternalSensorsOn();
		nextMeasurementMs = millis() + intervalToMilliseconds(config.measurementIntervalMinutes);
		if (config.lowPowerEnabled && sensorTestEnabled) powerExternalSensorsOff();
	}
	if (!mqttTaskBusy && deadlineReached(nextSendMs)) {
		if (startMqttPublishTask("fieldMqtt", false)) {
			nextSendMs = millis() + intervalToMilliseconds(config.sendIntervalMinutes);
		} else {
			nextSendMs = millis() + 5000UL;
			Serial.println("[MQTT] failed to create field task; retrying shortly");
		}
	}

	if (config.lowPowerEnabled && !mqttTaskBusy) {
		esp_sleep_enable_timer_wakeup(1000000ULL);
		esp_light_sleep_start();
	} else {
		delay(10);
	}
}
