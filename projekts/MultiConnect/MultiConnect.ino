/*
  LoRaSense V4.3 - All-in-one test and LoRaWAN sketch
 Evgenij test
  Changes in this version:
  - EU868 channel mask restored to 0x00FF.
  - Default LoRaWAN data rate restored to DR3.
  - Sensor measurements are performed and printed before OTAA join.
  - Sensors are also measured periodically while waiting for join.
  - MAX3485 code is included but disabled.

  Hardware:
  ADS1220:
    SCLK  -> GPIO6
    MISO  -> GPIO5
    MOSI  -> GPIO4
    CS    -> GPIO3
    DRDY  -> GPIO45

  MAX3485:
    RE/DE -> GPIO40
    DI    -> GPIO39
    RO    -> GPIO38

  MOSFET -> GPIO34

  INA226:
    SDA   -> GPIO41
    SCL   -> GPIO42

  DS18B20:
    DATA  -> GPIO47
    4.7 kOhm pull-up to 3.3 V

  Important for OneWire 2.3.8 and GPIO47:
  In OneWire_direct_gpio.h replace all:
    pin < 46
  with:
    pin < 49
*/

#include <Arduino.h>

#ifdef LORAWAN_DEVEUI_AUTO
#undef LORAWAN_DEVEUI_AUTO
#endif
#define LORAWAN_DEVEUI_AUTO 0

#include "LoRaWan_APP.h"
#include <SPI.h>
#include <Wire.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// =====================================================
// FEATURE SWITCHES
// =====================================================

#define ENABLE_LORAWAN  1
#define ENABLE_ADS1220  1
#define ENABLE_DMS      1
#define ENABLE_INA226   1
#define ENABLE_DS18B20  1
#define ENABLE_MOSFET   1
#define ENABLE_RS485    0

// GPIO34 powers the analog sensor rail during each measurement cycle.

// =====================================================
// PINS
// =====================================================

constexpr uint8_t PIN_ADS_SCLK = 6;
constexpr uint8_t PIN_ADS_MISO = 5;
constexpr uint8_t PIN_ADS_MOSI = 4;
constexpr uint8_t PIN_ADS_CS   = 3;
constexpr uint8_t PIN_ADS_DRDY = 45;

constexpr uint8_t PIN_RS485_RE_DE = 40;
constexpr uint8_t PIN_RS485_DI    = 39;
constexpr uint8_t PIN_RS485_RO    = 38;

constexpr uint8_t PIN_MOSFET = 34;

constexpr uint8_t PIN_I2C_SDA = 41;
constexpr uint8_t PIN_I2C_SCL = 42;

constexpr uint8_t PIN_DS18B20 = 47;

// =====================================================
// LORAWAN OTAA
// =====================================================

uint8_t devEui[] = {
  0x63, 0x1E, 0x56, 0x18,
  0xDF, 0x8E, 0xB3, 0xD4
};

uint8_t appEui[] = {
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00
};

uint8_t appKey[] = {
  0x63, 0x6A, 0xC0, 0x9B,
  0x24, 0x82, 0x4A, 0x47,
  0x30, 0x32, 0x80, 0x58,
  0xCE, 0x63, 0x6A, 0xDF
};

uint8_t nwkSKey[] = { 0 };
uint8_t appSKey[] = { 0 };
uint32_t devAddr = 0;

uint32_t appTxDutyCycle = 60000;

bool overTheAirActivation = true;
bool loraWanAdr = true;
bool isTxConfirmed = false;

uint8_t appPort = 2;
uint8_t confirmedNbTrials = 1;

// Restored standard Heltec EU868 mask: channels 0...7.
uint16_t userChannelsMask[6] = {
  0x00FF,
  0x0000,
  0x0000,
  0x0000,
  0x0000,
  0x0000
};

LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t loraWanClass = CLASS_A;

extern uint8_t appData[];
extern uint8_t appDataSize;

// =====================================================
// SETTINGS
// =====================================================

constexpr uint8_t ADS_AVERAGE_SAMPLES = 10;
constexpr uint8_t ADS_DISCARD_SAMPLES = 1;
constexpr uint8_t DMS_GAIN = 128;

constexpr int32_t WEG_RAW_AT_0_MM  = 0;
constexpr int32_t WEG_RAW_AT_10_MM = 8388607;
constexpr float WEG_LENGTH_MM = 10.0f;

constexpr uint8_t INA226_ADDRESS = 0x40;

// Read and print sensors every 5 seconds while OTAA is not joined.
constexpr uint32_t PRE_JOIN_MEASUREMENT_INTERVAL_MS = 5000;

// =====================================================
// TYPES
// =====================================================

struct ADSResult {
  bool valid;
  int32_t raw;
  float voltage;
};

struct Measurements {
  bool wegOk = false;
  bool dmsOk = false;
  bool inaOk = false;
  bool temperatureOk = false;

  int32_t wegRaw = 0;
  float wegVoltage = NAN;
  float wegMm = NAN;

  int32_t dmsRaw = 0;
  float dmsVoltage = NAN;
  float dmsMicrovolts = NAN;

  float temperatureC = NAN;
  float busVoltageV = NAN;
  float shuntVoltageMv = NAN;
};

Measurements measurements;

// =====================================================
// OBJECTS
// =====================================================

#if ENABLE_ADS1220
SPIClass adsSPI(HSPI);
SPISettings adsSPISettings(500000, MSBFIRST, SPI_MODE1);
#endif

#if ENABLE_DS18B20
OneWire oneWire(PIN_DS18B20);
DallasTemperature ds18b20(&oneWire);
#endif

#if ENABLE_RS485
HardwareSerial rs485Serial(1);
#endif

bool ads1220Available = false;
bool ina226Available = false;
bool ds18b20Available = false;

uint32_t lastPreJoinMeasurementMs = 0;

// =====================================================
// PAYLOAD HELPERS
// =====================================================

void putInt16BE(uint8_t &index, int16_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

void putUInt16BE(uint8_t &index, uint16_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

void putInt32BE(uint8_t &index, int32_t value) {
  appData[index++] = static_cast<uint8_t>((value >> 24) & 0xFF);
  appData[index++] = static_cast<uint8_t>((value >> 16) & 0xFF);
  appData[index++] = static_cast<uint8_t>((value >> 8) & 0xFF);
  appData[index++] = static_cast<uint8_t>(value & 0xFF);
}

// =====================================================
// ADS1220
// =====================================================

#if ENABLE_ADS1220

constexpr uint8_t ADS_CMD_RESET = 0x06;
constexpr uint8_t ADS_CMD_START = 0x08;
constexpr uint8_t ADS_CMD_RDATA = 0x10;
constexpr uint8_t ADS_CMD_RREG  = 0x20;
constexpr uint8_t ADS_CMD_WREG  = 0x40;

constexpr uint8_t ADS_MUX_AIN0_AIN1 = 0x00;
constexpr uint8_t ADS_MUX_AIN2_AIN3 = 0x50;

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

void adsWriteRegister(uint8_t address, uint8_t value) {
  adsSelect();
  adsSPI.transfer(ADS_CMD_WREG | ((address & 0x03) << 2));
  adsSPI.transfer(value);
  adsDeselect();
}

uint8_t adsReadRegister(uint8_t address) {
  adsSelect();
  adsSPI.transfer(ADS_CMD_RREG | ((address & 0x03) << 2));
  uint8_t value = adsSPI.transfer(0xFF);
  adsDeselect();
  return value;
}

uint8_t adsGainBits(uint8_t gain) {
  switch (gain) {
    case 1:   return 0x00;
    case 2:   return 0x02;
    case 4:   return 0x04;
    case 8:   return 0x06;
    case 16:  return 0x08;
    case 32:  return 0x0A;
    case 64:  return 0x0C;
    case 128: return 0x0E;
    default:  return 0x00;
  }
}

void configureADSChannel(uint8_t mux, uint8_t gain, bool bypassPga) {
  uint8_t reg0 = mux | adsGainBits(gain);

  if (bypassPga) {
    reg0 |= 0x01;
  }

  adsWriteRegister(0, reg0);

  // 20 SPS, normal mode, single-shot.
  adsWriteRegister(1, 0x00);

  // AVDD-AVSS reference, IDAC disabled.
  adsWriteRegister(2, 0xC0);

  adsWriteRegister(3, 0x00);

  delayMicroseconds(100);
}

bool waitForADSReady(uint32_t timeoutMs) {
  uint32_t startMs = millis();

  while (digitalRead(PIN_ADS_DRDY) == HIGH) {
    if (millis() - startMs >= timeoutMs) {
      return false;
    }

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

  if (raw & 0x00800000UL) {
    raw |= 0xFF000000UL;
  }

  return static_cast<int32_t>(raw);
}

bool adsSingleConversion(int32_t &raw) {
  adsCommand(ADS_CMD_START);

  if (!waitForADSReady(500)) {
    return false;
  }

  raw = adsReadRaw();
  return true;
}

ADSResult readADSChannel(uint8_t mux, uint8_t gain, bool bypassPga) {
  ADSResult result = { false, 0, NAN };

  configureADSChannel(mux, gain, bypassPga);

  for (uint8_t i = 0; i < ADS_DISCARD_SAMPLES; i++) {
    int32_t dummy = 0;

    if (!adsSingleConversion(dummy)) {
      return result;
    }
  }

  int64_t sum = 0;

  for (uint8_t i = 0; i < ADS_AVERAGE_SAMPLES; i++) {
    int32_t raw = 0;

    if (!adsSingleConversion(raw)) {
      return result;
    }

    sum += raw;
  }

  result.raw = static_cast<int32_t>(
    sum / ADS_AVERAGE_SAMPLES
  );

  constexpr float nominalReferenceV = 3.300f;

  result.voltage =
    static_cast<float>(result.raw) *
    nominalReferenceV /
    (
      8388608.0f *
      static_cast<float>(gain)
    );

  result.valid = true;
  return result;
}

float wegRawToMillimeters(int32_t raw) {
  const float denominator =
    static_cast<float>(
      WEG_RAW_AT_10_MM - WEG_RAW_AT_0_MM
    );

  if (fabsf(denominator) < 1.0f) {
    return NAN;
  }

  float mm =
    static_cast<float>(
      raw - WEG_RAW_AT_0_MM
    ) *
    WEG_LENGTH_MM /
    denominator;

  return constrain(mm, 0.0f, WEG_LENGTH_MM);
}

bool initializeADS1220() {
  Serial.println("[ADS1220] Initialization");

  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);

  pinMode(PIN_ADS_DRDY, INPUT_PULLUP);

  adsSPI.begin(
    PIN_ADS_SCLK,
    PIN_ADS_MISO,
    PIN_ADS_MOSI,
    PIN_ADS_CS
  );

  adsCommand(ADS_CMD_RESET);
  delay(10);

  adsWriteRegister(0, 0x10);
  delay(2);

  uint8_t readback = adsReadRegister(0);

  Serial.printf(
    "[ADS1220] REG0 readback: 0x%02X\n",
    readback
  );

  if (readback != 0x10) {
    Serial.println("[ADS1220] SPI test failed");
    return false;
  }

  Serial.println("[ADS1220] SPI test OK");
  return true;
}

void readADSMeasurements() {
  measurements.wegOk = false;
  measurements.dmsOk = false;

  ADSResult weg = readADSChannel(
    ADS_MUX_AIN2_AIN3,
    1,
    true
  );

  if (weg.valid) {
    measurements.wegRaw = weg.raw;
    measurements.wegVoltage = weg.voltage;
    measurements.wegMm = wegRawToMillimeters(weg.raw);
    measurements.wegOk = !isnan(measurements.wegMm);
  }

#if ENABLE_DMS
  ADSResult dms = readADSChannel(
    ADS_MUX_AIN0_AIN1,
    DMS_GAIN,
    false
  );

  if (dms.valid) {
    measurements.dmsRaw = dms.raw;
    measurements.dmsVoltage = dms.voltage;
    measurements.dmsMicrovolts =
      dms.voltage * 1000000.0f;
    measurements.dmsOk = true;
  }
#endif
}

#endif

// =====================================================
// INA226
// =====================================================

#if ENABLE_INA226

constexpr uint8_t INA226_REG_SHUNT        = 0x01;
constexpr uint8_t INA226_REG_BUS          = 0x02;
constexpr uint8_t INA226_REG_MANUFACTURER = 0xFE;
constexpr uint8_t INA226_REG_DIE_ID       = 0xFF;

bool readINA226Register(uint8_t reg, uint16_t &value) {
  Wire.beginTransmission(INA226_ADDRESS);
  Wire.write(reg);

  if (Wire.endTransmission(false) != 0) {
    return false;
  }

  if (Wire.requestFrom(
        INA226_ADDRESS,
        static_cast<uint8_t>(2)
      ) != 2) {
    return false;
  }

  value = static_cast<uint16_t>(Wire.read()) << 8;
  value |= static_cast<uint16_t>(Wire.read());

  return true;
}

bool initializeINA226() {
  Serial.println("[INA226] Initialization");

  Wire.beginTransmission(INA226_ADDRESS);

  if (Wire.endTransmission() != 0) {
    Serial.println("[INA226] Not found");
    return false;
  }

  uint16_t manufacturer = 0;
  uint16_t dieId = 0;

  if (!readINA226Register(
        INA226_REG_MANUFACTURER,
        manufacturer
      )) {
    return false;
  }

  if (!readINA226Register(
        INA226_REG_DIE_ID,
        dieId
      )) {
    return false;
  }

  Serial.printf(
    "[INA226] Manufacturer=0x%04X, Die=0x%04X\n",
    manufacturer,
    dieId
  );

  return manufacturer == 0x5449;
}

void readINA226Measurements() {
  measurements.inaOk = false;

  uint16_t rawBus = 0;
  uint16_t rawShunt = 0;

  if (!readINA226Register(INA226_REG_BUS, rawBus) ||
      !readINA226Register(INA226_REG_SHUNT, rawShunt)) {
    return;
  }

  measurements.busVoltageV =
    static_cast<float>(rawBus) * 0.00125f;

  measurements.shuntVoltageMv =
    static_cast<float>(
      static_cast<int16_t>(rawShunt)
    ) * 0.0025f;

  measurements.inaOk = true;
}

#endif

// =====================================================
// DS18B20
// =====================================================

#if ENABLE_DS18B20

bool initializeDS18B20() {
  Serial.println("[DS18B20] Initialization");

  ds18b20.begin();

  uint8_t count = ds18b20.getDeviceCount();

  Serial.printf(
    "[DS18B20] Devices found: %u\n",
    count
  );

  if (count == 0) {
    return false;
  }

  ds18b20.setResolution(12);
  return true;
}

void readTemperature() {
  measurements.temperatureOk = false;

  ds18b20.requestTemperatures();

  float value =
    ds18b20.getTempCByIndex(0);

  if (value == DEVICE_DISCONNECTED_C) {
    return;
  }

  measurements.temperatureC = value;
  measurements.temperatureOk = true;
}

#endif

// =====================================================
// MOSFET
// =====================================================

#if ENABLE_MOSFET

void initializeMOSFET() {
  pinMode(PIN_MOSFET, OUTPUT);
  digitalWrite(PIN_MOSFET, LOW);
}

void pulseMOSFET(uint32_t durationMs = 200) {
  digitalWrite(PIN_MOSFET, HIGH);
  delay(durationMs);
  digitalWrite(PIN_MOSFET, LOW);
}

#endif

// =====================================================
// RS485 - DISABLED
// =====================================================

#if ENABLE_RS485

void initializeRS485() {
  pinMode(PIN_RS485_RE_DE, OUTPUT);
  digitalWrite(PIN_RS485_RE_DE, LOW);

  rs485Serial.begin(
    9600,
    SERIAL_8N1,
    PIN_RS485_RO,
    PIN_RS485_DI
  );
}

#endif

// =====================================================
// SENSOR CYCLE
// =====================================================

void readAllMeasurements() {
#if ENABLE_MOSFET
  // Power the analog sensors before taking measurements.
  digitalWrite(PIN_MOSFET, HIGH);

  // Allow Wegsensor and analog signals to stabilize.
  delay(500);
#endif

#if ENABLE_ADS1220
  if (ads1220Available) {
    readADSMeasurements();
  }
#endif

#if ENABLE_INA226
  if (ina226Available) {
    readINA226Measurements();
  }
#endif

#if ENABLE_DS18B20
  if (ds18b20Available) {
    readTemperature();
  }
#endif

#if ENABLE_MOSFET
  // Switch sensor power off only after all measurements are complete.
  digitalWrite(PIN_MOSFET, LOW);
#endif
}

void printMeasurements(const char *title) {
  Serial.println();
  Serial.println("============================================");
  Serial.println(title);

  if (measurements.temperatureOk) {
    Serial.printf(
      "Temperature: %.2f C\n",
      measurements.temperatureC
    );
  } else {
    Serial.println("Temperature: ERROR");
  }

  if (measurements.wegOk) {
    Serial.printf(
      "Wegsensor: RAW=%ld | %.6f V | %.3f mm\n",
      static_cast<long>(measurements.wegRaw),
      measurements.wegVoltage,
      measurements.wegMm
    );
  } else {
    Serial.println("Wegsensor: ERROR");
  }

#if ENABLE_DMS
  if (measurements.dmsOk) {
    bool saturated =
      measurements.dmsRaw >= 8388606 ||
      measurements.dmsRaw <= -8388607;

    Serial.printf(
      "DMS: RAW=%ld | %.3f uV%s\n",
      static_cast<long>(measurements.dmsRaw),
      measurements.dmsMicrovolts,
      saturated ? " | SATURATED" : ""
    );
  } else {
    Serial.println("DMS: ERROR");
  }
#endif

  if (measurements.inaOk) {
    Serial.printf(
      "INA226: BUS=%.4f V | SHUNT=%.4f mV\n",
      measurements.busVoltageV,
      measurements.shuntVoltageMv
    );
  } else {
    Serial.println("INA226: ERROR");
  }

  Serial.println("============================================");
}

void runPreJoinMeasurementIfDue() {
  if (millis() - lastPreJoinMeasurementMs <
      PRE_JOIN_MEASUREMENT_INTERVAL_MS) {
    return;
  }

  lastPreJoinMeasurementMs = millis();

  readAllMeasurements();
  printMeasurements("PRE-JOIN SENSOR MEASUREMENT");
}

// =====================================================
// PAYLOAD
// =====================================================

static void prepareTxFrame(uint8_t port) {
  (void)port;

  uint8_t index = 0;

  appData[index++] = 1;

  uint8_t status = 0;

  if (measurements.temperatureOk) status |= (1U << 0);
  if (measurements.wegOk)         status |= (1U << 1);
  if (measurements.dmsOk)         status |= (1U << 2);
  if (measurements.inaOk)         status |= (1U << 3);

  bool dmsSaturated =
    measurements.dmsOk &&
    (
      measurements.dmsRaw >= 8388606 ||
      measurements.dmsRaw <= -8388607
    );

  if (dmsSaturated) {
    status |= (1U << 4);
  }

  appData[index++] = status;

  int16_t temperatureEncoded = 0x7FFF;

  if (measurements.temperatureOk) {
    float scaled =
      constrain(
        measurements.temperatureC * 100.0f,
        -32768.0f,
        32766.0f
      );

    temperatureEncoded =
      static_cast<int16_t>(lroundf(scaled));
  }

  putInt16BE(index, temperatureEncoded);

  uint16_t wegEncoded = 0xFFFF;

  if (measurements.wegOk) {
    float scaled =
      constrain(
        measurements.wegMm * 1000.0f,
        0.0f,
        65534.0f
      );

    wegEncoded =
      static_cast<uint16_t>(lroundf(scaled));
  }

  putUInt16BE(index, wegEncoded);

  putInt32BE(
    index,
    measurements.wegOk ? measurements.wegRaw : 0
  );

  int32_t dmsMicrovolts = 0;

  if (measurements.dmsOk) {
    dmsMicrovolts =
      static_cast<int32_t>(
        lroundf(measurements.dmsMicrovolts)
      );
  }

  putInt32BE(index, dmsMicrovolts);

  uint16_t busMillivolts = 0xFFFF;

  if (measurements.inaOk) {
    float scaled =
      constrain(
        measurements.busVoltageV * 1000.0f,
        0.0f,
        65534.0f
      );

    busMillivolts =
      static_cast<uint16_t>(lroundf(scaled));
  }

  putUInt16BE(index, busMillivolts);

  appDataSize = index;

  Serial.printf(
    "[LORA] Payload size: %u bytes\n",
    appDataSize
  );

  Serial.print("[LORA] Payload: ");

  for (uint8_t i = 0; i < appDataSize; i++) {
    Serial.printf("%02X ", appData[i]);
  }

  Serial.println();
}

// =====================================================
// SETUP
// =====================================================

void setup() {
  Serial.begin(115200);

  uint32_t serialStart = millis();

  while (!Serial && millis() - serialStart < 10000) {
    delay(10);
  }

  Serial.println();
  Serial.println("============================================");
  Serial.println("LoRaSense V4.3");
  Serial.println("DR3 + EU868 mask 0x00FF");
  Serial.println("Measurements before OTAA join enabled");
  Serial.println("============================================");

  Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);

#if ENABLE_MOSFET
  initializeMOSFET();
#endif

#if ENABLE_INA226
  Wire.begin(PIN_I2C_SDA, PIN_I2C_SCL);
  Wire.setClock(100000);

  ina226Available = initializeINA226();
#endif

#if ENABLE_DS18B20
  ds18b20Available = initializeDS18B20();
#endif

#if ENABLE_ADS1220
  ads1220Available = initializeADS1220();
#endif

#if ENABLE_RS485
  initializeRS485();
#endif

  Serial.println();
  Serial.println("--------------- STARTUP STATUS --------------");
  Serial.printf(
    "ADS1220: %s\n",
    ads1220Available ? "OK" : "ERROR"
  );
  Serial.printf(
    "INA226:  %s\n",
    ina226Available ? "OK" : "ERROR"
  );
  Serial.printf(
    "DS18B20: %s\n",
    ds18b20Available ? "OK" : "ERROR"
  );
  Serial.printf(
    "RS485:   %s\n",
#if ENABLE_RS485
    "ENABLED"
#else
    "DISABLED"
#endif
  );
  Serial.println("---------------------------------------------");

  // First measurement before starting OTAA join.
  readAllMeasurements();
  printMeasurements("INITIAL SENSOR MEASUREMENT BEFORE JOIN");

  lastPreJoinMeasurementMs = millis();
}

// =====================================================
// LOOP
// =====================================================

void loop() {
#if ENABLE_LORAWAN
  switch (deviceState) {
    case DEVICE_STATE_INIT:
      Serial.println("[LORA] INIT");

      LoRaWAN.init(
        loraWanClass,
        loraWanRegion
      );

      // Restored to the value from the working Heltec example.
      LoRaWAN.setDefaultDR(3);

      deviceState = DEVICE_STATE_JOIN;
      break;

    case DEVICE_STATE_JOIN:
      Serial.println("[LORA] JOIN");
      LoRaWAN.join();
      break;

    case DEVICE_STATE_SEND:
      Serial.println("[LORA] READ + SEND");

      readAllMeasurements();
      printMeasurements("LORAWAN UPLINK MEASUREMENT");
      prepareTxFrame(appPort);

      LoRaWAN.send();

      deviceState = DEVICE_STATE_CYCLE;
      break;

    case DEVICE_STATE_CYCLE:
      Serial.println("[LORA] CYCLE");

      LoRaWAN.cycle(appTxDutyCycle);
      deviceState = DEVICE_STATE_SLEEP;
      break;

    case DEVICE_STATE_SLEEP:
      LoRaWAN.sleep(loraWanClass);
      break;

    default:
      deviceState = DEVICE_STATE_INIT;
      break;
  }
#else
  static uint32_t lastMeasurementMs = 0;

  if (millis() - lastMeasurementMs >= 5000) {
    lastMeasurementMs = millis();

    readAllMeasurements();
    printMeasurements("LOCAL SENSOR MEASUREMENT");
  }

  delay(10);
#endif
}