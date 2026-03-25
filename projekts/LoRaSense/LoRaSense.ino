#include <Arduino.h>
#include <SPI.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// ---------------- PINS ----------------
static constexpr int PIN_SPI_MISO   = 39;
static constexpr int PIN_SPI_SCK    = 2;
static constexpr int PIN_SPI_MOSI   = 1;
static constexpr int PIN_SD_CS      = 7;
static constexpr int PIN_ADS_CS     = 38;
static constexpr int PIN_ADS_DRDY   = 4;
static constexpr int PIN_MOSFET     = 5;
static constexpr int PIN_DS18B20    = 6;

static SPIClass& spiBus = SPI;

// ---------------- ADS1220 ----------------
namespace ADS1220 {
  static constexpr uint8_t CMD_RESET = 0x06;
  static constexpr uint8_t CMD_START = 0x08;
  static constexpr uint8_t CMD_RDATA = 0x10;
  static constexpr uint8_t CMD_WREG  = 0x40;
}

// ---------------- SETTINGS ----------------
static constexpr int32_t ADS_FS = 1 << 23;
static constexpr float   ADS_VREF_VOLTS = 3.3f;

static constexpr uint8_t REG0_DMS  = 0x0E; // AIN0-AIN1, GAIN=128, PGA enabled
static constexpr uint8_t REG0_AIN2 = 0xA1; // AIN2-AVSS, GAIN=1, PGA_BYPASS=1

static constexpr float GAIN_DMS  = 128.0f;
static constexpr float GAIN_AIN2 = 1.0f;

static constexpr float EXPECTED_MV  = 3.8f;
static constexpr float TOLERANCE_MV = 1.0f;
static constexpr int   RETRY_MAX    = 2;

static constexpr uint32_t DRDY_TIMEOUT_US = 10000;

// REG1 = normal mode, 600 SPS, single-shot
static constexpr uint8_t ADS_REG1 = 0xA0;
static constexpr uint8_t ADS_REG2 = 0x40;
static constexpr uint8_t ADS_REG3 = 0x00;

// ---------------- DS18B20 ----------------
static OneWire oneWire(PIN_DS18B20);
static DallasTemperature dsSensor(&oneWire);

static float ds_temp_c = NAN;
static bool  ds_pending = false;
static uint32_t ds_request_ms = 0;
static uint32_t last_ds_request_loop_ms = 0;

static constexpr uint32_t DS_INTERVAL_MS = 1000;
static constexpr uint32_t DS_WAIT_MS     = 150;

// ---------------- AIN2 slow read ----------------
static float ain2_mV = NAN;
static uint32_t last_ain2_read_ms = 0;
static constexpr uint32_t AIN2_INTERVAL_MS = 1000;

// ---------------- Helpers ----------------
static inline void adsCS(bool en) {
  digitalWrite(PIN_ADS_CS, en ? LOW : HIGH);
}

static inline uint8_t adsXfer(uint8_t b) {
  return spiBus.transfer(b);
}

static float rawTo_mV_gain(int32_t raw, float gain) {
  float v = ((float)raw * ADS_VREF_VOLTS) / (gain * (float)ADS_FS);
  return v * 1000.0f;
}

static float dmsTo_mV_per_V(float dms_mV) {
  return dms_mV / ADS_VREF_VOLTS;
}

// ---------------- ADS low-level ----------------
static void adsCommand(uint8_t cmd) {
  adsCS(true);
  adsXfer(cmd);
  adsCS(false);
}

static void adsWriteRegs4(uint8_t r0, uint8_t r1, uint8_t r2, uint8_t r3) {
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x03);
  adsXfer(r0);
  adsXfer(r1);
  adsXfer(r2);
  adsXfer(r3);
  adsCS(false);
}

static void adsWriteReg0(uint8_t r0) {
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x00);
  adsXfer(r0);
  adsCS(false);
}

static int32_t adsReadRaw_RDATA() {
  adsCS(true);
  adsXfer(ADS1220::CMD_RDATA);
  uint8_t b2 = adsXfer(0xFF);
  uint8_t b1 = adsXfer(0xFF);
  uint8_t b0 = adsXfer(0xFF);
  adsCS(false);

  int32_t raw = ((uint32_t)b2 << 16) | ((uint32_t)b1 << 8) | b0;
  if (raw & 0x00800000) raw |= 0xFF000000;
  return raw;
}

static bool adsWaitDrdyLow(uint32_t timeout_us) {
  uint32_t start = micros();
  while (digitalRead(PIN_ADS_DRDY) == HIGH) {
    if ((uint32_t)(micros() - start) >= timeout_us) {
      return false;
    }
    delayMicroseconds(5);
  }
  return true;
}

static int32_t readSingleShot_DRDY() {
  for (int attempt = 0; attempt <= RETRY_MAX; ++attempt) {
    adsCommand(ADS1220::CMD_START);

    if (!adsWaitDrdyLow(DRDY_TIMEOUT_US)) {
      delayMicroseconds(100);
      continue;
    }

    return adsReadRaw_RDATA();
  }
  return -1;
}

static bool readChannel(uint8_t reg0, float gain, float& out_mV) {
  spiBus.beginTransaction(SPISettings(4000000, MSBFIRST, SPI_MODE1));
  adsWriteReg0(reg0);

  // discard first conversion after mux switch
  (void)readSingleShot_DRDY();

  // real conversion
  int32_t raw = readSingleShot_DRDY();
  spiBus.endTransaction();

  if (raw == -1) {
    out_mV = NAN;
    return false;
  }

  out_mV = rawTo_mV_gain(raw, gain);
  return true;
}

// fast DMS read without channel hopping
static bool readDmsFast(float& out_mV) {
  spiBus.beginTransaction(SPISettings(4000000, MSBFIRST, SPI_MODE1));
  int32_t raw = readSingleShot_DRDY();
  spiBus.endTransaction();

  if (raw == -1) {
    out_mV = NAN;
    return false;
  }

  out_mV = rawTo_mV_gain(raw, GAIN_DMS);
  return true;
}

// ---------------- Initialization ----------------
static void adsInit_fast() {
  pinMode(PIN_SD_CS, OUTPUT);
  digitalWrite(PIN_SD_CS, HIGH);

  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);

  pinMode(PIN_ADS_DRDY, INPUT);

  pinMode(PIN_MOSFET, OUTPUT);
  digitalWrite(PIN_MOSFET, LOW);

  spiBus.beginTransaction(SPISettings(4000000, MSBFIRST, SPI_MODE1));
  adsCommand(ADS1220::CMD_RESET);
  delay(5);

  adsWriteRegs4(REG0_DMS, ADS_REG1, ADS_REG2, ADS_REG3);
  spiBus.endTransaction();
}

// ---------------- Main ----------------
void setup() {
  Serial.begin(921600);
  delay(300);

  spiBus.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI);
  adsInit_fast();

  dsSensor.begin();
  dsSensor.setWaitForConversion(false);

  uint8_t devCount = dsSensor.getDeviceCount();
  for (uint8_t i = 0; i < devCount; ++i) {
    DeviceAddress addr;
    if (dsSensor.getAddress(addr, i)) {
      dsSensor.setResolution(addr, 9);
    }
  }

  ds_pending = false;
  ds_request_ms = 0;
  last_ds_request_loop_ms = 0;
  last_ain2_read_ms = 0;

  Serial.println("# ADS1220 self-test + fast DMS + slow AIN2 + slow DS18B20");
  Serial.println("# Output: dms_mV, dms_mV_per_V, ain2_mV, temp_C, frames/s");
}

// ---------------- loop ----------------
void loop() {
  enum State : uint8_t { ST_SELFTEST, ST_PAUSE_AFTER, ST_RUN };
  static State st = ST_SELFTEST;

  static uint32_t pauseUntilMs = 0;

  static float base5[5];
  static float on5[5];

  auto avg5 = [](const float a[5]) -> float {
    double s = 0.0;
    for (int i = 0; i < 5; ++i) s += a[i];
    return (float)(s / 5.0);
  };

  // ----------- SELFTEST -----------
  if (st == ST_SELFTEST) {
    spiBus.beginTransaction(SPISettings(4000000, MSBFIRST, SPI_MODE1));
    adsWriteReg0(REG0_DMS);
    spiBus.endTransaction();

    digitalWrite(PIN_MOSFET, LOW);

    Serial.println("\n# ===== SELFTEST START =====");
    Serial.println("# Baseline: taking 5 samples...");

    for (int i = 0; i < 5; ) {
      float mv;
      if (!readChannel(REG0_DMS, GAIN_DMS, mv)) continue;
      base5[i] = mv;

      Serial.print("# B"); Serial.print(i); Serial.print("=");
      Serial.println(mv, 6);
      i++;
    }

    float bAvg = avg5(base5);
    Serial.print("# Baseline AVG mV=");
    Serial.println(bAvg, 6);

    Serial.println("# MOSFET ON");
    digitalWrite(PIN_MOSFET, HIGH);
    delay(50);

    Serial.println("# MOSFET-ON: taking 5 samples...");
    for (int i = 0; i < 5; ) {
      float mv;
      if (!readChannel(REG0_DMS, GAIN_DMS, mv)) continue;
      on5[i] = mv;

      Serial.print("# M"); Serial.print(i); Serial.print("=");
      Serial.print(mv, 6);
      Serial.print("\tDELTA=");
      Serial.println(mv - bAvg, 6);
      i++;
    }

    float mAvg = avg5(on5);
    float delta = mAvg - bAvg;

    Serial.print("# MOSFET-ON AVG mV=");
    Serial.println(mAvg, 6);
    Serial.print("# DELTA(AVG) mV=");
    Serial.println(delta, 6);

    bool ok = (fabs(fabs(delta) - EXPECTED_MV) <= TOLERANCE_MV);
    if (ok) {
      Serial.println("# ===== SELFTEST OK =====");
    } else {
      Serial.println("# ===== SELFTEST FAIL =====");
      Serial.print("# Expected ~");
      Serial.print(EXPECTED_MV, 3);
      Serial.print(" ± ");
      Serial.println(TOLERANCE_MV, 3);
    }

    digitalWrite(PIN_MOSFET, LOW);
    Serial.println("# MOSFET OFF");

    Serial.println("# Pausing 5s...");
    pauseUntilMs = millis() + 5000;
    Serial.println("# ===== SELFTEST END =====\n");

    st = ST_PAUSE_AFTER;
    return;
  }

  // ----------- PAUSE AFTER TEST -----------
  if (st == ST_PAUSE_AFTER) {
    if ((int32_t)(millis() - pauseUntilMs) < 0) {
      if (!ds_pending && (millis() - last_ds_request_loop_ms >= DS_INTERVAL_MS)) {
        dsSensor.requestTemperatures();
        ds_pending = true;
        ds_request_ms = millis();
        last_ds_request_loop_ms = millis();
      }

      if (ds_pending && (millis() - ds_request_ms >= DS_WAIT_MS)) {
        float tmp = dsSensor.getTempCByIndex(0);
        ds_temp_c = (tmp != DEVICE_DISCONNECTED_C) ? tmp : NAN;
        ds_pending = false;
      }
      return;
    }

    // make sure DMS channel is active after test
    spiBus.beginTransaction(SPISettings(4000000, MSBFIRST, SPI_MODE1));
    adsWriteReg0(REG0_DMS);
    spiBus.endTransaction();

    // discard first conversion after entering run
    float dummy;
    readDmsFast(dummy);

    st = ST_RUN;
    return;
  }

  // ----------- RUN -----------
  static float dms_mV = NAN;
  static float dms_mV_per_V = NAN;
  static uint32_t frameCount = 0;
  static uint32_t lastFrameReportMs = 0;
  static uint32_t framesPerSecond = 0;

  // fast channel: DMS every loop
  if (readDmsFast(dms_mV)) {
    dms_mV_per_V = dmsTo_mV_per_V(dms_mV);
    frameCount++;
  } else {
    dms_mV_per_V = NAN;
  }

  uint32_t now = millis();

  // report frames/s once per second
  if (now - lastFrameReportMs >= 1000) {
    framesPerSecond = frameCount;
    frameCount = 0;
    lastFrameReportMs = now;
  }

  // slow channel: AIN2 once per second
  if (now - last_ain2_read_ms >= AIN2_INTERVAL_MS) {
    float tmpAin2 = NAN;
    if (readChannel(REG0_AIN2, GAIN_AIN2, tmpAin2)) {
      ain2_mV = tmpAin2;
    }

    // switch back to DMS
    spiBus.beginTransaction(SPISettings(4000000, MSBFIRST, SPI_MODE1));
    adsWriteReg0(REG0_DMS);
    spiBus.endTransaction();

    // discard one after switching back
    float dummy;
    readDmsFast(dummy);

    last_ain2_read_ms = now;
  }

  // DS18B20 once per second
  if (!ds_pending && (now - last_ds_request_loop_ms >= DS_INTERVAL_MS)) {
    dsSensor.requestTemperatures();
    ds_pending = true;
    ds_request_ms = now;
    last_ds_request_loop_ms = now;
  }

  if (ds_pending && (now - ds_request_ms >= DS_WAIT_MS)) {
    float tmp = dsSensor.getTempCByIndex(0);
    ds_temp_c = (tmp != DEVICE_DISCONNECTED_C) ? tmp : NAN;
    ds_pending = false;
  }

  // output every loop
  Serial.print("dms_mV\t");
  if (!isnan(dms_mV)) Serial.print(dms_mV, 6);
  else Serial.print("nan");

  Serial.print("\tdms_mV_per_V\t");
  if (!isnan(dms_mV_per_V)) Serial.print(dms_mV_per_V, 6);
  else Serial.print("nan");

  Serial.print("\tain2_mV\t");
  if (!isnan(ain2_mV)) Serial.print(ain2_mV, 6);
  else Serial.print("nan");

  Serial.print("\ttemp_C\t");
  if (!isnan(ds_temp_c)) Serial.print(ds_temp_c, 3);
  else Serial.print("nan");

  Serial.print("\tframes/s\t");
  Serial.println(framesPerSecond);
}