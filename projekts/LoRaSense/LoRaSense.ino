#include <Arduino.h>
#include <SPI.h>
#include <SD.h>
#include <WiFi.h>
#include <DNSServer.h>
#include <WebServer.h>
#include <Preferences.h>
#include <LittleFS.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h>
#include "HT_SSD1306Wire.h"
#include "logoMKP.h"
#include "HT_DisplayFonts.h"
#include <ctype.h>
#include <math.h>
#include "LoRaWan_APP.h"
// ============================================================
// MKP LoRaSense - Heltec WiFi LoRa 32 V3 fixed-PCB build
//
// CONFIG / AP mode
//   - fast live measurement
//   - web UI
//   - optional SD logging to DAT (UDBF)
//
// FIELD mode
//   - LoRaWAN only
//   - one fresh sensor snapshot is captured immediately before
//     every uplink, then the Heltec LoRaWAN stack returns to sleep
// ============================================================
// ---------------- PINS ----------------
static constexpr int PIN_SPI_MISO = 39;
static constexpr int PIN_SPI_SCK  = 2;
static constexpr int PIN_SPI_MOSI = 1;
static constexpr int PIN_SD_CS    = 7;
static constexpr int PIN_ADS_CS   = 38;
static constexpr int PIN_ADS_DRDY = 4;
static constexpr int PIN_MOSFET   = 5;
static constexpr int PIN_DS18B20  = 6;
static constexpr int PIN_AP_MODE  = 46;  // jumper to GND => CONFIG / AP mode
static constexpr const char* FW_VERSION = "1.8.0";
static constexpr const char* BOARD_NAME = "Heltec WiFi LoRa 32 V3";
static constexpr uint8_t AIN2_MODE_POT  = 0;
static constexpr uint8_t AIN2_MODE_VOLT = 1;
static constexpr float TEMP_FIXED_HZ = 1.0f;
static constexpr float DMS_MIN_HZ = 0.1f;
static constexpr float DMS_MAX_HZ = 2000.0f;
static constexpr float AIN2_MIN_HZ = 1.0f;
static constexpr float AIN2_MAX_HZ = 2000.0f;
static constexpr float ADS_BUDGET_CONVERSIONS_HZ = 4000.0f;
static constexpr size_t DEV_EUI_HEX_LEN = 16;
static constexpr size_t APP_EUI_HEX_LEN = 16;
static constexpr size_t APP_KEY_HEX_LEN = 32;
static constexpr size_t LOG_BASENAME_LEN = 24;
static constexpr size_t AIN2_VOLT_DISPLAY_LEN = 12;
static constexpr size_t AIN2_SCALING_UNIT_LEN = 12;
static WebServer server(80);

static constexpr uint16_t DNS_PORT = 53;
static DNSServer dnsServer;
static Preferences prefs;
static OneWire oneWire(PIN_DS18B20);
static DallasTemperature dsSensor(&oneWire);
static SSD1306Wire OLED_Display(0x3c, 500000, SDA_OLED, SCL_OLED, GEOMETRY_128_64, RST_OLED);
static bool g_oledSupported = true;
static bool g_oledEnabled = true;
static bool g_oledInitDone = false;
static uint32_t g_oledLastDrawMs = 0;
enum class RunMode : uint8_t { FIELD = 0, CONFIG = 1 };
static RunMode g_mode = RunMode::FIELD;
static bool g_loraEnabled = false;
static bool g_loraInitDone = false;
static uint32_t g_radioQuietUntilMs = 0;
static IPAddress g_apIp(0, 0, 0, 0);
static String g_apSsid;
static uint32_t g_diagBootMs = 0;
static uint32_t g_diagLastLoopStatsMs = 0;
static uint32_t g_httpReqCount = 0;
static uint32_t g_httpReqWindowCount = 0;
static uint32_t g_httpReqWindowStartedMs = 0;
static uint32_t g_httpSlowReqCount = 0;
static uint32_t g_httpMaxDurationMs = 0;
static String g_httpMaxDurationUri;
static WiFiServer g_streamServer(3333);
static WiFiClient g_streamClient;
static TaskHandle_t g_streamTaskHandle = nullptr;
static bool g_streamServerStarted = false;
static volatile uint32_t g_lastStreamedSampleSeq = 0;
static String g_streamStatus = "idle";
static uint32_t g_streamClientCount = 0;
static constexpr uint16_t STREAM_TCP_PORT = 3333;
// ---------------- ADS1220 ----------------
namespace ADS1220 {
  static constexpr uint8_t CMD_RESET = 0x06;
  static constexpr uint8_t CMD_START = 0x08;
  static constexpr uint8_t CMD_RDATA = 0x10;
  static constexpr uint8_t CMD_WREG  = 0x40;
}
static constexpr int32_t ADS_FS = 1 << 23;
static constexpr int32_t ADS_FAIL = INT32_MIN;
static constexpr float ADS_VREF_VOLTS = 3.3f;
static constexpr uint8_t REG0_DMS  = 0x0E; // AIN0-AIN1, gain=128, PGA enabled
static constexpr uint8_t REG0_AIN2 = 0xA1; // AIN2-AVSS, gain=1, PGA bypass
static constexpr float GAIN_DMS = 128.0f;
static constexpr float GAIN_AIN2 = 1.0f;
static constexpr float EXPECTED_MV = 3.8f;
static constexpr float TOLERANCE_MV = 1.0f;
static constexpr int RETRY_MAX = 2;
static constexpr uint32_t ADS_MAX_TIMEOUT_US = 400000;
static constexpr uint8_t ADS_REG2_EXTREF_BASE = 0x40; // external reference on REFP0/REFN0
static constexpr uint8_t ADS_REG2_EXTREF_50_60 = 0x44; // external reference + simultaneous 50/60 Hz rejection
static constexpr uint8_t ADS_REG3 = 0x00;
struct Cfg {
  char devEui[DEV_EUI_HEX_LEN + 1];
  char appEui[APP_EUI_HEX_LEN + 1];
  char appKey[APP_KEY_HEX_LEN + 1];
  uint16_t intervalMin;
  bool adr;
  uint8_t dr;
  bool dmsEnabled;
  float dmsHz;
  float dmsKFactor;
  bool ain2Enabled;
  float ain2Hz;
  uint8_t ain2Mode;
  float ain2LengthMm;
  float ain2AdcFullscaleV;
  float ain2InputFullscaleV;
  char ain2VoltDisplay[AIN2_VOLT_DISPLAY_LEN + 1];
  char ain2ScalingUnit[AIN2_SCALING_UNIT_LEN + 1];
  float ain2ScaleMin;
  float ain2ScaleMax;
  bool tempEnabled;
  bool sdLogEnabled;
  bool streamModeEnabled;
  char logBaseName[LOG_BASENAME_LEN + 1];
  uint32_t logRotateKB;
};
static Cfg cfg = {
  "631E5618DF8EB3D4",
  "0000000000000000",
  "636AC09B24824A4730328058CE636ADF",
  15,
  true,
  5,
  true,
  40.0f,
  0.0f,
  true,
  1.0f,
  AIN2_MODE_POT,
  10.0f,
  3.123f,
  10.0f,
  "volts",
  "mm",
  0.0f,
  10.0f,
  true,
  false,
  false,
  "lorasense",
  1024
};
struct PerfEstimate {
  float dmsTargetHz;
  float ain2TargetHz;
  float tempTargetHz;
  float adsRequestedConvHz;
  float adsLoadPct;
  float scale;
  float dmsEstimatedHz;
  float ain2EstimatedHz;
  float totalEstimatedHz;
};
// ---------------- Runtime sensor values ----------------
static float dms_mV = NAN;
static float dms_mV_per_V = NAN;
static int32_t lastDmsRaw = 0;
static bool lastDmsRawSeen = false;
static bool dms_valid = false;
static uint8_t g_dmsFailStreak = 0;
static uint32_t g_dmsLastOkUs = 0;
static float ain2_mV = NAN;
static int32_t lastAin2Raw = 0;
static bool lastAin2RawSeen = false;
static bool ain2_valid = false;
static uint8_t g_ain2FailStreak = 0;
static uint32_t g_ain2LastOkUs = 0;
static float ds_temp_c = NAN;
static bool temp_valid = false;
static bool ds_pending = false;
static uint32_t ds_request_ms = 0;
static uint32_t last_ds_request_loop_ms = 0;
static uint8_t g_tempFailStreak = 0;
static uint32_t g_tempLastOkMs = 0;
static bool selfTestOk = false;
static float selfTestDelta_mV = NAN;
static uint32_t last_dms_read_us = 0;
static uint32_t last_ain2_read_us = 0;
static uint32_t dmsFramesPerSecond = 0;
static uint32_t ain2FramesPerSecond = 0;
static uint32_t tempFramesPerSecond = 0;
static uint32_t totalFramesPerSecond = 0;
static uint32_t framesPerSecond = 0; // backward-compatible alias = DMS fps
static uint32_t g_lastSampleSeq = 0;
static constexpr uint8_t ADC_FAILS_BEFORE_INVALID = 3;
static constexpr uint32_t ADC_HOLD_TIMEOUT_US = 250000;
static constexpr uint8_t TEMP_FAILS_BEFORE_INVALID = 2;
static constexpr uint32_t TEMP_HOLD_TIMEOUT_MS = 4000;
struct AdsChanCfg {
  uint8_t reg0;
  uint8_t reg1;
  uint8_t reg2;
  float gain;
  float sps;
  uint32_t timeoutUs;
};
static AdsChanCfg g_adsDmsCfg{};
static AdsChanCfg g_adsAin2Cfg{};
static uint8_t g_adsCurReg0 = 0xFF;
static uint8_t g_adsCurReg1 = 0xFF;
static uint8_t g_adsCurReg2 = 0xFF;
static uint8_t g_adsCurReg3 = 0xFF;
static bool g_adsConfigSettled = false;
// ---------------- SD logging ----------------
static bool g_sdMounted = false;
static bool g_sdMountTried = false;
static String g_sdStatus = "disabled";
static File g_logFile;
static String g_logFileName;
static uint32_t g_logFileSeq = 0;
static uint32_t g_logLineCount = 0;
static uint32_t g_logLastFlushMs = 0;
static uint32_t g_lastLoggedSampleSeq = 0;
static volatile bool g_sdBusy = false;
static uint32_t g_lastLogWriteMs = 0;
static uint32_t g_logPeriodMs = 0;   // 0 = log every captured sample (use real sample timestamps)
static uint32_t g_logFlushPeriodMs = 10000; // optimized: rarer flush to reduce logging stalls
struct LogSample {
  uint32_t t_ms;
  uint32_t t_us;
  uint8_t dms_on;
  uint8_t dms_valid;
  int32_t dms_raw;
  float dms_mV;
  float dms_mV_per_V;
  uint8_t ain2_on;
  uint8_t ain2_valid;
  uint8_t ain2_mode;
  int32_t ain2_raw;
  float ain2_mV;
  float ain2_value;
  uint8_t temp_on;
  uint8_t temp_valid;
  float temp_C;
  uint8_t selftest_ok;
};
#pragma pack(push,1)
struct BinLogHeader {
  char magic[8];      // MKPBIN2\0
  uint16_t version;
  uint16_t headerSize;
  uint16_t recordSize;
  uint16_t flags;     // bit0 dms, bit1 ain2, bit2 temp, bit3 ain2_mode_volt
  uint32_t sampleHzNominal;
};
struct BinLogRecordV1 {
  uint32_t dt_us;       // delta from file start, microseconds
  int32_t dms_raw;      // ADS1220 raw signed code
  int32_t ain2_raw;     // ADS1220 raw signed code
  int16_t temp_c_x100;  // DS18B20 * 100
  uint16_t vbat_mV;     // battery in mV
  uint8_t flags;        // bit0 dms_valid, bit1 ain2_valid, bit2 temp_valid, bit3 selftest_ok
  uint8_t reserved;
};

#pragma pack(push,1)
struct StreamPacketV1 {
  char magic[4];        // MKPS
  uint16_t version;     // 1
  uint16_t bytes;       // sizeof(StreamPacketV1)
  uint32_t seq;
  uint32_t t_ms;
  uint32_t t_us;
  uint16_t flags;       // bit0 dms_on bit1 dms_valid bit2 ain2_on bit3 ain2_valid bit4 temp_on bit5 temp_valid bit6 ain2_volt
  int32_t dms_raw;
  float dms_mV;
  float dms_mV_per_V;
  int32_t ain2_raw;
  float ain2_mV;
  float ain2_value;
  int16_t temp_c_x100;
  uint16_t reserved;
};
#pragma pack(pop)
#pragma pack(pop)
static constexpr uint16_t BIN_FLAG_DMS_ENABLED       = 0x0001;
static constexpr uint16_t BIN_FLAG_AIN2_ENABLED      = 0x0002;
static constexpr uint16_t BIN_FLAG_TEMP_ENABLED      = 0x0004;
static constexpr uint16_t BIN_FLAG_AIN2_MODE_VOLT    = 0x0008;
static_assert(sizeof(BinLogHeader) == 20, "BinLogHeader size");
static_assert(sizeof(BinLogRecordV1) == 18, "BinLogRecordV1 size");
static uint32_t g_udbfFileStartUs = 0;
enum class UdbfSignalKind : uint8_t {
  DmsMvPerV = 0,
  Ain2Volt = 1,
  Ain2WayMm = 2,
  TempC = 3,
};
static uint16_t readVBatMilli() { return 0; }
static uint32_t nominalLogHz() {
  float maxHz = 0.0f;
  if (cfg.dmsEnabled && cfg.dmsHz > maxHz) maxHz = cfg.dmsHz;
  if (cfg.ain2Enabled && cfg.ain2Hz > maxHz) maxHz = cfg.ain2Hz;
  if (maxHz < 1.0f) maxHz = 1.0f;
  if (maxHz > 20000.0f) maxHz = 20000.0f;
  return (uint32_t)(maxHz + 0.5f);
}
static uint16_t currentBinFlags() {
  uint16_t flags = 0;
  if (cfg.dmsEnabled) flags |= BIN_FLAG_DMS_ENABLED;
  if (cfg.ain2Enabled) flags |= BIN_FLAG_AIN2_ENABLED;
  if (cfg.tempEnabled) flags |= BIN_FLAG_TEMP_ENABLED;
  if (cfg.ain2Mode == AIN2_MODE_VOLT) flags |= BIN_FLAG_AIN2_MODE_VOLT;
  return flags;
}
static uint16_t currentBinRecordSize() {
  uint16_t size = sizeof(uint32_t); // TimeCounter in microseconds
  if (cfg.dmsEnabled) size += sizeof(int32_t);
  if (cfg.ain2Enabled) size += sizeof(int32_t);
  if (cfg.tempEnabled) size += sizeof(int16_t);
  return size;
}
static bool writeBinHeader(File& f) {
  BinLogHeader h{};
  memcpy(h.magic, "MKPBIN2", 7);
  h.magic[7] = 0;
  h.version = 2;
  h.headerSize = (uint16_t)sizeof(BinLogHeader);
  h.recordSize = currentBinRecordSize();
  h.flags = currentBinFlags();
  h.sampleHzNominal = nominalLogHz();
  return f.write((const uint8_t*)&h, sizeof(h)) == sizeof(h);
}
static uint16_t quantizeTempC100(float t) {
  if (!isfinite(t)) return 0;
  long v = lroundf(t * 100.0f);
  if (v < -32768L) v = -32768L;
  if (v > 32767L) v = 32767L;
  return (uint16_t)(int16_t)v;
}
static bool writeBinRecord(File& f, const LogSample& s) {
  if (g_udbfFileStartUs == 0) g_udbfFileStartUs = s.t_us;
  const uint32_t timeCounterUs = (s.t_us >= g_udbfFileStartUs)
    ? (uint32_t)(s.t_us - g_udbfFileStartUs)
    : 0U;
  if (f.write((const uint8_t*)&timeCounterUs, sizeof(timeCounterUs)) != sizeof(timeCounterUs)) return false;
  if (cfg.dmsEnabled) {
    const int32_t v = s.dms_on ? s.dms_raw : 0;
    if (f.write((const uint8_t*)&v, sizeof(v)) != sizeof(v)) return false;
  }
  if (cfg.ain2Enabled) {
    const int32_t v = s.ain2_on ? s.ain2_raw : 0;
    if (f.write((const uint8_t*)&v, sizeof(v)) != sizeof(v)) return false;
  }
  if (cfg.tempEnabled) {
    const int16_t v = (int16_t)quantizeTempC100(s.temp_C);
    if (f.write((const uint8_t*)&v, sizeof(v)) != sizeof(v)) return false;
  }
  return true;
}
struct UdbfVarDef {
  const char* name;
  const char* unit;
  uint16_t precision;
  UdbfSignalKind kind;
};
static UdbfVarDef g_udbfVars[3];
static uint16_t g_udbfVarCount = 0;
static uint64_t g_udbfStartNs = 0;
static bool g_udbfHeaderWritten = false;
static uint64_t g_udbfRecordIndex = 0;
static uint32_t g_lastQueuedLogMs = 0;
static int monthFromShortName(const char* mon) {
  if (!mon) return 1;
  if (!strncmp(mon, "Jan", 3)) return 1;
  if (!strncmp(mon, "Feb", 3)) return 2;
  if (!strncmp(mon, "Mar", 3)) return 3;
  if (!strncmp(mon, "Apr", 3)) return 4;
  if (!strncmp(mon, "May", 3)) return 5;
  if (!strncmp(mon, "Jun", 3)) return 6;
  if (!strncmp(mon, "Jul", 3)) return 7;
  if (!strncmp(mon, "Aug", 3)) return 8;
  if (!strncmp(mon, "Sep", 3)) return 9;
  if (!strncmp(mon, "Oct", 3)) return 10;
  if (!strncmp(mon, "Nov", 3)) return 11;
  if (!strncmp(mon, "Dec", 3)) return 12;
  return 1;
}
static bool isLeapYear(int year) {
  return ((year % 4) == 0 && (year % 100) != 0) || ((year % 400) == 0);
}
static int daysInMonth(int year, int month) {
  static const int kDays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
  if (month == 2 && isLeapYear(year)) return 29;
  if (month < 1 || month > 12) return 30;
  return kDays[month - 1];
}
static int64_t daysFromCivil(int year, int month, int day) {
  year -= (month <= 2) ? 1 : 0;
  const int era = (year >= 0 ? year : year - 399) / 400;
  const unsigned yoe = (unsigned)(year - era * 400);
  const unsigned doy = (153U * (unsigned)(month + (month > 2 ? -3 : 9)) + 2U) / 5U + (unsigned)day - 1U;
  const unsigned doe = yoe * 365U + yoe / 4U - yoe / 100U + doy;
  return (int64_t)era * 146097LL + (int64_t)doe - 719468LL; // days since 1970-01-01
}
static double makeOleDate(int year, int month, int day, int hour, int minute, int second) {
  // OLE Automation epoch = 1899-12-30
  const int64_t unixDays = daysFromCivil(year, month, day);
  const int64_t oleOffsetDays = 25569LL; // days between 1899-12-30 and 1970-01-01
  const double frac = ((double)hour * 3600.0 + (double)minute * 60.0 + (double)second) / 86400.0;
  return (double)(unixDays + oleOffsetDays) + frac;
}
static double compileTimeOleDate() {
  char mon[4] = {0, 0, 0, 0};
  int day = 1;
  int year = 2026;
  int hh = 0;
  int mm = 0;
  int ss = 0;
  // __DATE__ => "Mmm dd yyyy", __TIME__ => "hh:mm:ss"
  sscanf(__DATE__, "%3s %d %d", mon, &day, &year);
  sscanf(__TIME__, "%d:%d:%d", &hh, &mm, &ss);
  const int month = monthFromShortName(mon);
  return makeOleDate(year, month, day, hh, mm, ss);
}
static void rebuildUdbfLayout() {
  g_udbfVarCount = 0;
  if (cfg.dmsEnabled) {
    g_udbfVars[g_udbfVarCount++] = {"DMS", "mV/V", 3, UdbfSignalKind::DmsMvPerV};
  }
  if (cfg.ain2Enabled) {
    if (cfg.ain2Mode == AIN2_MODE_VOLT) {
      g_udbfVars[g_udbfVarCount++] = {"AIN2", "V", 3, UdbfSignalKind::Ain2Volt};
    } else {
      g_udbfVars[g_udbfVarCount++] = {"WEG", "mm", 3, UdbfSignalKind::Ain2WayMm};
    }
  }
  if (cfg.tempEnabled) {
    g_udbfVars[g_udbfVarCount++] = {"TEMP", "C", 2, UdbfSignalKind::TempC};
  }
}
static void udbfWriteU16(File& f, uint16_t v) {
  uint8_t b[2] = {(uint8_t)(v & 0xFF), (uint8_t)((v >> 8) & 0xFF)};
  f.write(b, sizeof(b));
}
static void udbfWriteDouble(File& f, double v) {
  uint8_t b[sizeof(double)];
  memcpy(b, &v, sizeof(double));
  f.write(b, sizeof(double));
}
static void udbfWriteU32(File& f, uint32_t v) {
  uint8_t b[4] = {(uint8_t)(v & 0xFF), (uint8_t)((v >> 8) & 0xFF), (uint8_t)((v >> 16) & 0xFF), (uint8_t)((v >> 24) & 0xFF)};
  f.write(b, sizeof(b));
}
static void udbfWriteI16(File& f, int16_t v) {
  uint8_t b[2] = {(uint8_t)(v & 0xFF), (uint8_t)((uint16_t)v >> 8)};
  f.write(b, sizeof(b));
}
static int16_t udbfQuantizeValue(float v, uint16_t precision) {
  if (!isfinite(v)) return 0;
  float scale = 1.0f;
  for (uint16_t i = 0; i < precision; ++i) scale *= 10.0f;
  long q = lroundf(v * scale);
  if (q < -32768L) q = -32768L;
  if (q > 32767L) q = 32767L;
  return (int16_t)q;
}
static void udbfWriteCompactStringField(File& f, const char* txt) {
  const uint16_t len = (uint16_t)(strlen(txt) + 1U);
  udbfWriteU16(f, len);
  f.write((const uint8_t*)txt, len);
  f.write((uint8_t)0);
  f.write((uint8_t)0);
}
static void udbfWriteCompactUnitField(File& f, const char* txt) {
  const size_t len = strlen(txt) + 1U;
  f.write((const uint8_t*)txt, len);
  f.write((uint8_t)0);
  f.write((uint8_t)0);
}
static void udbfWriteU64(File& f, uint64_t v) {
  uint8_t b[8];
  for (int i = 0; i < 8; ++i) b[i] = (uint8_t)((v >> (8 * i)) & 0xFF);
  f.write(b, sizeof(b));
}
static void udbfWriteFloat(File& f, float v) {
  uint8_t b[sizeof(float)];
  memcpy(b, &v, sizeof(float));
  f.write(b, sizeof(float));
}
static void udbfWriteCString(File& f, const char* txt) {
  const size_t len = strlen(txt) + 1U;
  udbfWriteU16(f, (uint16_t)len);
  f.write((const uint8_t*)txt, len);
}
static uint64_t fnv1a64(const char* txt) {
  uint64_t h = 1469598103934665603ULL;
  if (!txt) return h;
  while (*txt) {
    h ^= (uint8_t)(*txt++);
    h *= 1099511628211ULL;
  }
  return h;
}
static String hexByte(uint8_t v) {
  char buf[3];
  snprintf(buf, sizeof(buf), "%02x", v);
  return String(buf);
}
static String pseudoUuidFromSeed(const String& seed) {
  uint64_t h1 = fnv1a64(seed.c_str());
  String s2 = seed + "#alt";
  uint64_t h2 = fnv1a64(s2.c_str());
  uint8_t b[16];
  for (int i = 0; i < 8; ++i) {
    b[i] = (uint8_t)((h1 >> (56 - i * 8)) & 0xFF);
    b[8 + i] = (uint8_t)((h2 >> (56 - i * 8)) & 0xFF);
  }
  b[6] = (uint8_t)((b[6] & 0x0F) | 0x40); // UUID v4 style
  b[8] = (uint8_t)((b[8] & 0x3F) | 0x80);
  String out;
  out.reserve(36);
  for (int i = 0; i < 16; ++i) {
    out += hexByte(b[i]);
    if (i == 3 || i == 5 || i == 7 || i == 9) out += '-';
  }
  return out;
}
static void udbfWriteVarAdditionalData(File& f, const char* uuidText) {
  // Mirrors the structure seen in the original DAT files:
  // u16 blockLen, u16 tag=5, u16 subtype=2, cstring uuid
  const size_t uuidLen = strlen(uuidText) + 1U;
  const uint16_t blockLen = (uint16_t)(2 + 2 + 2 + uuidLen);
  udbfWriteU16(f, blockLen);
  udbfWriteU16(f, 5);
  udbfWriteU16(f, 2);
  udbfWriteCString(f, uuidText);
}
static void udbfWriteFileAdditionalData(File& f, const char* jsonText) {
  // Best-effort reproduction of the extra file metadata block used by Gantner.
  // Layout observed in the reference file:
  // u16 blockLen, u32 tag=175, u32 subtype=6, u64 marker='aaa', u16 encoding=3, cstring json
  const size_t jsonLen = strlen(jsonText) + 1U;
  const uint16_t blockLen = (uint16_t)(4 + 4 + 8 + 2 + jsonLen);
  udbfWriteU16(f, blockLen);
  uint8_t tmp4[4] = {0xAF, 0x00, 0x00, 0x00};
  f.write(tmp4, sizeof(tmp4));
  uint8_t tmp6[4] = {0x06, 0x00, 0x00, 0x00};
  f.write(tmp6, sizeof(tmp6));
  uint8_t marker[8] = {0x61, 0x61, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00};
  f.write(marker, sizeof(marker));
  udbfWriteU16(f, 3);
  f.write((const uint8_t*)jsonText, jsonLen);
}
static String udbfBuildMetadataJson() {
  const uint64_t mac = ESP.getEfuseMac();
  char serial[17];
  snprintf(serial, sizeof(serial), "%04X%08lX", (uint16_t)(mac >> 32), (unsigned long)(mac & 0xFFFFFFFFUL));
  char measName[32];
  char mon[4] = {0,0,0,0};
  int day = 1, year = 2026, hh = 0, mi = 0, ss = 0;
  sscanf(__DATE__, "%3s %d %d", mon, &day, &year);
  sscanf(__TIME__, "%d:%d:%d", &hh, &mi, &ss);
  snprintf(measName, sizeof(measName), "%04d-%02d-%02d %02d:%02d:%02d", year, monthFromShortName(mon), day, hh, mi, ss);
  String deviceSeed = String("device:") + serial + ":" + BOARD_NAME;
  String measSeed   = String("meas:") + serial + ":" + measName;
  String sourceSeed = String("source:") + serial + ":" + cfg.logBaseName;
  String deviceId = pseudoUuidFromSeed(deviceSeed);
  String measId   = pseudoUuidFromSeed(measSeed);
  String sourceId = pseudoUuidFromSeed(sourceSeed);
  String json;
  json.reserve(512);
  json += '{';
  json += "\"_id\": \"0.309\",";
  json += "\"DeviceAppVersion\": \""; json += FW_VERSION; json += "\",";
  json += "\"DeviceID\": \""; json += deviceId; json += "\",";
  json += "\"DeviceLocation\": \""; json += BOARD_NAME; json += "\",";
  json += "\"DeviceSerialNumber\": \""; json += serial; json += "\",";
  json += "\"MeasID\": \""; json += measId; json += "\",";
  json += "\"MeasName\": \""; json += measName; json += "\",";
  json += "\"SourceID\": \""; json += sourceId; json += "\",";
  json += "\"SourceName\": \""; json += cfg.logBaseName; json += "\"";
  json += '}';
  return json;
}
static bool writeUdbfHeader(File& f) {
  rebuildUdbfLayout();
  static const char kVendor[] = "UniversalDataBinFile - GANTNER Instruments";
  static const double kStartTimeToDayFactor = 1.0;
  static const uint16_t kTimestampDataType = 7;   // Python DAT opens with this compact uint32 timestamp type
  static const double kTimestampToSecondFactor = 1e-6; // timestamps stored in microseconds
  const double startOleDate = compileTimeOleDate();
  f.write((uint8_t)0);
  udbfWriteU16(f, 107);
  udbfWriteU16(f, (uint16_t)sizeof(kVendor));
  f.write((const uint8_t*)kVendor, sizeof(kVendor));
  f.write((uint8_t)0);
  f.write((uint8_t)0);
  f.write((uint8_t)0);
  udbfWriteDouble(f, kStartTimeToDayFactor);
  udbfWriteU16(f, kTimestampDataType);
  udbfWriteDouble(f, kTimestampToSecondFactor);
  udbfWriteDouble(f, startOleDate);
  udbfWriteU64(f, 0ULL);
  udbfWriteU16(f, g_udbfVarCount);
  for (uint16_t i = 0; i < g_udbfVarCount; ++i) {
    const UdbfVarDef& v = g_udbfVars[i];
    // Python-generated DAT that opens correctly in test.viewer uses a very small
    // channel descriptor layout:
    //   u16 name_len, name+NUL, u16 type, u32 reserved_zero, u16 precision, unit+NUL
    // Keep this byte order exactly; any extra fields shift the unit pointer and the
    // viewer starts interpreting channel scale/type incorrectly.
    udbfWriteCompactStringField(f, v.name);
    udbfWriteU16(f, 4U);       // scalar numeric channel type as in working Python DAT
    udbfWriteU32(f, 0UL);      // reserved/zero field present in Python DAT
    udbfWriteU16(f, v.precision);
    udbfWriteCompactUnitField(f, v.unit);
  }
  static const char kSep[] = "***********************";
  f.write((const uint8_t*)kSep, sizeof(kSep) - 1U);
  g_udbfHeaderWritten = true;
  return true;
}
static float udbfValueFor(const LogSample& s, UdbfSignalKind kind) {
  switch (kind) {
    case UdbfSignalKind::DmsMvPerV:
      return (s.dms_on && s.dms_valid && isfinite(s.dms_mV_per_V)) ? s.dms_mV_per_V : 0.0f;
    case UdbfSignalKind::Ain2Volt:
      return (s.ain2_on && s.ain2_valid && s.ain2_mode == AIN2_MODE_VOLT && isfinite(s.ain2_value)) ? s.ain2_value : 0.0f;
    case UdbfSignalKind::Ain2WayMm:
      return (s.ain2_on && s.ain2_valid && s.ain2_mode != AIN2_MODE_VOLT && isfinite(s.ain2_value)) ? s.ain2_value : 0.0f;
    case UdbfSignalKind::TempC:
      return (s.temp_on && s.temp_valid && isfinite(s.temp_C)) ? s.temp_C : 0.0f;
  }
  return 0.0f;
}
static constexpr uint16_t LOG_BUF_SIZE = 512;
static constexpr uint16_t LOG_BATCH_SIZE = 64; // best result in tests with buffered logging
static constexpr uint32_t LOG_FORCE_FLUSH_MS = 250; // keep short drain latency without write_every_sample flush cost
static LogSample g_logBuf[LOG_BUF_SIZE];
static uint16_t g_logBufHead = 0;
static uint16_t g_logBufTail = 0;
static uint16_t g_logBufCount = 0;
static uint16_t g_logBufMax = 0;
static uint32_t g_logDropped = 0;
static uint32_t g_logLastDrainMs = 0;

static constexpr uint16_t STREAM_BUF_SIZE = 512;
static constexpr uint16_t STREAM_BATCH_SIZE = 64;
static StreamPacketV1 g_streamBuf[STREAM_BUF_SIZE];
static uint16_t g_streamBufHead = 0;
static uint16_t g_streamBufTail = 0;
static uint16_t g_streamBufCount = 0;
static uint16_t g_streamBufMax = 0;
static uint32_t g_streamDropped = 0;
static uint32_t g_streamProduced = 0;
static uint32_t g_streamSent = 0;
// ---------------- LoRaWAN globals required by Heltec ----------------
uint8_t devEui[8]   = { 0 };
uint8_t appEui[8]   = { 0 };
uint8_t appKey[16]  = { 0 };
uint8_t nwkSKey[16] = { 0 };
uint8_t appSKey[16] = { 0 };
uint32_t devAddr    = 0;
// Compatibility aliases for older Heltec cores.
uint8_t DevEui[8]   = { 0 };
uint8_t AppEui[8]   = { 0 };
uint8_t AppKey[16]  = { 0 };
uint8_t NwkSKey[16] = { 0 };
uint8_t AppSKey[16] = { 0 };
uint32_t DevAddr    = 0;
extern uint8_t appData[];
extern uint8_t appDataSize;
uint32_t appTxDutyCycle = 15UL * 60UL * 1000UL;
bool overTheAirActivation = true;
bool loraWanAdr = true;
bool isTxConfirmed = true;
uint8_t appPort = 2;
uint8_t confirmedNbTrials = 3;
uint16_t userChannelsMask[6] = { 0x00FF, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000 };
LoRaMacRegion_t loraWanRegion = ACTIVE_REGION;
DeviceClass_t loraWanClass = CLASS_A;
// High-speed sampling task (CONFIG/AP mode)
static TaskHandle_t g_sensorTaskHandle = nullptr;
static TaskHandle_t g_sdTaskHandle = nullptr;
static volatile bool g_sensorTaskRunning = false;
static volatile bool g_measurementsRunning = false;
static bool g_measurementsInitialized = false;
static bool g_measurementTasksStarted = false;
static SPISettings g_adsSpiSettings(4000000, MSBFIRST, SPI_MODE1);
static SemaphoreHandle_t g_spiBusMutex = nullptr;
static portMUX_TYPE g_logBufMux = portMUX_INITIALIZER_UNLOCKED;
static portMUX_TYPE g_streamBufMux = portMUX_INITIALIZER_UNLOCKED;
static char g_sdWriteBuf[4096];
// ============================================================
// Helpers
// ============================================================
static bool spiBusLock(TickType_t timeoutTicks = pdMS_TO_TICKS(5)) {
  return g_spiBusMutex && (xSemaphoreTake(g_spiBusMutex, timeoutTicks) == pdTRUE);
}
static void spiBusUnlock() {
  if (g_spiBusMutex) xSemaphoreGive(g_spiBusMutex);
}
static inline void adsCS(bool en) { digitalWrite(PIN_ADS_CS, en ? LOW : HIGH); }
static void restoreAdsBusAfterSd() {
  digitalWrite(PIN_SD_CS, HIGH);
  adsCS(false);
  SPI.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI, PIN_SD_CS);
  if (cfg.dmsEnabled) {
    restoreDmsRunMode();
  }
}
static inline bool adsBusPrepare() {
  if (!spiBusLock(pdMS_TO_TICKS(2))) return false;
  digitalWrite(PIN_SD_CS, HIGH);
  SPI.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI, PIN_ADS_CS);
  SPI.beginTransaction(g_adsSpiSettings);
  return true;
}
static inline void adsBusDone() {
  SPI.endTransaction();
  spiBusUnlock();
}
static inline void sdBusPrepare() {
  adsCS(false);
  SPI.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI, PIN_SD_CS);
}
static uint8_t adsXfer(uint8_t b) {
  return SPI.transfer(b);
}
static inline bool isApModeRequested() {
  pinMode(PIN_AP_MODE, INPUT_PULLUP);
  delay(2);
  return digitalRead(PIN_AP_MODE) == LOW;
}
static float rawTo_mV_gain(int32_t raw, float gain) {
  float v = ((float)raw * ADS_VREF_VOLTS) / (gain * (float)ADS_FS);
  return v * 1000.0f;
}
static float dmsTo_mV_per_V(float dms_mV_local) {
  return dms_mV_local / ADS_VREF_VOLTS;
}
static float clampf(float v, float lo, float hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}
static const char* ain2ModeName() {
  return (cfg.ain2Mode == AIN2_MODE_VOLT) ? "volt" : "pot";
}
static const char* ain2ModeNameFor(uint8_t mode) {
  return (mode == AIN2_MODE_VOLT) ? "volt" : "pot";
}
static const char* ain2DerivedUnitFor(uint8_t mode) {
  return (mode == AIN2_MODE_VOLT) ? "V" : "mm";
}
static uint8_t hexNibble(char c) {
  if (c >= '0' && c <= '9') return (uint8_t)(c - '0');
  if (c >= 'A' && c <= 'F') return (uint8_t)(10 + c - 'A');
  if (c >= 'a' && c <= 'f') return (uint8_t)(10 + c - 'a');
  return 0;
}
static bool parseHexToBytes(const char* hex, uint8_t* out, size_t outLen) {
  size_t hexLen = strlen(hex);
  if (hexLen != outLen * 2) return false;
  for (size_t i = 0; i < outLen; ++i) {
    out[i] = (uint8_t)((hexNibble(hex[i * 2]) << 4) | hexNibble(hex[i * 2 + 1]));
  }
  return true;
}
static void bytesToHex(const uint8_t* in, size_t len, char* out) {
  static const char* lut = "0123456789ABCDEF";
  for (size_t i = 0; i < len; ++i) {
    out[i * 2] = lut[(in[i] >> 4) & 0x0F];
    out[i * 2 + 1] = lut[in[i] & 0x0F];
  }
  out[len * 2] = '\0';
}
static void copyUpperNoSpace(char* dst, size_t dstLen, const char* src) {
  if (dstLen == 0) return;
  size_t j = 0;
  for (size_t i = 0; src && src[i] != '\0'; ++i) {
    unsigned char c = (unsigned char)src[i];
    if (isspace(c)) continue;
    if (j + 1 < dstLen) dst[j++] = (char)toupper(c);
  }
  dst[j] = '\0';
}
static void sanitizeFileStem(char* dst, size_t dstLen, const char* src) {
  if (dstLen == 0) return;
  size_t j = 0;
  for (size_t i = 0; src && src[i] != '\0'; ++i) {
    char c = src[i];
    bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
    if (!ok) continue;
    if (j + 1 < dstLen) dst[j++] = c;
  }
  if (j == 0) {
    strlcpy(dst, "lorasense", dstLen);
    return;
  }
  dst[j] = '\0';
}
static void sanitizeLowerToken(char* dst, size_t dstLen, const char* src, const char* fallback) {
  if (dstLen == 0) return;
  size_t j = 0;
  for (size_t i = 0; src && src[i] != '\0'; ++i) {
    unsigned char c = (unsigned char)src[i];
    if (isspace(c)) continue;
    if (j + 1 < dstLen) dst[j++] = (char)tolower(c);
  }
  dst[j] = '\0';
  if (j == 0 && fallback) strlcpy(dst, fallback, dstLen);
}
static bool strEqIgnoreCase(const char* a, const char* b) {
  if (!a || !b) return false;
  while (*a && *b) {
    if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return false;
    ++a;
    ++b;
  }
  return *a == '\0' && *b == '\0';
}
static bool isExactHex(const char* s, size_t len) {
  if (!s || strlen(s) != len) return false;
  for (size_t i = 0; i < len; ++i) {
    unsigned char c = (unsigned char)s[i];
    if (!isxdigit(c)) return false;
  }
  return true;
}
static float hzToPeriodMsFloat(float hz) {
  if (hz <= 0.0001f) return 1000000.0f;
  return 1000.0f / hz;
}
static uint32_t hzToPeriodMsClamped(float hz, uint32_t minMs, uint32_t maxMs) {
  float ms = hzToPeriodMsFloat(hz);
  if (ms < (float)minMs) ms = (float)minMs;
  if (ms > (float)maxMs) ms = (float)maxMs;
  return (uint32_t)(ms + 0.5f);
}
static float hzToPeriodUsFloat(float hz) {
  if (hz <= 0.0001f) return 1000000.0f;
  return 1000000.0f / hz;
}
static uint32_t hzToPeriodUsClamped(float hz, uint32_t minUs, uint32_t maxUs) {
  float us = hzToPeriodUsFloat(hz);
  if (us < (float)minUs) us = (float)minUs;
  if (us > (float)maxUs) us = (float)maxUs;
  return (uint32_t)(us + 0.5f);
}
static float periodMsToHz(uint32_t ms) {
  if (ms == 0) return 0.0f;
  return 1000.0f / (float)ms;
}
static float periodUsToHz(uint32_t us) {
  if (us == 0) return 0.0f;
  return 1000000.0f / (float)us;
}
static void syncLoraCompatAliases() {
  memcpy(DevEui, devEui, sizeof(devEui));
  memcpy(AppEui, appEui, sizeof(appEui));
  memcpy(AppKey, appKey, sizeof(appKey));
  memcpy(NwkSKey, nwkSKey, sizeof(nwkSKey));
  memcpy(AppSKey, appSKey, sizeof(appSKey));
  DevAddr = devAddr;
}
static PerfEstimate makePerfEstimate() {
  PerfEstimate p{};
  p.dmsTargetHz = cfg.dmsEnabled ? cfg.dmsHz : 0.0f;
  p.ain2TargetHz = cfg.ain2Enabled ? cfg.ain2Hz : 0.0f;
  p.tempTargetHz = cfg.tempEnabled ? TEMP_FIXED_HZ : 0.0f;
  float ain2Conv = 0.0f;
  if (cfg.ain2Enabled) ain2Conv = cfg.dmsEnabled ? 3.0f : 2.0f; // discard + read + optional DMS restore
  p.adsRequestedConvHz = p.dmsTargetHz + p.ain2TargetHz * ain2Conv;
  p.scale = 1.0f;
  if (p.adsRequestedConvHz > ADS_BUDGET_CONVERSIONS_HZ && p.adsRequestedConvHz > 0.0001f) {
    p.scale = ADS_BUDGET_CONVERSIONS_HZ / p.adsRequestedConvHz;
  }
  p.adsLoadPct = clampf((p.adsRequestedConvHz / ADS_BUDGET_CONVERSIONS_HZ) * 100.0f, 0.0f, 999.0f);
  p.dmsEstimatedHz = p.dmsTargetHz * p.scale;
  p.ain2EstimatedHz = p.ain2TargetHz * p.scale;
  p.totalEstimatedHz = p.dmsEstimatedHz + p.ain2EstimatedHz + p.tempTargetHz;
  return p;
}
static void getChipMac48(uint8_t out[6]) {
  uint64_t mac = ESP.getEfuseMac();
  for (int i = 0; i < 6; ++i) out[5 - i] = (uint8_t)((mac >> (i * 8)) & 0xFF);
}
static void makeChipDevEui(uint8_t out[8]) {
  uint8_t mac[6];
  getChipMac48(mac);
  out[0] = mac[0] ^ 0x02;
  out[1] = mac[1];
  out[2] = mac[2];
  out[3] = 0xFF;
  out[4] = 0xFE;
  out[5] = mac[3];
  out[6] = mac[4];
  out[7] = mac[5];
}
static void chipMac48Hex(char out[13]) {
  uint8_t mac[6];
  getChipMac48(mac);
  bytesToHex(mac, 6, out);
}
static String modeToString();
static bool oledAllowedInCurrentMode() { return g_oledSupported && g_mode == RunMode::CONFIG; }
static bool oledIsActiveNow() { return oledAllowedInCurrentMode() && g_oledEnabled; }
static float ain2DerivedValue();
static const char* ain2DerivedUnit();
static void chipDevEuiHex(char out[17]) {
  uint8_t eui[8];
  makeChipDevEui(eui);
  bytesToHex(eui, 8, out);
}
static void VextON(void) {
  pinMode(Vext, OUTPUT);
  digitalWrite(Vext, LOW);
}
static void VextOFF(void) {
  pinMode(Vext, OUTPUT);
  digitalWrite(Vext, HIGH);
}
static bool oledEnsureInit() {
  if (!oledAllowedInCurrentMode() || !g_oledEnabled) return false;
  if (g_oledInitDone) return true;
  VextON();
  delay(50);
  Wire.begin(SDA_OLED, SCL_OLED);
  OLED_Display.init();
  OLED_Display.clear();
  OLED_Display.display();
  g_oledInitDone = true;
  return true;
}
static void oledSleep() {
  if (!g_oledSupported) return;
  if (g_oledInitDone) {
    OLED_Display.clear();
    OLED_Display.display();
  }
  VextOFF();
  g_oledInitDone = false;
}
static void oledShowBootLogo() {
  if (!oledEnsureInit()) return;
  OLED_Display.clear();
  OLED_Display.drawXbm(0, 10, logoMKP_width, logoMKP_height, (const unsigned char*)logoMKP_bits);
  OLED_Display.setFont(DejaVu_Serif_8);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 50, "MARX KRONTAL PARTNER");
  OLED_Display.display();
}
static void oledShowBanner(const String& l1, const String& l2) {
  if (!oledEnsureInit()) return;
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 2, l1);
  OLED_Display.drawString(0, 20, l2);
  OLED_Display.display();
}
static String oledAin2String() {
  if (!cfg.ain2Enabled) return String("OFF");
  if (!ain2_valid) return String("--");
  return String(ain2DerivedValue(), 2) + " " + String(ain2DerivedUnit());
}
static String oledTempString() {
  if (!cfg.tempEnabled) return String("OFF");
  if (!temp_valid) return String("--");
  return String(ds_temp_c, 2) + " C";
}
static String oledDmsString() {
  if (!cfg.dmsEnabled) return String("OFF");
  if (!dms_valid) return String("--");
  return String(dms_mV_per_V, 3) + " mV/V";
}
static void oledUpdateLive(bool force = false) {
  if (!oledIsActiveNow()) return;
  uint32_t now = millis();
  if (!force && (uint32_t)(now - g_oledLastDrawMs) < 250UL) return;
  if (!oledEnsureInit()) return;
  OLED_Display.clear();
  OLED_Display.setFont(ArialMT_Plain_10);
  OLED_Display.setTextAlignment(TEXT_ALIGN_CENTER);
  OLED_Display.drawString(64, 0, String("LoRaSense ") + modeToString());
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 12, "DMS");
  OLED_Display.setTextAlignment(TEXT_ALIGN_RIGHT);
  OLED_Display.drawString(128, 12, oledDmsString());
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 24, "AIN2");
  OLED_Display.setTextAlignment(TEXT_ALIGN_RIGHT);
  OLED_Display.drawString(128, 24, oledAin2String());
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 36, "Temp");
  OLED_Display.setTextAlignment(TEXT_ALIGN_RIGHT);
  OLED_Display.drawString(128, 36, oledTempString());
  OLED_Display.setTextAlignment(TEXT_ALIGN_LEFT);
  OLED_Display.drawString(0, 48, "AP");
  OLED_Display.setTextAlignment(TEXT_ALIGN_RIGHT);
  OLED_Display.drawString(128, 48, (g_mode == RunMode::CONFIG) ? g_apIp.toString() : String("FIELD"));
  OLED_Display.display();
  g_oledLastDrawMs = now;
}
static void normalizeCfg(Cfg& c) {
  copyUpperNoSpace(c.devEui, sizeof(c.devEui), c.devEui);
  copyUpperNoSpace(c.appEui, sizeof(c.appEui), c.appEui);
  copyUpperNoSpace(c.appKey, sizeof(c.appKey), c.appKey);
  sanitizeFileStem(c.logBaseName, sizeof(c.logBaseName), c.logBaseName);
  if (c.intervalMin < 1) c.intervalMin = 1;
  if (c.intervalMin > 1440) c.intervalMin = 1440;
  if (c.dr > 5) c.dr = 5;
  c.dmsHz = clampf(c.dmsHz, DMS_MIN_HZ, DMS_MAX_HZ);
  if (!(c.dmsKFactor >= 0.0f)) c.dmsKFactor = 0.0f;
  c.ain2Hz = clampf(c.ain2Hz, AIN2_MIN_HZ, AIN2_MAX_HZ);
  if (c.ain2Mode != AIN2_MODE_POT && c.ain2Mode != AIN2_MODE_VOLT) c.ain2Mode = AIN2_MODE_POT;
  if (!(c.ain2LengthMm > 0.0f)) c.ain2LengthMm = 10.0f;
  if (!(c.ain2AdcFullscaleV > 0.05f)) c.ain2AdcFullscaleV = 3.123f;
  if (!(c.ain2InputFullscaleV > 0.05f)) c.ain2InputFullscaleV = 10.0f;
  sanitizeLowerToken(c.ain2VoltDisplay, sizeof(c.ain2VoltDisplay), c.ain2VoltDisplay, "volts");
  if (!strEqIgnoreCase(c.ain2VoltDisplay, "volts") && !strEqIgnoreCase(c.ain2VoltDisplay, "scaled")) {
    strlcpy(c.ain2VoltDisplay, "volts", sizeof(c.ain2VoltDisplay));
  }
  sanitizeLowerToken(c.ain2ScalingUnit, sizeof(c.ain2ScalingUnit), c.ain2ScalingUnit, "mm");
  if (strlen(c.ain2ScalingUnit) == 0) strlcpy(c.ain2ScalingUnit, "mm", sizeof(c.ain2ScalingUnit));
  if (!isfinite(c.ain2ScaleMin)) c.ain2ScaleMin = 0.0f;
  if (!isfinite(c.ain2ScaleMax)) c.ain2ScaleMax = 10.0f;
  if (c.logRotateKB < 64) c.logRotateKB = 64;
  if (c.logRotateKB > 1024UL * 1024UL) c.logRotateKB = 1024UL * 1024UL;
  if (c.streamModeEnabled) c.sdLogEnabled = false;
}
static bool validateCfg(const Cfg& c, String* errOut = nullptr) {
  if (!isExactHex(c.devEui, DEV_EUI_HEX_LEN)) {
    if (errOut) *errOut = "invalid DevEUI (need 16 hex chars)";
    return false;
  }
  if (!isExactHex(c.appEui, APP_EUI_HEX_LEN)) {
    if (errOut) *errOut = "invalid AppEUI (need 16 hex chars)";
    return false;
  }
  if (!isExactHex(c.appKey, APP_KEY_HEX_LEN)) {
    if (errOut) *errOut = "invalid AppKey (need 32 hex chars)";
    return false;
  }
  return true;
}
static void printHexLine(const char* label, const uint8_t* data, size_t len) {
  char buf[65];
  if (len * 2 >= sizeof(buf)) return;
  bytesToHex(data, len, buf);
  Serial.print(label);
  Serial.println(buf);
}
static void dumpLoraBuffers(const char* tag) {
  Serial.print("[LORA] ");
  Serial.println(tag);
  printHexLine("  devEui=", devEui, sizeof(devEui));
  printHexLine("  appEui=", appEui, sizeof(appEui));
  printHexLine("  appKey=", appKey, sizeof(appKey));
}
static void loadCfg() {
  Cfg defaults = cfg;
  prefs.begin("lorasense", true);
  String dE = prefs.getString("devEui", defaults.devEui);
  String aE = prefs.getString("appEui", defaults.appEui);
  String aK = prefs.getString("appKey", defaults.appKey);
  cfg.intervalMin = prefs.getUShort("minutes", defaults.intervalMin);
  cfg.adr = prefs.getBool("adr", defaults.adr);
  cfg.dr = prefs.getUChar("dr", defaults.dr);
  uint32_t oldDmsPerUs = prefs.getUInt("dmsperus", 0);
  uint32_t oldAin2PerUs = prefs.getUInt("aperus", 0);
  uint32_t oldDmsPer = prefs.getUInt("dmsper", hzToPeriodMsClamped(defaults.dmsHz, 1, 600000UL));
  uint32_t oldAin2Per = prefs.getUInt("aper", hzToPeriodMsClamped(defaults.ain2Hz, 1, 600000UL));
  cfg.dmsEnabled = prefs.getBool("dmson", defaults.dmsEnabled);
  cfg.dmsHz = prefs.getFloat("dmsHz", oldDmsPerUs ? periodUsToHz(oldDmsPerUs) : periodMsToHz(oldDmsPer));
  cfg.dmsKFactor = prefs.getFloat("dmsKFac", defaults.dmsKFactor);
  cfg.ain2Enabled = prefs.getBool("a2on", defaults.ain2Enabled);
  cfg.ain2Hz = prefs.getFloat("a2Hz", oldAin2PerUs ? periodUsToHz(oldAin2PerUs) : periodMsToHz(oldAin2Per));
  cfg.ain2Mode = prefs.getUChar("a2mode", defaults.ain2Mode);
  cfg.ain2LengthMm = prefs.getFloat("a2len", defaults.ain2LengthMm);
  cfg.ain2AdcFullscaleV = prefs.getFloat("a2adcfs", defaults.ain2AdcFullscaleV);
  cfg.ain2InputFullscaleV = prefs.getFloat("a2infs", defaults.ain2InputFullscaleV);
  String a2vdisp = prefs.getString("a2vdisp", defaults.ain2VoltDisplay);
  String a2unit = prefs.getString("a2unit", defaults.ain2ScalingUnit);
  cfg.ain2ScaleMin = prefs.getFloat("a2smin", defaults.ain2ScaleMin);
  cfg.ain2ScaleMax = prefs.getFloat("a2smax", defaults.ain2ScaleMax);
  cfg.tempEnabled = prefs.getBool("tmpon", defaults.tempEnabled);
  cfg.sdLogEnabled = prefs.getBool("sdon", defaults.sdLogEnabled);
  cfg.streamModeEnabled = prefs.getBool("streamon", defaults.streamModeEnabled);
  String base = prefs.getString("sdbase", defaults.logBaseName);
  cfg.logRotateKB = prefs.getUInt("sdkb", defaults.logRotateKB);
  g_oledEnabled = prefs.getBool("oledon", true);
  prefs.end();
  strlcpy(cfg.devEui, dE.c_str(), sizeof(cfg.devEui));
  strlcpy(cfg.appEui, aE.c_str(), sizeof(cfg.appEui));
  strlcpy(cfg.appKey, aK.c_str(), sizeof(cfg.appKey));
  strlcpy(cfg.ain2VoltDisplay, a2vdisp.c_str(), sizeof(cfg.ain2VoltDisplay));
  strlcpy(cfg.ain2ScalingUnit, a2unit.c_str(), sizeof(cfg.ain2ScalingUnit));
  strlcpy(cfg.logBaseName, base.c_str(), sizeof(cfg.logBaseName));
  normalizeCfg(cfg);
  String err;
  if (!validateCfg(cfg, &err)) {
    Serial.print("[CFG] invalid stored config, using defaults: ");
    Serial.println(err);
    cfg = defaults;
    normalizeCfg(cfg);
  }
}
static void saveCfg() {
  normalizeCfg(cfg);
  prefs.begin("lorasense", false);
  prefs.putString("devEui", cfg.devEui);
  prefs.putString("appEui", cfg.appEui);
  prefs.putString("appKey", cfg.appKey);
  prefs.putUShort("minutes", cfg.intervalMin);
  prefs.putBool("adr", cfg.adr);
  prefs.putUChar("dr", cfg.dr);
  prefs.putBool("dmson", cfg.dmsEnabled);
  prefs.putFloat("dmsHz", cfg.dmsHz);
  prefs.putFloat("dmsKFac", cfg.dmsKFactor);
  prefs.putUInt("dmsper", hzToPeriodMsClamped(cfg.dmsHz, 1, 600000UL));
  prefs.putUInt("dmsperus", hzToPeriodUsClamped(cfg.dmsHz, 500, 600000000UL));
  prefs.putBool("a2on", cfg.ain2Enabled);
  prefs.putFloat("a2Hz", cfg.ain2Hz);
  prefs.putUInt("aper", hzToPeriodMsClamped(cfg.ain2Hz, 1, 600000UL));
  prefs.putUInt("aperus", hzToPeriodUsClamped(cfg.ain2Hz, 500, 600000000UL));
  prefs.putUChar("a2mode", cfg.ain2Mode);
  prefs.putFloat("a2len", cfg.ain2LengthMm);
  prefs.putFloat("a2adcfs", cfg.ain2AdcFullscaleV);
  prefs.putFloat("a2infs", cfg.ain2InputFullscaleV);
  prefs.putString("a2vdisp", cfg.ain2VoltDisplay);
  prefs.putString("a2unit", cfg.ain2ScalingUnit);
  prefs.putFloat("a2smin", cfg.ain2ScaleMin);
  prefs.putFloat("a2smax", cfg.ain2ScaleMax);
  prefs.putBool("tmpon", cfg.tempEnabled);
  prefs.putBool("sdon", cfg.sdLogEnabled);
  prefs.putBool("streamon", cfg.streamModeEnabled);
  prefs.putString("sdbase", cfg.logBaseName);
  prefs.putUInt("sdkb", cfg.logRotateKB);
  prefs.putBool("oledon", g_oledEnabled);
  prefs.end();
}
static void applyLoraConfig() {
  memset(devEui, 0, sizeof(devEui));
  memset(appEui, 0, sizeof(appEui));
  memset(appKey, 0, sizeof(appKey));
  memset(nwkSKey, 0, sizeof(nwkSKey));
  memset(appSKey, 0, sizeof(appSKey));
  devAddr = 0;
  parseHexToBytes(cfg.devEui, devEui, sizeof(devEui));
  parseHexToBytes(cfg.appEui, appEui, sizeof(appEui));
  parseHexToBytes(cfg.appKey, appKey, sizeof(appKey));
  syncLoraCompatAliases();
  appTxDutyCycle = (uint32_t)cfg.intervalMin * 60UL * 1000UL;
  loraWanAdr = cfg.adr;
}
static float ain2DerivedValue() {
  if (!ain2_valid || isnan(ain2_mV)) return NAN;
  if (cfg.ain2Mode == AIN2_MODE_VOLT) {
    if (cfg.ain2AdcFullscaleV <= 0.001f) return NAN;
    float v = (ain2_mV / 1000.0f) * (cfg.ain2InputFullscaleV / cfg.ain2AdcFullscaleV);
    return clampf(v, 0.0f, cfg.ain2InputFullscaleV);
  }
  if (cfg.ain2AdcFullscaleV <= 0.001f) return NAN;
  float ratio = (ain2_mV / 1000.0f) / cfg.ain2AdcFullscaleV;
  return clampf(ratio, 0.0f, 1.0f) * cfg.ain2LengthMm;
}
static const char* ain2DerivedUnit() {
  return (cfg.ain2Mode == AIN2_MODE_VOLT) ? "V" : "mm";
}
static uint8_t adsBuildReg1(uint8_t drBits, uint8_t modeBits) {
  return (uint8_t)(((drBits & 0x07u) << 5) | ((modeBits & 0x03u) << 3));
}
static uint32_t adsTimeoutUsForSps(float sps) {
  if (!(sps > 0.0f)) return ADS_MAX_TIMEOUT_US;
  float us = (1000000.0f / sps) * 1.6f + 3000.0f;
  if (us < 5000.0f) us = 5000.0f;
  if (us > (float)ADS_MAX_TIMEOUT_US) us = (float)ADS_MAX_TIMEOUT_US;
  return (uint32_t)us;
}
static AdsChanCfg makeAdsChanCfg(uint8_t reg0, float gain, float requestedHz) {
  struct RateDef { float sps; uint8_t dr; uint8_t mode; uint8_t reg2; };
  static const RateDef kRates[] = {
    {5.0f,    0, 1, ADS_REG2_EXTREF_50_60},
    {11.25f,  1, 1, ADS_REG2_EXTREF_BASE},
    {20.0f,   0, 0, ADS_REG2_EXTREF_50_60},
    {22.5f,   2, 1, ADS_REG2_EXTREF_BASE},
    {40.0f,   0, 2, ADS_REG2_EXTREF_BASE},
    {44.0f,   3, 1, ADS_REG2_EXTREF_BASE},
    {45.0f,   1, 0, ADS_REG2_EXTREF_BASE},
    {82.5f,   4, 1, ADS_REG2_EXTREF_BASE},
    {90.0f,   2, 0, ADS_REG2_EXTREF_BASE},
    {150.0f,  5, 1, ADS_REG2_EXTREF_BASE},
    {175.0f,  3, 0, ADS_REG2_EXTREF_BASE},
    {180.0f,  2, 2, ADS_REG2_EXTREF_BASE},
    {250.0f,  6, 1, ADS_REG2_EXTREF_BASE},
    {330.0f,  4, 0, ADS_REG2_EXTREF_BASE},
    {350.0f,  3, 2, ADS_REG2_EXTREF_BASE},
    {600.0f,  5, 0, ADS_REG2_EXTREF_BASE},
    {660.0f,  4, 2, ADS_REG2_EXTREF_BASE},
    {1000.0f, 6, 0, ADS_REG2_EXTREF_BASE},
    {1200.0f, 5, 2, ADS_REG2_EXTREF_BASE},
    {2000.0f, 6, 2, ADS_REG2_EXTREF_BASE},
  };
  const RateDef* best = &kRates[0];
  float bestDiff = fabsf(requestedHz - kRates[0].sps);
  for (size_t i = 1; i < sizeof(kRates)/sizeof(kRates[0]); ++i) {
    float diff = fabsf(requestedHz - kRates[i].sps);
    if (diff < bestDiff) {
      best = &kRates[i];
      bestDiff = diff;
    }
  }
  AdsChanCfg cfg{};
  cfg.reg0 = reg0;
  cfg.reg1 = adsBuildReg1(best->dr, best->mode);
  cfg.reg2 = best->reg2;
  cfg.gain = gain;
  cfg.sps = best->sps;
  cfg.timeoutUs = adsTimeoutUsForSps(best->sps);
  return cfg;
}
static void refreshAdsChannelConfigs() {
  g_adsDmsCfg = makeAdsChanCfg(REG0_DMS, GAIN_DMS, cfg.dmsHz);
  g_adsAin2Cfg = makeAdsChanCfg(REG0_AIN2, GAIN_AIN2, cfg.ain2Hz);
}
static void invalidateDms() {
  // Keep the last raw ADC code so exported BIN/CSV does not create artificial
  // drops to zero when a single ADS read times out or is temporarily invalid.
  // Validity is still tracked via dms_valid / record flags.
  dms_valid = false;
  dms_mV = NAN;
  dms_mV_per_V = NAN;
  g_dmsFailStreak = 0;
  g_dmsLastOkUs = 0;
}
static void invalidateAin2() {
  // Keep the last raw ADC code for continuity in exported trend data.
  ain2_valid = false;
  ain2_mV = NAN;
  g_ain2FailStreak = 0;
  g_ain2LastOkUs = 0;
}
static void invalidateTemp() {
  temp_valid = false;
  ds_temp_c = NAN;
  ds_pending = false;
  g_tempFailStreak = 0;
  g_tempLastOkMs = 0;
}
static bool applyDownSpikeFilter(int32_t candidate, int32_t& lastRaw, bool& haveLast,
                                 int32_t dropThresholdRaw, float dropRatioMin) {
  if (!haveLast) {
    lastRaw = candidate;
    haveLast = true;
    return true;
  }
  const int32_t prev = lastRaw;
  const int32_t drop = prev - candidate;
  if (drop > dropThresholdRaw && candidate < (int32_t)(prev * dropRatioMin)) {
    // Reject one-sample downward glitches that create artificial vertical drops
    // in the exported trend data. Keep the previous accepted raw value.
    return false;
  }
  lastRaw = candidate;
  return true;
}
static void prepareTxFrame(uint8_t port);
static void syncSdLogger(bool forceReopen);
static bool startMeasurementsManual();
static void stopMeasurementsManual();
static void ensureMeasurementTasksStarted();
// ============================================================
// ADS low-level
// ============================================================
static void adsCommand(uint8_t cmd) {
  if (!adsBusPrepare()) return;
  adsCS(true);
  adsXfer(cmd);
  adsCS(false);
  adsBusDone();
}
static void adsWriteRegs4(uint8_t r0, uint8_t r1, uint8_t r2, uint8_t r3) {
  if (!adsBusPrepare()) return;
  adsCS(true);
  adsXfer(ADS1220::CMD_WREG | 0x03);
  adsXfer(r0);
  adsXfer(r1);
  adsXfer(r2);
  adsXfer(r3);
  adsCS(false);
  adsBusDone();
  g_adsCurReg0 = r0;
  g_adsCurReg1 = r1;
  g_adsCurReg2 = r2;
  g_adsCurReg3 = r3;
  g_adsConfigSettled = false;
}
static void adsApplyConfig(const AdsChanCfg& c) {
  if (g_adsCurReg0 == c.reg0 && g_adsCurReg1 == c.reg1 && g_adsCurReg2 == c.reg2 && g_adsCurReg3 == ADS_REG3) return;
  adsWriteRegs4(c.reg0, c.reg1, c.reg2, ADS_REG3);
}
static int32_t adsReadRaw_RDATA() {
  if (!adsBusPrepare()) return ADS_FAIL;
  adsCS(true);
  adsXfer(ADS1220::CMD_RDATA);
  uint8_t b2 = adsXfer(0xFF);
  uint8_t b1 = adsXfer(0xFF);
  uint8_t b0 = adsXfer(0xFF);
  adsCS(false);
  adsBusDone();
  int32_t raw = ((int32_t)b2 << 16) | ((int32_t)b1 << 8) | b0;
  if (raw & 0x800000) raw |= 0xFF000000;
  return raw;
}
static bool adsWaitDrdyLow(uint32_t timeout_us) {
  uint32_t start = micros();
  uint32_t spins = 0;
  while (digitalRead(PIN_ADS_DRDY) == HIGH) {
    if ((uint32_t)(micros() - start) >= timeout_us) return false;
    // Let the scheduler breathe often enough to avoid task watchdog resets,
    // but keep the wait mostly tight for high-speed sampling.
    if ((++spins & 0x3Fu) == 0u) {
      taskYIELD();
    }
  }
  return true;
}
static int32_t readSingleShot_DRDY(uint32_t timeoutUs) {
  for (int attempt = 0; attempt <= RETRY_MAX; ++attempt) {
    adsCommand(ADS1220::CMD_START);
    if (!adsWaitDrdyLow(timeoutUs)) {
      delayMicroseconds(100);
      continue;
    }
    return adsReadRaw_RDATA();
  }
  return ADS_FAIL;
}
static bool readChannelRaw(const AdsChanCfg& chan, float& out_mV, int32_t* rawOut) {
  adsApplyConfig(chan);
  if (!g_adsConfigSettled) {
    if (readSingleShot_DRDY(chan.timeoutUs) == ADS_FAIL) {
      out_mV = NAN;
      return false;
    }
    g_adsConfigSettled = true;
  }
  int32_t raw = readSingleShot_DRDY(chan.timeoutUs);
  if (raw == ADS_FAIL) {
    out_mV = NAN;
    return false;
  }
  if (rawOut) *rawOut = raw;
  out_mV = rawTo_mV_gain(raw, chan.gain);
  return true;
}
static bool readDmsFast(float& out_mV, int32_t* rawOut = nullptr) {
  return readChannelRaw(g_adsDmsCfg, out_mV, rawOut);
}
static void adsInit() {
  refreshAdsChannelConfigs();
  pinMode(PIN_SD_CS, OUTPUT);
  digitalWrite(PIN_SD_CS, HIGH);
  SPI.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI, PIN_ADS_CS);
  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);
  pinMode(PIN_ADS_DRDY, INPUT);
  pinMode(PIN_MOSFET, OUTPUT);
  digitalWrite(PIN_MOSFET, LOW);
  adsCommand(ADS1220::CMD_RESET);
  delay(5);
  adsWriteRegs4(g_adsDmsCfg.reg0, g_adsDmsCfg.reg1, g_adsDmsCfg.reg2, ADS_REG3);
}
static void restoreDmsRunMode() {
  adsApplyConfig(g_adsDmsCfg);
}
static bool runSelfTest() {
  Serial.println("# ===== SELFTEST START =====");
  if (!cfg.dmsEnabled) {
    Serial.println("# SELFTEST skipped: DMS disabled");
    selfTestOk = false;
    selfTestDelta_mV = NAN;
    return false;
  }
  adsApplyConfig(g_adsDmsCfg);
  digitalWrite(PIN_MOSFET, LOW);
  float base5[5];
  float on5[5];
  auto avg5 = [](const float a[5]) -> float {
    double s = 0.0;
    for (int i = 0; i < 5; ++i) s += a[i];
    return (float)(s / 5.0);
  };
  auto collectSamples = [](float* dst, int count) -> bool {
    const int maxAttempts = count * 4;
    int ok = 0;
    for (int attempt = 0; attempt < maxAttempts && ok < count; ++attempt) {
      float mv = NAN;
      int32_t raw = 0;
      if (!readChannelRaw(g_adsDmsCfg, mv, &raw)) continue;
      dst[ok++] = mv;
    }
    return ok == count;
  };
  bool haveBase = collectSamples(base5, 5);
  digitalWrite(PIN_MOSFET, HIGH);
  delay(50);
  bool haveOn = collectSamples(on5, 5);
  digitalWrite(PIN_MOSFET, LOW);
  if (!haveBase || !haveOn) {
    Serial.println("# SELFTEST failed: ADS timeout / no valid samples");
    selfTestOk = false;
    selfTestDelta_mV = NAN;
    restoreDmsRunMode();
    return false;
  }
  float bAvg = avg5(base5);
  float mAvg = avg5(on5);
  selfTestDelta_mV = mAvg - bAvg;
  selfTestOk = (fabs(fabs(selfTestDelta_mV) - EXPECTED_MV) <= TOLERANCE_MV);
  Serial.print("# SELFTEST delta=");
  Serial.print(selfTestDelta_mV, 6);
  Serial.print(" mV  result=");
  Serial.println(selfTestOk ? "OK" : "FAIL");
  restoreDmsRunMode();
  return selfTestOk;
}
// ============================================================
// Sensor runtime
// ============================================================
static void dsInit() {
  dsSensor.begin();
  dsSensor.setWaitForConversion(false);
  uint8_t devCount = dsSensor.getDeviceCount();
  for (uint8_t i = 0; i < devCount; ++i) {
    DeviceAddress addr;
    if (dsSensor.getAddress(addr, i)) dsSensor.setResolution(addr, 9);
  }
  ds_pending = false;
  ds_request_ms = 0;
  last_ds_request_loop_ms = 0;
}
static bool readTempBlocking(float& outTempC) {
  dsSensor.requestTemperatures();
  delay(120);
  float tmp = dsSensor.getTempCByIndex(0);
  if (tmp == DEVICE_DISCONNECTED_C) return false;
  outTempC = tmp;
  return true;
}
static void sensorLoopAp() {
  static uint32_t dmsCount = 0;
  static uint32_t ain2Count = 0;
  static uint32_t tempCount = 0;
  static uint32_t totalCount = 0;
  static uint32_t lastFrameReportMs = 0;
  uint32_t now = millis();
  uint32_t nowUs = micros();
  bool radioQuiet = g_loraEnabled && ((int32_t)(now - g_radioQuietUntilMs) < 0);
  bool sampleChanged = false;
  if (!cfg.dmsEnabled) invalidateDms();
  if (!cfg.ain2Enabled) invalidateAin2();
  if (!cfg.tempEnabled) invalidateTemp();
  // ---------------- DMS + AIN2 ----------------
  if (!radioQuiet) {
    if (cfg.dmsEnabled) {
      uint32_t dmsPeriodUs = hzToPeriodUsClamped(cfg.dmsHz, 500, 600000000UL);
      if ((uint32_t)(nowUs - last_dms_read_us) >= dmsPeriodUs) {
        int32_t rawDms = lastDmsRaw;
        if (readDmsFast(dms_mV, &rawDms)) {
          if (!applyDownSpikeFilter(rawDms, lastDmsRaw, lastDmsRawSeen, 80, 0.75f)) {
            dms_mV = rawTo_mV_gain(lastDmsRaw, g_adsDmsCfg.gain);
          }
          dms_mV_per_V = dmsTo_mV_per_V(dms_mV);
          dms_valid = true;
          g_dmsFailStreak = 0;
          g_dmsLastOkUs = nowUs;
          dmsCount++;
          totalCount++;
          sampleChanged = true;
        } else {
          if (g_dmsFailStreak < 255) g_dmsFailStreak++;
          if (g_dmsFailStreak >= ADC_FAILS_BEFORE_INVALID &&
              (g_dmsLastOkUs == 0 || (uint32_t)(nowUs - g_dmsLastOkUs) >= ADC_HOLD_TIMEOUT_US)) {
            invalidateDms();
          }
        }
        last_dms_read_us += dmsPeriodUs; if ((uint32_t)(nowUs - last_dms_read_us) > dmsPeriodUs) last_dms_read_us = nowUs;
      }
    }
    if (cfg.ain2Enabled) {
      uint32_t ain2PeriodUs = hzToPeriodUsClamped(cfg.ain2Hz, 500, 600000000UL);
      if ((uint32_t)(nowUs - last_ain2_read_us) >= ain2PeriodUs) {
        float tmpAin2 = NAN;
        int32_t rawAin2 = lastAin2Raw;
        if (readChannelRaw(g_adsAin2Cfg, tmpAin2, &rawAin2)) {
          lastAin2Raw = rawAin2;
          lastAin2RawSeen = true;
          ain2_mV = tmpAin2;
          ain2_valid = true;
          g_ain2FailStreak = 0;
          g_ain2LastOkUs = nowUs;
          ain2Count++;
          totalCount++;
          sampleChanged = true;
        } else {
          if (g_ain2FailStreak < 255) g_ain2FailStreak++;
          if (g_ain2FailStreak >= ADC_FAILS_BEFORE_INVALID &&
              (g_ain2LastOkUs == 0 || (uint32_t)(nowUs - g_ain2LastOkUs) >= ADC_HOLD_TIMEOUT_US)) {
            invalidateAin2();
          }
        }
        last_ain2_read_us += ain2PeriodUs; if ((uint32_t)(nowUs - last_ain2_read_us) > ain2PeriodUs) last_ain2_read_us = nowUs;
      }
    }
  }
  // ---------------- Temperature ----------------
  if (cfg.tempEnabled) {
    if (!ds_pending && ((uint32_t)(now - last_ds_request_loop_ms) >= 1000UL)) {
      dsSensor.requestTemperatures();
      ds_pending = true;
      ds_request_ms = now;
      last_ds_request_loop_ms = now;
    }
    if (ds_pending && ((uint32_t)(now - ds_request_ms) >= 120UL)) {
      float tmp = dsSensor.getTempCByIndex(0);
      if (tmp != DEVICE_DISCONNECTED_C) {
        ds_temp_c = tmp;
        temp_valid = true;
        g_tempFailStreak = 0;
        g_tempLastOkMs = now;
        tempCount++;
        totalCount++;
        sampleChanged = true;
      } else {
        if (g_tempFailStreak < 255) g_tempFailStreak++;
        if (g_tempFailStreak >= TEMP_FAILS_BEFORE_INVALID &&
            (g_tempLastOkMs == 0 || (uint32_t)(now - g_tempLastOkMs) >= TEMP_HOLD_TIMEOUT_MS)) {
          invalidateTemp();
        }
      }
      ds_pending = false;
    }
  }
  // ---------------- FPS/stat ----------------
  if ((uint32_t)(now - lastFrameReportMs) >= 1000UL) {
    dmsFramesPerSecond = dmsCount;
    ain2FramesPerSecond = ain2Count;
    tempFramesPerSecond = tempCount;
    totalFramesPerSecond = totalCount;
    framesPerSecond = dmsFramesPerSecond;
    dmsCount = 0;
    ain2Count = 0;
    tempCount = 0;
    totalCount = 0;
    lastFrameReportMs = now;
  }
  if (sampleChanged) {
    g_lastSampleSeq++;
    captureStreamSample(g_lastSampleSeq, now, nowUs);
    captureLogSample(now);
  }
  oledUpdateLive(false);
}

static void sensorTask(void* /*arg*/) {
  // Give Wi-Fi / system tasks time to settle after boot.
  vTaskDelay(pdMS_TO_TICKS(50));
  g_sensorTaskRunning = true;
  uint32_t nextWakeUs = micros();
  uint32_t lastYieldMs = millis();
  for (;;) {
    if (g_mode != RunMode::CONFIG || !g_measurementsRunning) {
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }
    sensorLoopAp();
    // Adaptive wake-up: for high-rate channels stay tight, for slower rates yield more.
    float maxHz = 0.0f;
    if (cfg.dmsEnabled && cfg.dmsHz > maxHz) maxHz = cfg.dmsHz;
    if (cfg.ain2Enabled && cfg.ain2Hz > maxHz) maxHz = cfg.ain2Hz;
    uint32_t sleepUs = (maxHz >= 500.0f) ? 100U : (maxHz >= 200.0f ? 250U : 1000U);
    nextWakeUs += sleepUs;
    uint32_t nowUs = micros();
    int32_t remain = (int32_t)(nextWakeUs - nowUs);
    if (remain <= 0) {
      nextWakeUs = nowUs;
      taskYIELD();
    } else if (remain >= 1000) {
      vTaskDelay(pdMS_TO_TICKS((remain + 999) / 1000));
      lastYieldMs = millis();
    } else {
      delayMicroseconds((uint32_t)remain);
      if ((uint32_t)(millis() - lastYieldMs) >= 2U) {
        taskYIELD();
        lastYieldMs = millis();
      }
    }
  }
}
static void captureFieldSnapshot() {
  if (cfg.dmsEnabled) {
    int32_t rawDms = lastDmsRaw;
    if (readDmsFast(dms_mV, &rawDms)) {
      if (!applyDownSpikeFilter(rawDms, lastDmsRaw, lastDmsRawSeen, 80, 0.75f)) {
        dms_mV = rawTo_mV_gain(lastDmsRaw, g_adsDmsCfg.gain);
      }
      dms_mV_per_V = dmsTo_mV_per_V(dms_mV);
      dms_valid = true;
    } else {
      invalidateDms();
    }
  } else {
    invalidateDms();
  }
  if (cfg.ain2Enabled) {
    float tmpAin2 = NAN;
    int32_t rawAin2 = lastAin2Raw;
    if (readChannelRaw(g_adsAin2Cfg, tmpAin2, &rawAin2)) {
      lastAin2Raw = rawAin2;
      lastAin2RawSeen = true;
      ain2_mV = tmpAin2;
      ain2_valid = true;
    } else {
      invalidateAin2();
    }
  } else {
    invalidateAin2();
  }
  if (cfg.tempEnabled) {
    float tmpC = NAN;
    if (readTempBlocking(tmpC)) {
      ds_temp_c = tmpC;
      temp_valid = true;
    } else {
      invalidateTemp();
    }
  } else {
    invalidateTemp();
  }
  dmsFramesPerSecond = 0;
  ain2FramesPerSecond = 0;
  tempFramesPerSecond = 0;
  totalFramesPerSecond = 0;
  framesPerSecond = 0;
}
static void resetLogBuffer() {
  portENTER_CRITICAL(&g_logBufMux);
  g_logBufHead = 0;
  g_logBufTail = 0;
  g_logBufCount = 0;
  g_logBufMax = 0;
  g_logDropped = 0;
  portEXIT_CRITICAL(&g_logBufMux);
}
static bool pushLogSample(const LogSample& s) {
  bool ok = true;
  portENTER_CRITICAL(&g_logBufMux);
  if (g_logBufCount >= LOG_BUF_SIZE) {
    g_logDropped++;
    ok = false;
  } else {
    g_logBuf[g_logBufHead] = s;
    g_logBufHead = (uint16_t)((g_logBufHead + 1U) % LOG_BUF_SIZE);
    g_logBufCount++;
    if (g_logBufCount > g_logBufMax) g_logBufMax = g_logBufCount;
  }
  portEXIT_CRITICAL(&g_logBufMux);
  return ok;
}
static bool popLogSample(LogSample& s) {
  bool ok = false;
  portENTER_CRITICAL(&g_logBufMux);
  if (g_logBufCount != 0) {
    s = g_logBuf[g_logBufTail];
    g_logBufTail = (uint16_t)((g_logBufTail + 1U) % LOG_BUF_SIZE);
    g_logBufCount--;
    ok = true;
  }
  portEXIT_CRITICAL(&g_logBufMux);
  return ok;
}

static void resetStreamBuffer() {
  portENTER_CRITICAL(&g_streamBufMux);
  g_streamBufHead = 0;
  g_streamBufTail = 0;
  g_streamBufCount = 0;
  g_streamBufMax = 0;
  g_streamDropped = 0;
  g_streamProduced = 0;
  g_streamSent = 0;
  portEXIT_CRITICAL(&g_streamBufMux);
}
static bool pushStreamPacket(const StreamPacketV1& s) {
  bool ok = true;
  portENTER_CRITICAL(&g_streamBufMux);
  g_streamProduced++;
  if (g_streamBufCount >= STREAM_BUF_SIZE) {
    g_streamDropped++;
    ok = false;
  } else {
    g_streamBuf[g_streamBufHead] = s;
    g_streamBufHead = (uint16_t)((g_streamBufHead + 1U) % STREAM_BUF_SIZE);
    g_streamBufCount++;
    if (g_streamBufCount > g_streamBufMax) g_streamBufMax = g_streamBufCount;
  }
  portEXIT_CRITICAL(&g_streamBufMux);
  return ok;
}
static uint16_t popStreamBatch(StreamPacketV1* out, uint16_t maxCount) {
  uint16_t n = 0;
  portENTER_CRITICAL(&g_streamBufMux);
  while (g_streamBufCount != 0 && n < maxCount) {
    out[n++] = g_streamBuf[g_streamBufTail];
    g_streamBufTail = (uint16_t)((g_streamBufTail + 1U) % STREAM_BUF_SIZE);
    g_streamBufCount--;
  }
  portEXIT_CRITICAL(&g_streamBufMux);
  return n;
}
static void captureLogSample(uint32_t t_ms) {
  if (g_mode != RunMode::CONFIG) return;
  if (!cfg.sdLogEnabled) return;
  if (g_logPeriodMs > 0 && (uint32_t)(t_ms - g_lastQueuedLogMs) < g_logPeriodMs) return;
  g_lastQueuedLogMs = t_ms;
  LogSample s{};
  s.t_ms = t_ms;
  s.t_us = micros();
  s.dms_on = cfg.dmsEnabled ? 1U : 0U;
  s.dms_valid = dms_valid ? 1U : 0U;
  s.dms_raw = lastDmsRaw;
  s.dms_mV = dms_valid ? dms_mV : NAN;
  s.dms_mV_per_V = dms_valid ? dms_mV_per_V : NAN;
  s.ain2_on = cfg.ain2Enabled ? 1U : 0U;
  s.ain2_valid = ain2_valid ? 1U : 0U;
  s.ain2_mode = cfg.ain2Mode;
  s.ain2_raw = lastAin2Raw;
  s.ain2_mV = ain2_valid ? ain2_mV : NAN;
  s.ain2_value = ain2_valid ? ain2DerivedValue() : NAN;
  s.temp_on = cfg.tempEnabled ? 1U : 0U;
  s.temp_valid = temp_valid ? 1U : 0U;
  s.temp_C = temp_valid ? ds_temp_c : NAN;
  s.selftest_ok = selfTestOk ? 1U : 0U;
  pushLogSample(s);
}
// ============================================================
// SD logging
// ============================================================
static uint32_t g_lastSdMountTryMs = 0;
static uint32_t g_sdMountRetryMs = 3000;
static inline bool sdSpiBegin() {
  if (!spiBusLock(pdMS_TO_TICKS(2))) return false;
  pinMode(PIN_SD_CS, OUTPUT);
  digitalWrite(PIN_SD_CS, HIGH);
  sdBusPrepare();
  SPI.begin(PIN_SPI_SCK, PIN_SPI_MISO, PIN_SPI_MOSI, PIN_SD_CS);
  delayMicroseconds(5);
  return true;
}
static inline void sdSpiEnd() {
  digitalWrite(PIN_SD_CS, HIGH);
  delayMicroseconds(5);
  SPI.end();
  restoreAdsBusAfterSd();
  spiBusUnlock();
}
static bool sdMountIfNeeded() {
  if (g_sdMounted) return true;
  if (g_mode != RunMode::CONFIG) return false;
  const uint32_t now = millis();
  if ((uint32_t)(now - g_lastSdMountTryMs) < g_sdMountRetryMs) {
    return false;
  }
  g_lastSdMountTryMs = now;
  g_sdMountTried = true;
  g_sdBusy = true;
  g_sdStatus = "mounting";
  bool ok = false;
  const uint32_t freqs[] = {1000000UL, 400000UL, 250000UL};
  for (size_t i = 0; i < sizeof(freqs) / sizeof(freqs[0]); ++i) {
    if (!sdSpiBegin()) { delay(30); continue; }
    ok = SD.begin(PIN_SD_CS, SPI, freqs[i]);
    sdSpiEnd();
    if (ok) break;
    delay(30);
  }
  g_sdMounted = ok;
  g_sdStatus = ok ? "mounted" : "mount_failed";
  g_sdBusy = false;
  if (!ok) {
    Serial.println("[SD] mount failed");
    return false;
  }
  Serial.println("[SD] mounted");
  return true;
}
static void closeLogFile() {
  g_sdBusy = true;
  if (g_logFile) {
    if (!sdSpiBegin()) { g_sdBusy = false; return; }
    g_logFile.flush();
    g_logFile.close();
    sdSpiEnd();
  } else {
    restoreAdsBusAfterSd();
  }
  g_sdMounted = false;
  g_sdBusy = false;
  g_logFileName = "";
  g_logLineCount = 0;
  g_lastLoggedSampleSeq = 0;
  resetLogBuffer();
}
static bool openNextLogFile() {
  if (!sdMountIfNeeded()) return false;
  if (g_logFile) {
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return false; }
    g_logFile.flush();
    g_logFile.close();
    sdSpiEnd();
    g_sdBusy = false;
    g_logFileName = "";
    g_logLineCount = 0;
    g_lastLoggedSampleSeq = 0;
  }
  char path[64];
  for (uint32_t seq = g_logFileSeq + 1; seq < 1000000UL; ++seq) {
    snprintf(path, sizeof(path), "/%s_%03lu.bin", cfg.logBaseName, (unsigned long)seq);
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return false; }
    Serial.print("[SD] probe file: ");
    Serial.println(path);
    bool exists = SD.exists(path);
    if (!exists) {
      g_logFile = SD.open(path, FILE_WRITE);
      if (!g_logFile) {
        sdSpiEnd();
        g_sdBusy = false;
        g_sdStatus = "open_failed";
        Serial.print("[SD] open failed: ");
        Serial.println(path);
        return false;
      }
      g_logFileSeq = seq;
      g_logFileName = String(path);
      g_logLineCount = 0;
      g_udbfStartNs = 0;
      g_udbfHeaderWritten = false;
      g_udbfRecordIndex = 0;
      g_udbfFileStartUs = 0;
      g_lastQueuedLogMs = 0;
      if (!writeBinHeader(g_logFile)) {
        g_logFile.close();
        sdSpiEnd();
        g_sdBusy = false;
        g_sdStatus = "header_failed";
        return false;
      }
      g_logFile.flush();
      g_logLastFlushMs = millis();
      g_logLastDrainMs = millis();
      sdSpiEnd();
      g_sdBusy = false;
      g_sdStatus = "logging";
      Serial.print("[SD] logging to ");
      Serial.println(g_logFileName);
      return true;
    }
    sdSpiEnd();
    g_sdBusy = false;
  }
  g_sdStatus = "no_filename";
  return false;
}
static void syncSdLogger(bool forceReopen) {
  if (g_mode != RunMode::CONFIG) {
    closeLogFile();
    g_sdStatus = "field_disabled";
    return;
  }
  if (!cfg.sdLogEnabled) {
    closeLogFile();
    g_sdStatus = "disabled";
    return;
  }
  if (!sdMountIfNeeded()) return;
  if (forceReopen || !g_logFile) {
    (void)openNextLogFile();
  }
}
static void loggerLoop() {
  if (g_mode != RunMode::CONFIG) return;
  if (!cfg.sdLogEnabled) return;
  uint16_t bufCountSnapshot;
  portENTER_CRITICAL(&g_logBufMux);
  bufCountSnapshot = g_logBufCount;
  portEXIT_CRITICAL(&g_logBufMux);
  if (bufCountSnapshot == 0) return;
  const uint32_t now = millis();
  if (bufCountSnapshot < LOG_BATCH_SIZE && (uint32_t)(now - g_logLastDrainMs) < LOG_FORCE_FLUSH_MS) {
    return;
  }
  syncSdLogger(false);
  if (!g_logFile) return;
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  if (cfg.logRotateKB > 0 && (uint32_t)g_logFile.size() >= cfg.logRotateKB * 1024UL) {
    sdSpiEnd();
    g_sdBusy = false;
    if (!openNextLogFile()) return;
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return; }
  }
  LogSample s;
  uint16_t writtenSamples = 0;
  while (writtenSamples < LOG_BATCH_SIZE) {
    if (!popLogSample(s)) break;
    if (!writeBinRecord(g_logFile, s)) break;
    writtenSamples++;
  }
  if (writtenSamples > 0) {
    g_logLineCount += writtenSamples;
    g_lastLoggedSampleSeq = g_lastSampleSeq;
    g_lastLogWriteMs = now;
  }
  if ((uint32_t)(now - g_logLastFlushMs) >= g_logFlushPeriodMs) {
    g_logFile.flush();
    g_logLastFlushMs = now;
  }
  g_logLastDrainMs = now;
  sdSpiEnd();
  g_sdBusy = false;
}
static void sdTask(void* /*arg*/) {
  vTaskDelay(pdMS_TO_TICKS(200));
  for (;;) {
    if (g_mode == RunMode::CONFIG && g_measurementsRunning && cfg.sdLogEnabled) {
      loggerLoop();
      vTaskDelay(pdMS_TO_TICKS(20));
    } else {
      vTaskDelay(pdMS_TO_TICKS(100));
    }
  }
}
static bool isSafeBinPath(const String& name) {
  if (!name.length()) return false;
  if (name.indexOf("..") >= 0) return false;
  if (name.indexOf('\\') >= 0) return false;
  if (!name.startsWith("/")) return false;
  if (!name.endsWith(".bin")) return false;
  return true;
}
static bool isSafeFilePath(const String& name) {
  if (!name.length()) return false;
  if (name.indexOf("..") >= 0) return false;
  if (name.indexOf('\\') >= 0) return false;
  if (!name.startsWith("/")) return false;
  String lower = name;
  lower.toLowerCase();
  return lower.endsWith(".bin") || lower.endsWith(".dat") || lower.endsWith(".csv") ||
         lower.endsWith(".txt") || lower.endsWith(".json") || lower.endsWith(".udbf");
}
static uint32_t crc32Update(uint32_t crc, const uint8_t* data, size_t len) {
  crc = ~crc;
  for (size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int b = 0; b < 8; ++b) {
      crc = (crc & 1U) ? ((crc >> 1) ^ 0xEDB88320UL) : (crc >> 1);
    }
  }
  return ~crc;
}
static void zipWriteLe16(WiFiClient& client, uint16_t v) {
  uint8_t b[2] = { (uint8_t)(v & 0xFF), (uint8_t)((v >> 8) & 0xFF) };
  client.write(b, sizeof(b));
}
static void zipWriteLe32(WiFiClient& client, uint32_t v) {
  uint8_t b[4] = {
    (uint8_t)(v & 0xFF),
    (uint8_t)((v >> 8) & 0xFF),
    (uint8_t)((v >> 16) & 0xFF),
    (uint8_t)((v >> 24) & 0xFF)
  };
  client.write(b, sizeof(b));
}
static String zipBaseName(const String& path) {
  String out = path;
  int slash = out.lastIndexOf('/');
  if (slash >= 0) out = out.substring(slash + 1);
  if (!out.length()) out = "file.bin";
  return out;
}
static void zipWriteLocalHeader(WiFiClient& client, const String& name, uint32_t crc, uint32_t size) {
  client.write((const uint8_t*)"PK", 4);
  zipWriteLe16(client, 20);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe32(client, crc);
  zipWriteLe32(client, size);
  zipWriteLe32(client, size);
  zipWriteLe16(client, (uint16_t)name.length());
  zipWriteLe16(client, 0);
  client.write((const uint8_t*)name.c_str(), name.length());
}
static void zipWriteCentralHeader(WiFiClient& client, const String& name, uint32_t crc, uint32_t size, uint32_t localOffset) {
  client.write((const uint8_t*)"PK", 4);
  zipWriteLe16(client, 20);
  zipWriteLe16(client, 20);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe32(client, crc);
  zipWriteLe32(client, size);
  zipWriteLe32(client, size);
  zipWriteLe16(client, (uint16_t)name.length());
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe32(client, 0);
  zipWriteLe32(client, localOffset);
  client.write((const uint8_t*)name.c_str(), name.length());
}
static void zipWriteEndOfCentralDir(WiFiClient& client, uint16_t entryCount, uint32_t centralSize, uint32_t centralOffset) {
  client.write((const uint8_t*)"PK", 4);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, 0);
  zipWriteLe16(client, entryCount);
  zipWriteLe16(client, entryCount);
  zipWriteLe32(client, centralSize);
  zipWriteLe32(client, centralOffset);
  zipWriteLe16(client, 0);
}
static String csvBaseNameFromBin(const String& path) {
  String out = zipBaseName(path);
  if (out.endsWith(".bin")) out = out.substring(0, out.length() - 4) + ".csv";
  else out += ".csv";
  return out;
}
static void handleApiSdConvertBinCsv() {
  if (cfg.streamModeEnabled) {
    sendJsonError(409, "stream_mode_active");
    return;
  }
  if (g_mode != RunMode::CONFIG) {
    sendJsonError(403, "convert_available_in_config_only");
    return;
  }
  if (!sdMountIfNeeded()) {
    sendJsonError(503, "sd_not_mounted");
    return;
  }
  String name = server.hasArg("name") ? server.arg("name") : "";
  name.trim();
  if (!isSafeBinPath(name)) {
    sendJsonError(400, "invalid_bin_name");
    return;
  }
  String outPath = csvBaseNameFromBin(name);
  if (!outPath.startsWith("/")) outPath = String("/") + outPath;
  if (g_logFile && g_logFileName == name) {
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return; }
    g_logFile.flush();
    sdSpiEnd();
    g_sdBusy = false;
  }
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  File f = SD.open(name.c_str(), FILE_READ);
  if (!f) {
    sdSpiEnd();
    g_sdBusy = false;
    sendJsonError(404, "bin_not_found");
    return;
  }
  BinLogHeader h{};
  if (f.read((uint8_t*)&h, sizeof(h)) != (int)sizeof(h)) {
    f.close();
    sdSpiEnd();
    g_sdBusy = false;
    sendJsonError(400, "invalid_bin_format");
    return;
  }
  const bool isV1 = (strncmp(h.magic, "MKPBIN1", 7) == 0);
  const bool isV2 = (strncmp(h.magic, "MKPBIN2", 7) == 0);
  if (!isV1 && !isV2) {
    f.close();
    sdSpiEnd();
    g_sdBusy = false;
    sendJsonError(400, "invalid_bin_format");
    return;
  }
  const bool dmsEnabled = isV2 ? ((h.flags & BIN_FLAG_DMS_ENABLED) != 0) : true;
  const bool ain2Enabled = isV2 ? ((h.flags & BIN_FLAG_AIN2_ENABLED) != 0) : true;
  const bool tempEnabled = isV2 ? ((h.flags & BIN_FLAG_TEMP_ENABLED) != 0) : true;
  const bool ain2VoltMode = isV2 ? ((h.flags & BIN_FLAG_AIN2_MODE_VOLT) != 0) : (cfg.ain2Mode == AIN2_MODE_VOLT);
  const uint16_t expectedRecordSize = isV2
    ? (uint16_t)(sizeof(uint32_t) + (dmsEnabled ? sizeof(int32_t) : 0) + (ain2Enabled ? sizeof(int32_t) : 0) + (tempEnabled ? sizeof(int16_t) : 0))
    : (uint16_t)sizeof(BinLogRecordV1);
  if (h.recordSize != expectedRecordSize) {
    f.close();
    sdSpiEnd();
    g_sdBusy = false;
    sendJsonError(400, "unsupported_bin_layout");
    return;
  }
  if (SD.exists(outPath.c_str())) SD.remove(outPath.c_str());
  File out = SD.open(outPath.c_str(), FILE_WRITE);
  if (!out) {
    f.close();
    sdSpiEnd();
    g_sdBusy = false;
    sendJsonError(500, "csv_create_failed");
    return;
  }
  String header = "TimeCounter";
  if (dmsEnabled) header += ",DMS_RAW,DMS_MV,DMS_MV_V,DMS,DMS_uM_M";
  if (ain2Enabled) header += ain2VoltMode ? ",AIN2_RAW,AIN2_V" : ",AIN2_RAW,AIN2_mm";
  if (tempEnabled) header += ",TEMP_C";
  header += "\r\n";
  out.print(header);
  uint32_t rows = 0;
  while (f.available() >= h.recordSize) {
    uint32_t timeCounterUs = 0;
    int32_t dmsRaw = 0;
    int32_t ain2Raw = 0;
    int16_t tempC100 = 0;
    bool rowOk = true;
    if (f.read((uint8_t*)&timeCounterUs, sizeof(timeCounterUs)) != (int)sizeof(timeCounterUs)) break;
    if (isV1) {
      BinLogRecordV1 r{};
      r.dt_us = timeCounterUs;
      if (f.read(((uint8_t*)&r) + sizeof(uint32_t), sizeof(BinLogRecordV1) - sizeof(uint32_t)) != (int)(sizeof(BinLogRecordV1) - sizeof(uint32_t))) break;
      dmsRaw = r.dms_raw;
      ain2Raw = r.ain2_raw;
      tempC100 = r.temp_c_x100;
    } else {
      if (dmsEnabled && f.read((uint8_t*)&dmsRaw, sizeof(dmsRaw)) != (int)sizeof(dmsRaw)) { rowOk = false; }
      if (rowOk && ain2Enabled && f.read((uint8_t*)&ain2Raw, sizeof(ain2Raw)) != (int)sizeof(ain2Raw)) { rowOk = false; }
      if (rowOk && tempEnabled && f.read((uint8_t*)&tempC100, sizeof(tempC100)) != (int)sizeof(tempC100)) { rowOk = false; }
      if (!rowOk) break;
    }
    String row = String(timeCounterUs);
    if (dmsEnabled) {
      const float dmsMv = rawTo_mV_gain(dmsRaw, GAIN_DMS);
      const float dmsMvPerV = dmsTo_mV_per_V(dmsMv);
      row += "," + String(dmsRaw);
      row += "," + String(dmsMv, 4);
      row += "," + String(dmsMvPerV, 4);
      row += ",";        // DMS engineering value placeholder
      row += ",";        // DMS_uM_M placeholder until gauge factor / bridge type is configured
    }
    if (ain2Enabled) {
      row += "," + String(ain2Raw);
      const float ain2MvLocal = rawTo_mV_gain(ain2Raw, GAIN_AIN2);
      if (ain2VoltMode) {
        const float adcV = ain2MvLocal / 1000.0f;
        const float fullscale = (cfg.ain2AdcFullscaleV > 0.001f) ? cfg.ain2AdcFullscaleV : 1.0f;
        const float inputScale = (cfg.ain2InputFullscaleV > 0.0f) ? cfg.ain2InputFullscaleV : fullscale;
        const float v = clampf(adcV / fullscale, 0.0f, 1.0f) * inputScale;
        row += "," + String(v, 4);
      } else {
        const float mm = clampf((ain2MvLocal / 3300.0f), 0.0f, 1.0f) * cfg.ain2LengthMm;
        row += "," + String(mm, 4);
      }
    }
    if (tempEnabled) {
      row += "," + String(((float)tempC100) / 100.0f, 2);
    }
    row += "\n";
    out.print(row);
    ++rows;
    delay(0);
  }
  out.flush();
  out.close();
  f.close();
  sdSpiEnd();
  g_sdBusy = false;
  String resp = String("{\"ok\":true,\"saved\":\"") + outPath + "\",\"rows\":" + String(rows) + "}";
  server.send(200, "application/json", resp);
}
static void handleApiSdArchive() {
  if (cfg.streamModeEnabled) {
    sendJsonError(409, "stream_mode_active");
    return;
  }
  if (g_mode != RunMode::CONFIG) {
    sendJsonError(403, "archive_available_in_config_only");
    return;
  }
  if (!sdMountIfNeeded()) {
    sendJsonError(503, "sd_not_mounted");
    return;
  }
  static constexpr int MAX_SEL = 24;
  struct ZipMeta {
    String path;
    String name;
    uint32_t size;
    uint32_t crc;
    uint32_t offset;
    bool valid;
  } meta[MAX_SEL];
  int selCount = 0;
  for (int i = 0; i < server.args() && selCount < MAX_SEL; ++i) {
    if (server.argName(i) == "name") {
      String n = server.arg(i);
      n.trim();
      if (!isSafeFilePath(n)) continue;
      bool dup = false;
      for (int j = 0; j < selCount; ++j) if (meta[j].path == n) { dup = true; break; }
      if (!dup) {
        meta[selCount].path = n;
        meta[selCount].name = zipBaseName(n);
        meta[selCount].size = 0;
        meta[selCount].crc = 0;
        meta[selCount].offset = 0;
        meta[selCount].valid = false;
        ++selCount;
      }
    }
  }
  if (selCount <= 0) {
    sendJsonError(400, "no_file_selected");
    return;
  }
  bool currentSelected = false;
  for (int i = 0; i < selCount; ++i) if (meta[i].path == g_logFileName) { currentSelected = true; break; }
  if (currentSelected && g_logFile) {
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return; }
    g_logFile.flush();
    sdSpiEnd();
    g_sdBusy = false;
  }
  if (selCount == 1) {
    String name = meta[0].path;
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return; }
    File f = SD.open(name.c_str(), FILE_READ);
    if (!f) {
      sdSpiEnd();
      g_sdBusy = false;
      sendJsonError(404, "file_not_found");
      return;
    }
    String downloadName = zipBaseName(name);
    server.sendHeader("Content-Type", "application/octet-stream");
    String cd = "attachment; filename=\"" + downloadName + "\"";
    server.sendHeader("Content-Disposition", cd);
    server.sendHeader("Cache-Control", "no-store");
    server.streamFile(f, "application/octet-stream");
    f.close();
    sdSpiEnd();
    g_sdBusy = false;
    return;
  }
  uint32_t localRegionSize = 0;
  uint32_t centralSize = 0;
  int validCount = 0;
  static uint8_t ioBuf[1024];
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  for (int i = 0; i < selCount; ++i) {
    File f = SD.open(meta[i].path.c_str(), FILE_READ);
    if (!f) continue;
    uint32_t crc = 0;
    uint32_t sz = 0;
    while (f.available()) {
      size_t n = f.read(ioBuf, sizeof(ioBuf));
      if (!n) break;
      crc = crc32Update(crc, ioBuf, n);
      sz += (uint32_t)n;
      delay(0);
    }
    f.close();
    meta[i].size = sz;
    meta[i].crc = crc;
    meta[i].valid = true;
    meta[i].offset = localRegionSize;
    localRegionSize += 30U + (uint32_t)meta[i].name.length() + sz;
    centralSize += 46U + (uint32_t)meta[i].name.length();
    ++validCount;
  }
  sdSpiEnd();
  g_sdBusy = false;
  if (validCount <= 0) {
    sendJsonError(404, "file_not_found");
    return;
  }
  uint32_t totalLen = localRegionSize + centralSize + 22U;
  server.setContentLength(totalLen);
  server.sendHeader("Content-Type", "application/zip");
  server.sendHeader("Content-Disposition", "attachment; filename=\"data.zip\"");
  server.sendHeader("Cache-Control", "no-store");
  server.send(200, "application/zip", "");
  WiFiClient client = server.client();
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  for (int i = 0; i < selCount; ++i) {
    if (!meta[i].valid) continue;
    File f = SD.open(meta[i].path.c_str(), FILE_READ);
    if (!f) continue;
    zipWriteLocalHeader(client, meta[i].name, meta[i].crc, meta[i].size);
    while (f.available()) {
      size_t n = f.read(ioBuf, sizeof(ioBuf));
      if (!n) break;
      client.write(ioBuf, n);
      delay(0);
    }
    f.close();
    delay(0);
  }
  uint32_t centralOffset = localRegionSize;
  for (int i = 0; i < selCount; ++i) {
    if (!meta[i].valid) continue;
    zipWriteCentralHeader(client, meta[i].name, meta[i].crc, meta[i].size, meta[i].offset);
    delay(0);
  }
  zipWriteEndOfCentralDir(client, (uint16_t)validCount, centralSize, centralOffset);
  sdSpiEnd();
  g_sdBusy = false;
}
static void handleApiSdDownload() {
  if (cfg.streamModeEnabled) {
    sendJsonError(409, "stream_mode_active");
    return;
  }
  if (g_mode != RunMode::CONFIG) {
    sendJsonError(403, "download_available_in_config_only");
    return;
  }
  if (!cfg.sdLogEnabled) {
    sendJsonError(409, "sd_logging_disabled");
    return;
  }
  if (!sdMountIfNeeded()) {
    sendJsonError(503, "sd_not_mounted");
    return;
  }
  String name = server.hasArg("name") ? server.arg("name") : g_logFileName;
  name.trim();
  if (!isSafeBinPath(name)) {
    sendJsonError(400, "invalid_file_name");
    return;
  }
  if (g_logFile && g_logFileName == name) {
    g_sdBusy = true;
    if (!sdSpiBegin()) { g_sdBusy = false; return; }
    g_logFile.flush();
    sdSpiEnd();
    g_sdBusy = false;
  }
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  File f = SD.open(name.c_str(), FILE_READ);
  if (!f) {
    sdSpiEnd();
    g_sdBusy = false;
    sendJsonError(404, "file_not_found");
    return;
  }
  String downloadName = name;
  int slash = downloadName.lastIndexOf('/');
  if (slash >= 0) downloadName = downloadName.substring(slash + 1);
  server.sendHeader("Content-Type", "application/octet-stream");
  server.sendHeader("Content-Disposition", String("attachment; filename=\"") + downloadName + "\"");
  server.sendHeader("Cache-Control", "no-store");
  server.streamFile(f, "application/octet-stream");
  f.close();
  sdSpiEnd();
  g_sdBusy = false;
}
static void handleApiSdDelete() {
  if (cfg.streamModeEnabled) {
    sendJsonError(409, "stream_mode_active");
    return;
  }
  if (g_mode != RunMode::CONFIG) {
    sendJsonError(403, "delete_available_in_config_only");
    return;
  }
  if (!sdMountIfNeeded()) {
    sendJsonError(503, "sd_not_mounted");
    return;
  }
  if (!server.hasArg("plain")) {
    sendJsonError(400, "missing_body");
    return;
  }
  DynamicJsonDocument req(3072);
  DeserializationError err = deserializeJson(req, server.arg("plain"));
  if (err || !req["names"].is<JsonArray>()) {
    sendJsonError(400, "invalid_delete_payload");
    return;
  }
  JsonArray names = req["names"].as<JsonArray>();
  DynamicJsonDocument resp(3072);
  resp["ok"] = true;
  JsonArray deleted = resp.createNestedArray("deleted");
  JsonArray skipped = resp.createNestedArray("skipped");
  bool needRefreshCurrent = false;
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  for (JsonVariant v : names) {
    String n = v.as<String>();
    n.trim();
    if (!isSafeFilePath(n)) {
      skipped.add(n);
      continue;
    }
    if (n == g_logFileName) {
      skipped.add(n);
      continue;
    }
    if (SD.exists(n.c_str()) && SD.remove(n.c_str())) {
      deleted.add(n);
      needRefreshCurrent = true;
    } else {
      skipped.add(n);
    }
    delay(0);
  }
  sdSpiEnd();
  g_sdBusy = false;
  if (needRefreshCurrent) syncSdLogger(false);
  String out;
  serializeJson(resp, out);
  server.send(200, "application/json", out);
}
static void handleApiSdList() {
  if (cfg.streamModeEnabled) {
    sendJsonError(409, "stream_mode_active");
    return;
  }
  if (g_mode != RunMode::CONFIG) {
    sendJsonError(403, "list_available_in_config_only");
    return;
  }
  if (!sdMountIfNeeded()) {
    sendJsonError(503, "sd_not_mounted");
    return;
  }
  DynamicJsonDocument doc(4096);
  doc["ok"] = true;
  doc["current"] = g_logFileName;
  JsonArray files = doc.createNestedArray("files");
  g_sdBusy = true;
  if (!sdSpiBegin()) { g_sdBusy = false; return; }
  File root = SD.open("/");
  if (root) {
    File entry = root.openNextFile();
    while (entry) {
      if (!entry.isDirectory()) {
        String n = entry.name();
        if (!n.startsWith("/")) n = "/" + n;
        if (isSafeFilePath(n)) {
          JsonObject it = files.createNestedObject();
          it["name"] = n;
          it["size"] = (uint32_t)entry.size();
        }
      }
      entry.close();
      entry = root.openNextFile();
    }
    root.close();
  }
  sdSpiEnd();
  g_sdBusy = false;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void sendJsonError(int code, const char* error) {
  String out = String("{\"ok\":false,\"error\":\"") + error + "\"}";
  server.send(code, "application/json", out);
}
// ============================================================
// JSON / Web
// ============================================================
static String modeToString() {
  return (g_mode == RunMode::CONFIG) ? "CONFIG" : "FIELD";
}

static uint16_t streamFlagsNow() {
  uint16_t flags = 0;
  if (cfg.dmsEnabled) flags |= 1u << 0;
  if (dms_valid)      flags |= 1u << 1;
  if (cfg.ain2Enabled) flags |= 1u << 2;
  if (ain2_valid)      flags |= 1u << 3;
  if (cfg.tempEnabled) flags |= 1u << 4;
  if (temp_valid)      flags |= 1u << 5;
  if (cfg.ain2Mode == AIN2_MODE_VOLT) flags |= 1u << 6;
  return flags;
}
static StreamPacketV1 makeStreamPacket(uint32_t seq, uint32_t t_ms, uint32_t t_us) {
  StreamPacketV1 p{};
  memcpy(p.magic, "MKPS", 4);
  p.version = 1;
  p.bytes = (uint16_t)sizeof(StreamPacketV1);
  p.seq = seq;
  p.t_ms = t_ms;
  p.t_us = t_us;
  p.flags = streamFlagsNow();
  p.dms_raw = lastDmsRaw;
  p.dms_mV = dms_valid ? dms_mV : NAN;
  p.dms_mV_per_V = dms_valid ? dms_mV_per_V : NAN;
  p.ain2_raw = lastAin2Raw;
  p.ain2_mV = ain2_valid ? ain2_mV : NAN;
  p.ain2_value = ain2_valid ? ain2DerivedValue() : NAN;
  p.temp_c_x100 = temp_valid ? (int16_t)lroundf(ds_temp_c * 100.0f) : INT16_MIN;
  p.reserved = 0;
  return p;
}
static void captureStreamSample(uint32_t seq, uint32_t t_ms, uint32_t t_us) {
  if (g_mode != RunMode::CONFIG) return;
  if (!cfg.streamModeEnabled) return;
  StreamPacketV1 pkt = makeStreamPacket(seq, t_ms, t_us);
  pushStreamPacket(pkt);
}
static void closeStreamClient() {
  if (g_streamClient) {
    g_streamClient.stop();
  }
}
static void streamTask(void* /*arg*/) {
  vTaskDelay(pdMS_TO_TICKS(250));
  StreamPacketV1 batch[STREAM_BATCH_SIZE];
  for (;;) {
    if (g_mode != RunMode::CONFIG || !cfg.streamModeEnabled) {
      if (g_streamClient) closeStreamClient();
      g_streamStatus = cfg.streamModeEnabled ? "waiting_client" : "disabled";
      vTaskDelay(pdMS_TO_TICKS(50));
      continue;
    }
    if (!g_streamClient || !g_streamClient.connected()) {
      if (g_streamClient) closeStreamClient();
      WiFiClient candidate = g_streamServer.available();
      if (candidate) {
        candidate.setNoDelay(true);
        g_streamClient = candidate;
        g_streamClientCount++;
        g_streamStatus = "streaming";
      } else {
        g_streamStatus = "waiting_client";
        vTaskDelay(pdMS_TO_TICKS(5));
        continue;
      }
    }

    uint16_t n = popStreamBatch(batch, STREAM_BATCH_SIZE);
    if (n == 0) {
      vTaskDelay(pdMS_TO_TICKS(1));
      continue;
    }

    const size_t want = (size_t)n * sizeof(StreamPacketV1);
    const size_t sent = g_streamClient.write((const uint8_t*)batch, want);
    if (sent != want) {
      closeStreamClient();
      g_streamStatus = "client_disconnected";
      vTaskDelay(pdMS_TO_TICKS(5));
      continue;
    }

    g_lastStreamedSampleSeq = batch[n - 1].seq;
    g_streamSent += n;
    g_streamStatus = "streaming";
    taskYIELD();
  }
}
static void appendPerf(JsonObject obj) {
  PerfEstimate p = makePerfEstimate();
  obj["dms_target_hz"] = p.dmsTargetHz;
  obj["ain2_target_hz"] = p.ain2TargetHz;
  obj["temp_target_hz"] = p.tempTargetHz;
  obj["ads_conv_hz"] = p.adsRequestedConvHz;
  obj["ads_load_pct"] = p.adsLoadPct;
  obj["dms_est_hz"] = p.dmsEstimatedHz;
  obj["ain2_est_hz"] = p.ain2EstimatedHz;
  obj["total_est_hz"] = p.totalEstimatedHz;
}
static void appendConfig(JsonObject obj) {
  obj["devEui"] = cfg.devEui;
  obj["appEui"] = cfg.appEui;
  obj["appKey"] = cfg.appKey;
  obj["minutes"] = cfg.intervalMin;
  obj["adr"] = cfg.adr;
  obj["dr"] = cfg.dr;
  obj["dms_enabled"] = cfg.dmsEnabled ? 1 : 0;
  obj["dms_hz"] = cfg.dmsHz;
  obj["dms_k_factor"] = cfg.dmsKFactor;
  obj["ain2_enabled"] = cfg.ain2Enabled ? 1 : 0;
  obj["ain2_hz"] = cfg.ain2Hz;
  obj["ain2_mode"] = ain2ModeName();
  obj["ain2_length_mm"] = cfg.ain2LengthMm;
  obj["ain2_adc_fullscale_v"] = cfg.ain2AdcFullscaleV;
  obj["ain2_input_fullscale_v"] = cfg.ain2InputFullscaleV;
  obj["ain2_volt_display"] = cfg.ain2VoltDisplay;
  obj["ain2_scaling_unit"] = cfg.ain2ScalingUnit;
  obj["ain2_scale_min"] = cfg.ain2ScaleMin;
  obj["ain2_scale_max"] = cfg.ain2ScaleMax;
  obj["temp_enabled"] = cfg.tempEnabled ? 1 : 0;
  obj["temp_hz"] = TEMP_FIXED_HZ;
  obj["sd_log_enabled"] = cfg.sdLogEnabled ? 1 : 0;
  obj["stream_mode_enabled"] = cfg.streamModeEnabled ? 1 : 0;
  obj["sd_base"] = cfg.logBaseName;
  obj["sd_rotate_kb"] = cfg.logRotateKB;
  obj["measurements_running"] = g_measurementsRunning ? 1 : 0;
  obj["measurements_initialized"] = g_measurementsInitialized ? 1 : 0;
  JsonObject perfObj = obj["perf"].to<JsonObject>();
  appendPerf(perfObj);
}
static void appendLive(JsonObject obj) {
  obj["selftest_ok"] = selfTestOk ? 1 : 0;
  obj["selftest_delta_mV"] = selfTestDelta_mV;
  obj["selftest_value"] = selfTestDelta_mV;
  obj["selftest_supported"] = 1;
  obj["selftest_run_supported"] = 1;
  obj["oled_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  obj["oled_activate_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  obj["oled_active"] = oledIsActiveNow() ? 1 : 0;
  obj["measurements_running"] = g_measurementsRunning ? 1 : 0;
  obj["measurements_initialized"] = g_measurementsInitialized ? 1 : 0;
  obj["dms_enabled"] = cfg.dmsEnabled ? 1 : 0;
  obj["dms_valid"] = dms_valid ? 1 : 0;
  obj["dms_raw"] = lastDmsRaw;
  obj["dms_mV"] = dms_valid ? dms_mV : 0.0f;
  obj["dms_mV_per_V"] = dms_valid ? dms_mV_per_V : 0.0f;
  obj["dms_rate_hz_actual"] = dmsFramesPerSecond;
  obj["dms_age_ms"] = g_dmsLastOkUs ? (uint32_t)((micros() - g_dmsLastOkUs) / 1000UL) : 0U;
  obj["ain2_enabled"] = cfg.ain2Enabled ? 1 : 0;
  obj["ain2_valid"] = ain2_valid ? 1 : 0;
  obj["ain2_raw"] = lastAin2Raw;
  obj["ain2_mV"] = ain2_valid ? ain2_mV : 0.0f;
  obj["ain2_mode"] = ain2ModeName();
  obj["ain2_value"] = ain2_valid ? ain2DerivedValue() : 0.0f;
  obj["ain2_unit"] = ain2DerivedUnit();
  obj["ain2_rate_hz_actual"] = ain2FramesPerSecond;
  obj["ain2_age_ms"] = g_ain2LastOkUs ? (uint32_t)((micros() - g_ain2LastOkUs) / 1000UL) : 0U;
  obj["temp_enabled"] = cfg.tempEnabled ? 1 : 0;
  obj["temp_valid"] = temp_valid ? 1 : 0;
  obj["temp_C"] = temp_valid ? ds_temp_c : 0.0f;
  obj["temp_rate_hz_actual"] = tempFramesPerSecond;
  obj["temp_age_ms"] = g_tempLastOkMs ? (uint32_t)(millis() - g_tempLastOkMs) : 0U;
  obj["frames_s"] = framesPerSecond;
  obj["total_rate_hz_actual"] = totalFramesPerSecond;
  JsonObject perfObj = obj["perf"].to<JsonObject>();
  appendPerf(perfObj);
  obj["sd_mounted"] = g_sdMounted ? 1 : 0;
  obj["sd_status"] = g_sdStatus;
  obj["sd_file"] = g_logFileName;
  obj["stream_mode_enabled"] = cfg.streamModeEnabled ? 1 : 0;
  obj["stream_status"] = g_streamStatus;
  obj["stream_port"] = STREAM_TCP_PORT;
  obj["sd_lines"] = g_logLineCount;
  obj["sd_buffered"] = g_logBufCount;
  obj["sd_buffered_max"] = g_logBufMax;
  obj["sd_dropped"] = g_logDropped;
}
static void appendController(JsonObject obj) {
  char mac48[13];
  char chipEui[17];
  chipMac48Hex(mac48);
  chipDevEuiHex(chipEui);
  obj["fw"] = FW_VERSION;
  obj["build_date"] = __DATE__;
  obj["build_time"] = __TIME__;
  obj["mode"] = modeToString();
  obj["ip"] = (g_mode == RunMode::CONFIG) ? g_apIp.toString() : String("-");
  obj["uptime_s"] = millis() / 1000UL;
  obj["heap_free"] = ESP.getFreeHeap();
  obj["board"] = BOARD_NAME;
  obj["chip_model"] = ESP.getChipModel();
  obj["chip_rev"] = ESP.getChipRevision();
  obj["cpu_mhz"] = ESP.getCpuFreqMHz();
  obj["flash_mb"] = (uint32_t)(ESP.getFlashChipSize() / (1024UL * 1024UL));
  obj["chip_mac48"] = mac48;
  obj["chip_eui64"] = chipEui;
  obj["ap_ssid"] = (g_mode == RunMode::CONFIG) ? g_apSsid : String("-");
  obj["sd_status"] = g_sdStatus;
  obj["sd_file"] = g_logFileName;
  obj["stream_mode_enabled"] = cfg.streamModeEnabled ? 1 : 0;
  obj["stream_status"] = g_streamStatus;
  obj["stream_port"] = STREAM_TCP_PORT;
  obj["stream_client_connected"] = (g_streamClient && g_streamClient.connected()) ? 1 : 0;
  obj["stream_clients_total"] = g_streamClientCount;
  obj["stream_produced"] = g_streamProduced;
  obj["stream_sent"] = g_streamSent;
  obj["stream_dropped"] = g_streamDropped;
  obj["stream_buffered"] = g_streamBufCount;
  obj["stream_buffered_max"] = g_streamBufMax;
  obj["selftest_supported"] = 1;
  obj["selftest_run_supported"] = 1;
  obj["selftest_value"] = selfTestDelta_mV;
  obj["oled_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  obj["oled_activate_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  obj["oled_active"] = oledIsActiveNow() ? 1 : 0;
}

static void ensureMeasurementTasksStarted() {
  if (g_measurementTasksStarted) return;
  xTaskCreatePinnedToCore(sensorTask, "sensorTask", 8192, nullptr, 1, &g_sensorTaskHandle, 1);
  xTaskCreatePinnedToCore(sdTask, "sdTask", 8192, nullptr, 1, &g_sdTaskHandle, 1);
  xTaskCreatePinnedToCore(streamTask, "streamTask", 6144, nullptr, 1, &g_streamTaskHandle, 0);
  g_measurementTasksStarted = true;
}

static void stopMeasurementsManual() {
  g_measurementsRunning = false;
  digitalWrite(PIN_MOSFET, LOW);
  closeLogFile();
  resetLogBuffer();
  invalidateDms();
  invalidateAin2();
  invalidateTemp();
  dmsFramesPerSecond = 0;
  ain2FramesPerSecond = 0;
  tempFramesPerSecond = 0;
  totalFramesPerSecond = 0;
  framesPerSecond = 0;
  g_lastSampleSeq = 0;
  if (g_mode == RunMode::CONFIG) {
    g_sdStatus = cfg.sdLogEnabled ? "ready" : "disabled";
    oledShowBanner("CONFIG MODE", "Measurements stopped");
  }
}

static bool startMeasurementsManual() {
  if (g_mode != RunMode::CONFIG) return false;
  if (!g_measurementsInitialized) {
    adsInit();
    dsInit();
    if (cfg.dmsEnabled) runSelfTest();
    else {
      selfTestOk = false;
      selfTestDelta_mV = NAN;
    }
    if (cfg.dmsEnabled) restoreDmsRunMode();
    g_measurementsInitialized = true;
  }
  ensureMeasurementTasksStarted();
  last_dms_read_us = micros();
  last_ain2_read_us = micros();
  g_lastSampleSeq = 0;
  g_lastLoggedSampleSeq = 0;
  g_lastStreamedSampleSeq = 0;
  syncSdLogger(true);
  g_measurementsRunning = true;
  if (g_mode == RunMode::CONFIG) {
    oledShowBanner("CONFIG MODE", "Measurements running");
  }
  return true;
}

static void handleApiMeasureControlPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"missing body\"}");
    return;
  }
  StaticJsonDocument<256> req;
  DeserializationError err = deserializeJson(req, server.arg("plain"));
  if (err) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"invalid json\"}");
    return;
  }
  const String action = String((const char*)(req["action"] | ""));
  bool ok = false;
  if (action == "start") {
    ok = startMeasurementsManual();
  } else if (action == "stop") {
    stopMeasurementsManual();
    ok = true;
  } else {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"invalid action\"}");
    return;
  }
  StaticJsonDocument<384> doc;
  doc["ok"] = ok;
  doc["measurements_running"] = g_measurementsRunning ? 1 : 0;
  doc["measurements_initialized"] = g_measurementsInitialized ? 1 : 0;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

static void handleApiLive() {
  if (cfg.streamModeEnabled) {
    sendJsonError(409, "stream_mode_active");
    return;
  }
  StaticJsonDocument<1024> doc;
  doc["mode"] = modeToString();
  appendLive(doc.to<JsonObject>());
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiState() {
  if (cfg.streamModeEnabled) {
    StaticJsonDocument<1024> doc;
    doc["fw"] = FW_VERSION;
    doc["build_date"] = __DATE__;
    doc["build_time"] = __TIME__;
    doc["mode"] = modeToString();
    doc["ip"] = (g_mode == RunMode::CONFIG) ? g_apIp.toString() : String("-");
    doc["uptime_s"] = millis() / 1000UL;
    doc["heap_free"] = ESP.getFreeHeap();
    doc["selftest_supported"] = 1;
    doc["selftest_run_supported"] = 1;
    doc["selftest_value"] = selfTestDelta_mV;
    doc["oled_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
    doc["oled_activate_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  doc["oled_active"] = oledIsActiveNow() ? 1 : 0;
    appendConfig(doc["cfg"].to<JsonObject>());
    appendController(doc["controller"].to<JsonObject>());
    String out;
    serializeJson(doc, out);
    server.send(200, "application/json", out);
    return;
  }
  StaticJsonDocument<2304> doc;
  doc["fw"] = FW_VERSION;
  doc["build_date"] = __DATE__;
  doc["build_time"] = __TIME__;
  doc["mode"] = modeToString();
  doc["ip"] = (g_mode == RunMode::CONFIG) ? g_apIp.toString() : String("-");
  doc["uptime_s"] = millis() / 1000UL;
  doc["heap_free"] = ESP.getFreeHeap();
  doc["selftest_supported"] = 1;
  doc["selftest_run_supported"] = 1;
  doc["selftest_value"] = selfTestDelta_mV;
  doc["oled_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  doc["oled_activate_supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  doc["oled_active"] = oledIsActiveNow() ? 1 : 0;
  doc["lora_enabled"] = g_loraEnabled ? 1 : 0;
  doc["lora_init_done"] = g_loraInitDone ? 1 : 0;
  appendLive(doc.to<JsonObject>());
  appendConfig(doc["cfg"].to<JsonObject>());
  appendController(doc["controller"].to<JsonObject>());
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiConfigGet() {
  StaticJsonDocument<1024> doc;
  appendConfig(doc.to<JsonObject>());
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiConfigPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"missing body\"}");
    return;
  }
  StaticJsonDocument<1536> doc;
  DeserializationError err = deserializeJson(doc, server.arg("plain"));
  if (err) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"json\"}");
    return;
  }
  Cfg next = cfg;
  if (!doc["devEui"].isNull()) copyUpperNoSpace(next.devEui, sizeof(next.devEui), doc["devEui"].as<const char*>());
  if (!doc["appEui"].isNull()) copyUpperNoSpace(next.appEui, sizeof(next.appEui), doc["appEui"].as<const char*>());
  if (!doc["appKey"].isNull()) copyUpperNoSpace(next.appKey, sizeof(next.appKey), doc["appKey"].as<const char*>());
  if (!doc["minutes"].isNull()) next.intervalMin = doc["minutes"].as<uint16_t>();
  if (!doc["adr"].isNull()) next.adr = doc["adr"].as<bool>();
  if (!doc["dr"].isNull()) next.dr = doc["dr"].as<uint8_t>();
  if (!doc["dms_enabled"].isNull()) next.dmsEnabled = doc["dms_enabled"].as<int>() != 0;
  if (!doc["dms_hz"].isNull()) next.dmsHz = doc["dms_hz"].as<float>();
  if (!doc["dms_k_factor"].isNull()) next.dmsKFactor = doc["dms_k_factor"].as<float>();
  if (!doc["ain2_enabled"].isNull()) next.ain2Enabled = doc["ain2_enabled"].as<int>() != 0;
  if (!doc["ain2_hz"].isNull()) next.ain2Hz = doc["ain2_hz"].as<float>();
  if (!doc["ain2_mode"].isNull()) {
    const char* m = doc["ain2_mode"].as<const char*>();
    next.ain2Mode = (m && strcmp(m, "volt") == 0) ? AIN2_MODE_VOLT : AIN2_MODE_POT;
  }
  if (!doc["ain2_length_mm"].isNull()) next.ain2LengthMm = doc["ain2_length_mm"].as<float>();
  if (!doc["ain2_adc_fullscale_v"].isNull()) next.ain2AdcFullscaleV = doc["ain2_adc_fullscale_v"].as<float>();
  if (!doc["ain2_input_fullscale_v"].isNull()) next.ain2InputFullscaleV = doc["ain2_input_fullscale_v"].as<float>();
  if (!doc["ain2_volt_display"].isNull()) {
    const char* s = doc["ain2_volt_display"].as<const char*>();
    if (s) strlcpy(next.ain2VoltDisplay, s, sizeof(next.ain2VoltDisplay));
  }
  if (!doc["ain2_scaling_unit"].isNull()) {
    const char* s = doc["ain2_scaling_unit"].as<const char*>();
    if (s) strlcpy(next.ain2ScalingUnit, s, sizeof(next.ain2ScalingUnit));
  }
  if (!doc["ain2_scale_min"].isNull()) next.ain2ScaleMin = doc["ain2_scale_min"].as<float>();
  if (!doc["ain2_scale_max"].isNull()) next.ain2ScaleMax = doc["ain2_scale_max"].as<float>();
  if (!doc["temp_enabled"].isNull()) next.tempEnabled = doc["temp_enabled"].as<int>() != 0;
  if (!doc["sd_log_enabled"].isNull()) next.sdLogEnabled = doc["sd_log_enabled"].as<int>() != 0;
  if (!doc["stream_mode_enabled"].isNull()) next.streamModeEnabled = doc["stream_mode_enabled"].as<int>() != 0;
  if (!doc["sd_base"].isNull()) {
    const char* base = doc["sd_base"].as<const char*>();
    if (base) strlcpy(next.logBaseName, base, sizeof(next.logBaseName));
  }
  if (!doc["sd_rotate_kb"].isNull()) next.logRotateKB = doc["sd_rotate_kb"].as<uint32_t>();
  normalizeCfg(next);
  String error;
  if (!validateCfg(next, &error)) {
    String out = String("{\"ok\":false,\"error\":\"") + error + "\"}";
    server.send(400, "application/json", out);
    return;
  }
  bool reopenSd = (strcmp(next.logBaseName, cfg.logBaseName) != 0) || (next.sdLogEnabled != cfg.sdLogEnabled) || (next.logRotateKB != cfg.logRotateKB) || (next.streamModeEnabled != cfg.streamModeEnabled);
  cfg = next;
  saveCfg();
  applyLoraConfig();
  if (g_measurementsInitialized && cfg.dmsEnabled) restoreDmsRunMode();
  if (g_measurementsRunning) syncSdLogger(true);
  if (!cfg.dmsEnabled) invalidateDms();
  if (!cfg.ain2Enabled) invalidateAin2();
  if (!cfg.tempEnabled) invalidateTemp();
  if (g_mode == RunMode::CONFIG) syncSdLogger(reopenSd);
  StaticJsonDocument<1152> resp;
  resp["ok"] = true;
  appendConfig(resp["cfg"].to<JsonObject>());
  String out;
  serializeJson(resp, out);
  server.send(200, "application/json", out);
}
static void handleApiSelftestPost() {
  if (g_mode == RunMode::CONFIG && !g_measurementsInitialized) {
    adsInit();
    dsInit();
    g_measurementsInitialized = true;
  }
  bool resultOk = runSelfTest();
  StaticJsonDocument<512> doc;
  doc["ok"] = true;
  doc["result_ok"] = resultOk ? 1 : 0;
  doc["selftest_ok"] = selfTestOk ? 1 : 0;
  doc["selftest_value"] = selfTestDelta_mV;
  doc["selftest_delta_mV"] = selfTestDelta_mV;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiOledPost() {
  StaticJsonDocument<384> req;
  bool wantEnabled = !g_oledEnabled;
  if (server.hasArg("plain")) {
    String body = server.arg("plain");
    if (body.length() > 0) {
      DeserializationError err = deserializeJson(req, body);
      if (!err) {
        if (!req["enabled"].isNull()) wantEnabled = req["enabled"].as<int>() != 0;
        else if (!req["on"].isNull()) wantEnabled = req["on"].as<int>() != 0;
        else if (!req["active"].isNull()) wantEnabled = req["active"].as<int>() != 0;
        else if (!req["action"].isNull()) {
          const char* action = req["action"].as<const char*>();
          if (action && strcmp(action, "toggle") == 0) wantEnabled = !g_oledEnabled;
          else if (action && (strcmp(action, "on") == 0 || strcmp(action, "enable") == 0 || strcmp(action, "show") == 0)) wantEnabled = true;
          else if (action && (strcmp(action, "off") == 0 || strcmp(action, "disable") == 0 || strcmp(action, "hide") == 0)) wantEnabled = false;
        }
      }
    }
  }

  g_oledEnabled = wantEnabled;
  bool startedNow = false;

  if (!oledAllowedInCurrentMode()) {
    oledSleep();
  } else if (g_oledEnabled) {
    startedNow = startMeasurementsManual();
    oledEnsureInit();
    oledUpdateLive(true);
  } else {
    oledSleep();
  }

  StaticJsonDocument<256> doc;
  doc["ok"] = true;
  doc["supported"] = oledAllowedInCurrentMode() ? 1 : 0;
  doc["enabled"] = g_oledEnabled ? 1 : 0;
  doc["active"] = oledIsActiveNow() ? 1 : 0;
  doc["measurements_running"] = g_measurementsRunning ? 1 : 0;
  doc["measurements_initialized"] = g_measurementsInitialized ? 1 : 0;
  doc["started_now"] = startedNow ? 1 : 0;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiReset() {
  server.send(200, "application/json", "{\"ok\":true}");
  delay(200);
  ESP.restart();
}
static void handleApiDevEuiGet() {
  StaticJsonDocument<384> doc;
  char mac48[13];
  char chipHex[17];
  chipMac48Hex(mac48);
  chipDevEuiHex(chipHex);
  doc["activeLoRaDevEUI"] = cfg.devEui;
  doc["chipMac48"] = mac48;
  doc["chipEUI"] = chipHex;
  doc["storedDevEUI"] = cfg.devEui;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiDevEuiPost() {
  if (!server.hasArg("plain")) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"missing body\"}");
    return;
  }
  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, server.arg("plain"));
  if (err) {
    server.send(400, "application/json", "{\"ok\":false,\"error\":\"json\"}");
    return;
  }
  Cfg next = cfg;
  if (!doc["use"].isNull()) {
    const char* use = doc["use"].as<const char*>();
    if (use && strcmp(use, "chip") == 0) {
      char chipHex[17];
      chipDevEuiHex(chipHex);
      strlcpy(next.devEui, chipHex, sizeof(next.devEui));
    }
  }
  if (!doc["devEui"].isNull()) copyUpperNoSpace(next.devEui, sizeof(next.devEui), doc["devEui"].as<const char*>());
  normalizeCfg(next);
  String error;
  if (!validateCfg(next, &error)) {
    String out = String("{\"ok\":false,\"error\":\"") + error + "\"}";
    server.send(400, "application/json", out);
    return;
  }
  cfg = next;
  saveCfg();
  applyLoraConfig();
  handleApiDevEuiGet();
}

static void handleApiStreamGet() {
  StaticJsonDocument<512> doc;
  doc["ok"] = true;
  doc["enabled"] = cfg.streamModeEnabled ? 1 : 0;
  doc["status"] = g_streamStatus;
  doc["port"] = STREAM_TCP_PORT;
  doc["ssid"] = g_apSsid;
  doc["ip"] = g_apIp.toString();
  doc["client_connected"] = (g_streamClient && g_streamClient.connected()) ? 1 : 0;
  doc["clients_total"] = g_streamClientCount;
  doc["streamed_seq"] = g_lastStreamedSampleSeq;
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}
static void handleApiStreamPost() {
  if (!server.hasArg("plain")) {
    sendJsonError(400, "missing body");
    return;
  }
  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, server.arg("plain"));
  if (err) {
    sendJsonError(400, "json");
    return;
  }
  Cfg next = cfg;
  if (!doc["enabled"].isNull()) next.streamModeEnabled = doc["enabled"].as<int>() != 0;
  normalizeCfg(next);
  bool streamChanged = (next.streamModeEnabled != cfg.streamModeEnabled);
  cfg = next;
  saveCfg();

  if (cfg.streamModeEnabled && g_mode == RunMode::CONFIG) {
    if (!g_measurementsInitialized) {
      adsInit();
      dsInit();
      if (cfg.dmsEnabled) {
        restoreDmsRunMode();
      }
      g_measurementsInitialized = true;
    }
    ensureMeasurementTasksStarted();
    g_measurementsRunning = true;
  }

  syncSdLogger(true);
  if (streamChanged || cfg.streamModeEnabled) {
    resetStreamBuffer();
  }
  if (!cfg.streamModeEnabled) {
    closeStreamClient();
    g_streamStatus = "disabled";
  } else {
    g_streamStatus = "waiting_client";
  }
  handleApiStreamGet();
}
static bool shouldLogHttpVerbose() {
  if (g_mode != RunMode::CONFIG) return false;
  const uint32_t now = millis();
  return (g_diagBootMs != 0 && (now - g_diagBootMs) <= 180000UL);
}

static void logHttpEvent(const char* tag, const String& uri, uint32_t startedMs, const String& extra = "") {
  if (!shouldLogHttpVerbose()) return;
  const uint32_t now = millis();
  const uint32_t dt = now - startedMs;
  ++g_httpReqCount;
  if (g_httpReqWindowStartedMs == 0 || (now - g_httpReqWindowStartedMs) >= 1000UL) {
    g_httpReqWindowStartedMs = now;
    g_httpReqWindowCount = 0;
  }
  ++g_httpReqWindowCount;
  if (dt > g_httpMaxDurationMs) {
    g_httpMaxDurationMs = dt;
    g_httpMaxDurationUri = uri;
  }
  if (dt >= 200UL) ++g_httpSlowReqCount;
  Serial.printf("[HTTP][%10lu ms][%s] %s dt=%lu ms heap=%u win=%lu host=%s ua=%s%s%s\n",
                now, tag, uri.c_str(), dt, ESP.getFreeHeap(),
                g_httpReqWindowCount, server.hostHeader().c_str(),
                server.header("User-Agent").c_str(),
                extra.length() ? " " : "", extra.c_str());
}

static void logLoopStatsIfNeeded() {
  if (g_mode != RunMode::CONFIG) return;
  if (!shouldLogHttpVerbose()) return;
  const uint32_t now = millis();
  if (g_diagLastLoopStatsMs == 0 || (now - g_diagLastLoopStatsMs) >= 5000UL) {
    Serial.printf("[DIAG][%10lu ms] heap=%u minHeap=%u wifiClients=%d httpTotal=%lu slow=%lu max=%lu uri=%s stream=%s\n",
                  now, ESP.getFreeHeap(), ESP.getMinFreeHeap(), WiFi.softAPgetStationNum(),
                  g_httpReqCount, g_httpSlowReqCount, g_httpMaxDurationMs,
                  g_httpMaxDurationUri.c_str(), g_streamStatus.c_str());
    g_diagLastLoopStatsMs = now;
  }
}

bool serveFile(const char* path, const char* mime) {
  const uint32_t startedMs = millis();
  if (!LittleFS.exists(path)) {
    logHttpEvent("MISS", String(path), startedMs, "fs=missing");
    return false;
  }
  File f = LittleFS.open(path, "r");
  if (!f) {
    logHttpEvent("MISS", String(path), startedMs, "fs=open_failed");
    return false;
  }
  const size_t size = f.size();
  server.streamFile(f, mime);
  f.close();
  logHttpEvent("FILE", String(path), startedMs, String("bytes=") + String((uint32_t)size) + " mime=" + String(mime));
  return true;
}

static bool isIpv4Host(const String& host) {
  if (!host.length()) return false;
  for (size_t i = 0; i < host.length(); ++i) {
    const char c = host[i];
    if (!((c >= '0' && c <= '9') || c == '.')) return false;
  }
  return true;
}
static String mimeFromPath(const String& path) {
  String p = path;
  p.toLowerCase();
  if (p.endsWith(".html") || p.endsWith(".htm")) return "text/html";
  if (p.endsWith(".css")) return "text/css";
  if (p.endsWith(".js")) return "application/javascript";
  if (p.endsWith(".json")) return "application/json";
  if (p.endsWith(".svg")) return "image/svg+xml";
  if (p.endsWith(".png")) return "image/png";
  if (p.endsWith(".jpg") || p.endsWith(".jpeg")) return "image/jpeg";
  if (p.endsWith(".ico")) return "image/x-icon";
  if (p.endsWith(".txt")) return "text/plain";
  return "application/octet-stream";
}
static void redirectToRoot() {
  const uint32_t startedMs = millis();
  String target = String("http://") + g_apIp.toString() + "/";
  server.sendHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  server.sendHeader("Pragma", "no-cache");
  server.sendHeader("Expires", "-1");
  server.sendHeader("Location", target, true);
  server.send(302, "text/plain", "");
  logHttpEvent("REDIR", server.uri(), startedMs, String("to=") + target);
}

static void handleWindowsConnectTest() {
  const uint32_t startedMs = millis();
  server.sendHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  server.sendHeader("Pragma", "no-cache");
  server.sendHeader("Expires", "-1");
  server.send(200, "text/plain", "Microsoft Connect Test");
  logHttpEvent("WIN", server.uri(), startedMs, "probe=connecttest");
}

static void handleWindowsNcsi() {
  const uint32_t startedMs = millis();
  server.sendHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  server.sendHeader("Pragma", "no-cache");
  server.sendHeader("Expires", "-1");
  server.send(200, "text/plain", "Microsoft NCSI");
  logHttpEvent("WIN", server.uri(), startedMs, "probe=ncsi");
}

static void handleNotFound() {
  const uint32_t startedMs = millis();
  const String uri = server.uri();
  if (uri.startsWith("/api/")) {
    sendJsonError(404, "not_found");
    logHttpEvent("API404", uri, startedMs);
    return;
  }
  if (LittleFS.exists(uri)) {
    const String mime = mimeFromPath(uri);
    if (serveFile(uri.c_str(), mime.c_str())) return;
  }
  const String host = server.hostHeader();
  if (g_mode == RunMode::CONFIG && uri == "/connecttest.txt") {
    redirectToRoot();
    return;
  }
  if (g_mode == RunMode::CONFIG && uri == "/ncsi.txt") {
    redirectToRoot();
    return;
  }
  if (g_mode == RunMode::CONFIG && uri == "/wpad.dat") {
    static const char pac[] =
        "function FindProxyForURL(url, host) { return \"DIRECT\"; }\r\n";
    server.send(200, "application/x-ns-proxy-autoconfig", pac);
    logHttpEvent("WPAD", uri, startedMs, "mode=direct");
    return;
  }
  if (g_mode == RunMode::CONFIG && uri == "/cname.aspx") {
    server.send(204, "text/plain", "");
    logHttpEvent("CNAME", uri, startedMs, "mode=204");
    return;
  }
  const bool captiveProbe =
      uri == "/generate_204" ||
      uri == "/gen_204" ||
      uri == "/hotspot-detect.html" ||
      uri == "/library/test/success.html" ||
      uri == "/success.txt" ||
      uri == "/redirect" ||
      uri == "/canonical.html" ||
      (!host.isEmpty() && !isIpv4Host(host) && host != g_apIp.toString() && host != "lorasense.local");
  if (g_mode == RunMode::CONFIG && captiveProbe) {
    redirectToRoot();
    return;
  }
  if (g_mode == RunMode::CONFIG) {
    server.send(404, "text/plain", "not found");
    logHttpEvent("404", uri, startedMs, "config-drop");
    return;
  }
  server.send(404, "text/plain", "not found");
  logHttpEvent("404", uri, startedMs);
}
static void handleRoot() {
  const uint32_t startedMs = millis();
  if (serveFile("/index.html", "text/html")) {
    logHttpEvent("ROOT", server.uri(), startedMs, "source=fs");
    return;
  }
  server.send(200, "text/html",
              "<html><body><h1>MKP LoRaSense</h1>"
              "<p>Filesystem missing /index.html</p></body></html>");
  logHttpEvent("ROOT", server.uri(), startedMs, "source=fallback_html");
}

static void startApAndWeb() {
  if (!LittleFS.begin(true)) {
    Serial.println("[FS] LittleFS mount failed");
  }
  g_apSsid = String("LoRaSense-") + String(&cfg.devEui[8]);
  WiFi.mode(WIFI_AP);
  WiFi.softAP(g_apSsid.c_str());
  g_apIp = WiFi.softAPIP();
  g_diagBootMs = millis();
  g_diagLastLoopStatsMs = 0;
  g_httpReqCount = 0;
  g_httpReqWindowCount = 0;
  g_httpReqWindowStartedMs = 0;
  g_httpSlowReqCount = 0;
  g_httpMaxDurationMs = 0;
  g_httpMaxDurationUri = "";
  dnsServer.stop();
  dnsServer.setErrorReplyCode(DNSReplyCode::NoError);
  dnsServer.start(DNS_PORT, "*", g_apIp);
  Serial.printf("[AP] SSID=%s IP=%s heap=%u\n", g_apSsid.c_str(), g_apIp.toString().c_str(), ESP.getFreeHeap());

  const char* headerKeys[] = {"User-Agent", "Host"};
  server.collectHeaders(headerKeys, 2);
  server.on("/", HTTP_GET, handleRoot);
  server.on("/index.html", HTTP_GET, handleRoot);
  server.on("/generate_204", HTTP_GET, redirectToRoot);
  server.on("/gen_204", HTTP_GET, redirectToRoot);
  server.on("/hotspot-detect.html", HTTP_GET, redirectToRoot);
  server.on("/library/test/success.html", HTTP_GET, redirectToRoot);
  server.on("/connecttest.txt", HTTP_GET, redirectToRoot);
  server.on("/ncsi.txt", HTTP_GET, redirectToRoot);
  server.on("/wpad.dat", HTTP_GET, []() {
    static const char pac[] =
        "function FindProxyForURL(url, host) { return \"DIRECT\"; }\r\n";
    server.send(200, "application/x-ns-proxy-autoconfig", pac);
  });
  server.on("/cname.aspx", HTTP_GET, []() {
    server.send(204, "text/plain", "");
  });
  server.on("/success.txt", HTTP_GET, redirectToRoot);
  server.on("/redirect", HTTP_GET, redirectToRoot);
  server.on("/canonical.html", HTTP_GET, redirectToRoot);

  server.on("/i18n.json", HTTP_GET, []() {
    if (!serveFile("/i18n.json", "application/json")) server.send(404, "text/plain", "not found");
  });
  server.on("/logo.svg", HTTP_GET, []() {
    if (!serveFile("/logo.svg", "image/svg+xml")) server.send(404, "text/plain", "not found");
  });

  server.on("/lorasense_qr.png", HTTP_GET, []() {
    if (!serveFile("/lorasense_qr.png", "image/png")) server.send(404, "text/plain", "not found");
  });
  server.on("/api/live", HTTP_GET, handleApiLive);
  server.on("/api/state", HTTP_GET, handleApiState);
  server.on("/api/config", HTTP_GET, handleApiConfigGet);
  server.on("/api/config", HTTP_POST, handleApiConfigPost);
  server.on("/api/selftest", HTTP_POST, handleApiSelftestPost);
  server.on("/api/measure", HTTP_POST, handleApiMeasureControlPost);
  server.on("/api/oled", HTTP_POST, handleApiOledPost);
  server.on("/api/reset", HTTP_POST, handleApiReset);
  server.on("/api/deveui", HTTP_GET, handleApiDevEuiGet);
  server.on("/api/deveui", HTTP_POST, handleApiDevEuiPost);
  server.on("/api/stream", HTTP_GET, handleApiStreamGet);
  server.on("/api/stream", HTTP_POST, handleApiStreamPost);
  server.on("/api/sd/list", HTTP_GET, handleApiSdList);
  server.on("/api/sd/download", HTTP_GET, handleApiSdDownload);
  server.on("/api/sd/archive", HTTP_GET, handleApiSdArchive);
  server.on("/api/sd/convert_bin_csv", HTTP_GET, handleApiSdConvertBinCsv);
  server.on("/api/sd/delete", HTTP_POST, handleApiSdDelete);
  server.onNotFound(handleNotFound);
  server.begin();
  g_streamServer.begin();
  g_streamServer.setNoDelay(true);
  g_streamServerStarted = true;
  resetStreamBuffer();
  g_streamStatus = cfg.streamModeEnabled ? "waiting_client" : "disabled";
  syncSdLogger(true);
  Serial.print("[AP] SSID=");
  Serial.print(g_apSsid);
  Serial.print(" IP=");
  Serial.println(g_apIp);
}
// ============================================================
// LoRa
// ============================================================
static void prepareTxFrame(uint8_t port) {
  (void)port;
  StaticJsonDocument<384> doc;
  doc["dms_mV"] = dms_valid ? dms_mV : 0.0f;
  doc["dms_mV_per_V"] = dms_valid ? dms_mV_per_V : 0.0f;
  doc["ain2_mV"] = ain2_valid ? ain2_mV : 0.0f;
  if (cfg.ain2Mode == AIN2_MODE_VOLT) doc["ain2_V"] = ain2_valid ? ain2DerivedValue() : 0.0f;
  else doc["ain2_mm"] = ain2_valid ? ain2DerivedValue() : 0.0f;
  doc["temp_C"] = temp_valid ? ds_temp_c : 0.0f;
  doc["frames_s"] = framesPerSecond;
  doc["selftest_ok"] = selfTestOk ? 1 : 0;
  appDataSize = (uint8_t)serializeJson(doc, appData, LORAWAN_APP_DATA_MAX_SIZE);
}
static void loraLoop() {
  if (!g_loraEnabled) return;
  switch (deviceState) {
    case DEVICE_STATE_INIT:
      Serial.println("[LORA] INIT");
      syncLoraCompatAliases();
      dumpLoraBuffers("buffers before LoRaWAN.init");
      LoRaWAN.init(loraWanClass, loraWanRegion);
      LoRaWAN.setDefaultDR(cfg.dr);
      g_loraInitDone = true;
      deviceState = DEVICE_STATE_JOIN;
      break;
    case DEVICE_STATE_JOIN:
      Serial.println("[LORA] JOIN");
      g_radioQuietUntilMs = millis() + 7000UL;
      LoRaWAN.join();
      break;
    case DEVICE_STATE_SEND:
      Serial.println("[LORA] SEND");
      captureFieldSnapshot();
      oledUpdateLive(true);
      prepareTxFrame(appPort);
      g_radioQuietUntilMs = millis() + 3000UL;
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
}
// ============================================================
// Setup / loop
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(300);
  g_diagBootMs = millis();
  g_spiBusMutex = xSemaphoreCreateMutex();
  loadCfg();
  applyLoraConfig();
  g_mode = isApModeRequested() ? RunMode::CONFIG : RunMode::FIELD;
  if (!oledIsActiveNow()) oledSleep();
  if (g_mode == RunMode::CONFIG) {
    g_loraEnabled = false;
    g_sdStatus = cfg.sdLogEnabled ? "ready" : "disabled";
    oledShowBootLogo();
    delay(1500);
    oledShowBanner("CONFIG MODE", "Manual measurement start");
    startApAndWeb();
    oledShowBanner("CONFIG MODE", g_apSsid);
    if (g_oledEnabled && oledAllowedInCurrentMode()) {
      startMeasurementsManual();
      oledEnsureInit();
      oledUpdateLive(true);
    }
  } else {
    adsInit();
    dsInit();
    runSelfTest();
    if (cfg.dmsEnabled) restoreDmsRunMode();
  }
  if (g_mode == RunMode::FIELD) {
    oledSleep();
  }
  last_dms_read_us = micros();
  last_ain2_read_us = micros();
  Serial.print("FW ");
  Serial.print(FW_VERSION);
  Serial.print(" build ");
  Serial.print(__DATE__);
  Serial.print(" ");
  Serial.println(__TIME__);
  Serial.print("Mode: ");
  Serial.println(modeToString());
  Serial.print("DevEUI=");
  Serial.print(cfg.devEui);
  Serial.print(" AppEUI=");
  Serial.print(cfg.appEui);
  Serial.print(" AppKey=");
  Serial.println(cfg.appKey);
  Serial.print("LoRa interval: ");
  Serial.print(appTxDutyCycle);
  Serial.println(" ms");
  Serial.println("[ADS] software SPI on fixed PCB pins; LoRa SPI isolated");
  dumpLoraBuffers("buffers before Mcu.begin");
  if (g_mode == RunMode::FIELD) {
    WiFi.persistent(false);
    WiFi.disconnect(true, true);
    WiFi.mode(WIFI_OFF);
    delay(20);
    Mcu.begin(HELTEC_BOARD, SLOW_CLK_TPYE);
    g_loraEnabled = true;
    g_sdStatus = "field_disabled";
  } else {
    ensureMeasurementTasksStarted();
    g_measurementsRunning = false;
    oledUpdateLive(true);
  }
}
void loop() {
  if (g_mode == RunMode::CONFIG) {
    dnsServer.processNextRequest();
    server.handleClient();
    logLoopStatsIfNeeded();
    delay(cfg.streamModeEnabled ? 5 : 1);
  } else {
    loraLoop();
    delay(2);
  }
}
