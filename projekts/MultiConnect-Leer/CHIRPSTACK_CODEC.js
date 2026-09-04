/*
 * ChirpStack uplink codec for MultiConnect-Leer Payload V5.
 * fPort 2, 12 bytes, big-endian.
 * The uplink contains only battery percent, battery voltage, position,
 * Wegsensor raw value, and temperature.
 */

function readInt16BE(bytes, offset) {
  var value = (bytes[offset] << 8) | bytes[offset + 1];
  return (value & 0x8000) ? value - 0x10000 : value;
}

function readUInt16BE(bytes, offset) {
  return (bytes[offset] << 8) | bytes[offset + 1];
}

function readInt32BE(bytes, offset) {
  var value =
    bytes[offset] * 0x1000000 +
    bytes[offset + 1] * 0x10000 +
    bytes[offset + 2] * 0x100 +
    bytes[offset + 3];

  return value >= 0x80000000
    ? value - 0x100000000
    : value;
}

function decodeUplink(input) {
  if (!input || input.fPort !== 2) {
    return {
      errors: ["Unsupported fPort: " + (input ? input.fPort : "undefined")]
    };
  }

  var bytes = input.bytes || [];

  if (!Array.isArray(bytes) || bytes.length !== 12) {
    return {
      errors: [
        "Invalid Payload V5 length: " +
        bytes.length +
        ", expected 12"
      ]
    };
  }

  var version = bytes[0];
  if (version !== 5) {
    return {
      errors: ["Unsupported MultiConnect-Leer protocol version: " + version]
    };
  }

  var batteryPercentRaw = bytes[1];
  var batteryMillivolts = readUInt16BE(bytes, 2);
  var positionRaw = readUInt16BE(bytes, 4);
  var wegRaw = readInt32BE(bytes, 6);
  var temperatureRaw = readInt16BE(bytes, 10);

  return {
    data: {
      battery_percent:
        batteryPercentRaw !== 0xFF
          ? batteryPercentRaw
          : null,
      battery_voltage_v:
        batteryMillivolts !== 0xFFFF
          ? batteryMillivolts / 1000.0
          : null,
      position_mm:
        positionRaw !== 0xFFFF
          ? positionRaw / 1000.0
          : null,
      weg_raw:
        wegRaw !== 0x7FFFFFFF
          ? wegRaw
          : null,
      temperature_c:
        temperatureRaw !== 0x7FFF
          ? temperatureRaw / 100.0
          : null
    }
  };
}
