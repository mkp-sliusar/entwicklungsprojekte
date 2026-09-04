/*
 * MKP MultiConnect ChirpStack uplink decoder.
 *
 * V5 payload:
 *   byte 0       payload version (5)
 *   byte 1       status flags
 *   bytes 2..5  selected-field mask, big-endian, bits 0..30
 *   remaining   selected values in ascending field-id order
 *
 * The field mask makes every value optional. A selected value with its
 * reserved sentinel is returned as null when the sensor is unavailable.
 *
 * V6 payload keeps the V5 fields and adds:
 *   bytes 6..7  SISGEO device mask, big-endian, bits 0..11
 *   per selected device: one field mask byte and selected float32 values
 */

var MULTICONNECT_PAYLOAD_VERSION = 5;
var MULTICONNECT_PAYLOAD_VERSION_V6 = 6;

var MULTICONNECT_FIELDS = [
  { key: "temperature_c", type: "i16", bytes: 2, scale: 100, sentinel: 32767 },
  { key: "position_mm", type: "u16", bytes: 2, scale: 1000, sentinel: 65535 },
  { key: "weg_raw", type: "i32", bytes: 4, scale: 1, sentinel: 2147483647 },
  { key: "ina_bus_v", type: "u16", bytes: 2, scale: 1000, sentinel: 65535 },
  { key: "ina_shunt_mv", type: "i16", bytes: 2, scale: 1000, sentinel: 32767 },
  { key: "ina_current_ma", type: "i32", bytes: 4, scale: 1, sentinel: 2147483647 },
  { key: "ina_power_w", type: "i32", bytes: 4, scale: 1000, sentinel: 2147483647 },
  { key: "battery_v", type: "u16", bytes: 2, scale: 1000, sentinel: 65535 },
  { key: "battery_percent", type: "u8", bytes: 1, scale: 1, sentinel: 255 },
  { key: "air_temperature_c", type: "i16", bytes: 2, scale: 100, sentinel: 32767 },
  { key: "humidity_pct", type: "u16", bytes: 2, scale: 100, sentinel: 65535 },
  { key: "pressure_pa", type: "u32", bytes: 4, scale: 1, sentinel: 4294967295 },
  { key: "light_lux", type: "u32", bytes: 4, scale: 1, sentinel: 4294967295 },
  { key: "min_wind_direction_deg", type: "u16", bytes: 2, scale: 10, sentinel: 65535 },
  { key: "max_wind_direction_deg", type: "u16", bytes: 2, scale: 10, sentinel: 65535 },
  { key: "average_wind_direction_deg", type: "u16", bytes: 2, scale: 10, sentinel: 65535 },
  { key: "min_wind_speed_ms", type: "u16", bytes: 2, scale: 100, sentinel: 65535 },
  { key: "max_wind_speed_ms", type: "u16", bytes: 2, scale: 100, sentinel: 65535 },
  { key: "average_wind_speed_ms", type: "u16", bytes: 2, scale: 100, sentinel: 65535 },
  { key: "accumulated_rainfall_mm", type: "u32", bytes: 4, scale: 100, sentinel: 4294967295 },
  { key: "accumulated_rainfall_duration_s", type: "u32", bytes: 4, scale: 1, sentinel: 4294967295 },
  { key: "rain_intensity_mm_h", type: "u16", bytes: 2, scale: 100, sentinel: 65535 },
  { key: "max_rain_intensity_mm_h", type: "u16", bytes: 2, scale: 100, sentinel: 65535 },
  { key: "heating_temperature_c", type: "i16", bytes: 2, scale: 100, sentinel: 32767 },
  { key: "tilt_state", type: "u8", bytes: 1, scale: 1, sentinel: 255 },
  { key: "pm25_ug_m3", type: "u32", bytes: 4, scale: 1, sentinel: 4294967295 },
  { key: "pm10_ug_m3", type: "u32", bytes: 4, scale: 1, sentinel: 4294967295 },
  { key: "co2_ppm", type: "u16", bytes: 2, scale: 1, sentinel: 65535 },
  { key: "noise_db", type: "u16", bytes: 2, scale: 10, sentinel: 65535 },
  { key: "solar_radiation_wm2", type: "u32", bytes: 4, scale: 1, sentinel: 4294967295 },
  { key: "sunshine_duration_h", type: "u32", bytes: 4, scale: 10, sentinel: 4294967295 }
];

var MULTICONNECT_SISGEO_FIELDS = [
  { key: "value1", label: "Value 1" },
  { key: "value2", label: "Value 2" },
  { key: "temperature_c", label: "Sensor temperature" },
  { key: "humidity_pct", label: "Sensor humidity" },
  { key: "supply_voltage_v", label: "Supply voltage" }
];

function readU16(bytes, offset) {
  return bytes[offset] * 256 + bytes[offset + 1];
}

function readI16(bytes, offset) {
  var value = readU16(bytes, offset);
  return value >= 32768 ? value - 65536 : value;
}

function readU32(bytes, offset) {
  return bytes[offset] * 16777216 +
    bytes[offset + 1] * 65536 +
    bytes[offset + 2] * 256 +
    bytes[offset + 3];
}

function readI32(bytes, offset) {
  var value = readU32(bytes, offset);
  return value >= 2147483648 ? value - 4294967296 : value;
}

function readF32(bytes, offset) {
  var bits = readU32(bytes, offset);
  var sign = (bits >>> 31) === 0 ? 1 : -1;
  var exponent = (bits >>> 23) & 0xFF;
  var fraction = bits & 0x7FFFFF;

  if (exponent === 0xFF) return null;
  if (exponent === 0) {
    return sign * (fraction / 8388608) * Math.pow(2, -126);
  }
  return sign * (1 + fraction / 8388608) * Math.pow(2, exponent - 127);
}

function readTypedValue(bytes, offset, field) {
  if (field.type === "u8") return bytes[offset];
  if (field.type === "u16") return readU16(bytes, offset);
  if (field.type === "i16") return readI16(bytes, offset);
  if (field.type === "u32") return readU32(bytes, offset);
  if (field.type === "i32") return readI32(bytes, offset);
  return null;
}

function statusObject(status) {
  return {
    local_temperature_valid: (status & 1) !== 0,
    local_position_valid: (status & 2) !== 0,
    local_weg_valid: (status & 4) !== 0,
    ina_valid: (status & 8) !== 0,
    battery_valid: (status & 16) !== 0,
    modbus_valid: (status & 32) !== 0,
    modbus_complete: (status & 64) !== 0
  };
}

function decodeV5(bytes) {
  var errors = [];
  var warnings = [];

  if (bytes.length < 6) {
    return { errors: ["MultiConnect V5 payload must contain at least 6 bytes"] };
  }

  var status = bytes[1];
  var mask = readU32(bytes, 2);
  var offset = 6;
  var data = {
    payload_version: bytes[0],
    field_mask: mask,
    selected_fields: [],
    status: statusObject(status)
  };

  for (var fieldId = 0; fieldId < MULTICONNECT_FIELDS.length; fieldId += 1) {
    var field = MULTICONNECT_FIELDS[fieldId];
    var selected = (mask & Math.pow(2, fieldId)) !== 0;
    if (!selected) continue;

    data.selected_fields.push(field.key);
    if (offset + field.bytes > bytes.length) {
      errors.push("Truncated value for field " + field.key);
      data[field.key] = null;
      break;
    }

    var raw = readTypedValue(bytes, offset, field);
    offset += field.bytes;
    data[field.key] = raw === field.sentinel ? null : raw / field.scale;
  }

  if (data.tilt_state !== null && data.tilt_state !== undefined) {
    data.tilt_state_text = data.tilt_state === 0
      ? "upright"
      : data.tilt_state === 1
        ? "tilted"
        : "unknown";
  }

  if (offset < bytes.length) {
    warnings.push("Payload contains " + (bytes.length - offset) + " trailing byte(s)");
  }

  var result = { data: data };
  if (errors.length) result.errors = errors;
  if (warnings.length) result.warnings = warnings;
  return result;
}

function decodeV6(bytes) {
  var errors = [];
  var warnings = [];

  if (bytes.length < 8) {
    return { errors: ["MultiConnect V6 payload must contain at least 8 bytes"] };
  }

  var status = bytes[1];
  var mask = readU32(bytes, 2);
  var sisgeoDeviceMask = readU16(bytes, 6);
  var offset = 8;
  var data = {
    payload_version: bytes[0],
    field_mask: mask,
    sisgeo_device_mask: sisgeoDeviceMask,
    selected_fields: [],
    sisgeo_devices: [],
    status: statusObject(status)
  };

  for (var baseFieldId = 0; baseFieldId < MULTICONNECT_FIELDS.length; baseFieldId += 1) {
    var baseField = MULTICONNECT_FIELDS[baseFieldId];
    if ((mask & Math.pow(2, baseFieldId)) === 0) continue;

    data.selected_fields.push(baseField.key);
    if (offset + baseField.bytes > bytes.length) {
      errors.push("Truncated value for field " + baseField.key);
      data[baseField.key] = null;
      return { data: data, errors: errors };
    }

    var baseRaw = readTypedValue(bytes, offset, baseField);
    offset += baseField.bytes;
    data[baseField.key] = baseRaw === baseField.sentinel ? null : baseRaw / baseField.scale;
  }

  for (var deviceIndex = 0; deviceIndex < 12; deviceIndex += 1) {
    if ((sisgeoDeviceMask & Math.pow(2, deviceIndex)) === 0) continue;
    if (offset >= bytes.length) {
      errors.push("Missing field mask for SISGEO device " + deviceIndex);
      break;
    }

    var fieldMask = bytes[offset++];
    var device = {
      device_index: deviceIndex,
      field_mask: fieldMask,
      selected_fields: []
    };
    for (var fieldId = 0; fieldId < MULTICONNECT_SISGEO_FIELDS.length; fieldId += 1) {
      if ((fieldMask & Math.pow(2, fieldId)) === 0) continue;
      var field = MULTICONNECT_SISGEO_FIELDS[fieldId];
      if (offset + 4 > bytes.length) {
        errors.push("Truncated value for SISGEO device " + deviceIndex + " field " + field.key);
        device[field.key] = null;
        break;
      }
      device.selected_fields.push(field.key);
      device[field.key] = readF32(bytes, offset);
      offset += 4;
    }
    data.sisgeo_devices.push(device);
  }

  if (data.tilt_state !== null && data.tilt_state !== undefined) {
    data.tilt_state_text = data.tilt_state === 0
      ? "upright"
      : data.tilt_state === 1
        ? "tilted"
        : "unknown";
  }
  if (offset < bytes.length) {
    warnings.push("Payload contains " + (bytes.length - offset) + " trailing byte(s)");
  }

  var result = { data: data };
  if (errors.length) result.errors = errors;
  if (warnings.length) result.warnings = warnings;
  return result;
}

function decodeV4(bytes) {
  if (bytes.length < 25) {
    return { errors: ["MultiConnect V4 payload must contain at least 25 bytes"] };
  }

  var status = bytes[1];
  var data = {
    payload_version: 4,
    legacy_payload: true,
    status: {
      local_temperature_valid: (status & 1) !== 0,
      local_position_valid: (status & 2) !== 0,
      ina_valid: (status & 4) !== 0,
      battery_valid: (status & 8) !== 0
    },
    temperature_c: (status & 1) !== 0 ? readI16(bytes, 2) / 100 : null,
    position_mm: (status & 2) !== 0 ? readU16(bytes, 4) / 1000 : null,
    weg_raw: (status & 2) !== 0 ? readI32(bytes, 6) : null,
    ina_bus_v: (status & 4) !== 0 ? readU16(bytes, 10) / 1000 : null,
    ina_shunt_mv: (status & 4) !== 0 ? readI16(bytes, 12) / 1000 : null,
    ina_current_ma: (status & 4) !== 0 ? readI32(bytes, 14) : null,
    ina_power_w: (status & 4) !== 0 ? readI32(bytes, 18) / 1000 : null,
    battery_v: (status & 8) !== 0 ? readU16(bytes, 22) / 1000 : null,
    battery_percent: (status & 8) !== 0 ? bytes[24] : null
  };

  var result = { data: data };
  if (bytes.length > 25) {
    result.warnings = ["V4 payload contains " + (bytes.length - 25) + " trailing byte(s)"];
  }
  return result;
}

function decodeUplink(input) {
  var bytes = input && input.bytes ? input.bytes : [];
  if (!Array.isArray(bytes)) {
    return { errors: ["input.bytes must be an array"] };
  }
  if (bytes.length === 0) {
    return { errors: ["Payload is empty"] };
  }
  if (bytes[0] === MULTICONNECT_PAYLOAD_VERSION) return decodeV5(bytes);
  if (bytes[0] === MULTICONNECT_PAYLOAD_VERSION_V6) return decodeV6(bytes);
  if (bytes[0] === 4) return decodeV4(bytes);
  return { errors: ["Unsupported MultiConnect payload version: " + bytes[0]] };
}
