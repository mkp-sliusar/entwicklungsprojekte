/*
 * MKP MultiConnect Node-RED Function node
 *
 * Input:
 *   msg.payload = ChirpStack uplink event with decoded msg.payload.object
 *   (V5 selectable payload or legacy V4 payload).
 *
 * Output:
 *   msg.query  = parameterized PostgreSQL INSERT statement
 *   msg.params = values for node-red-contrib-postgresql
 *
 * Run the database migration that adds the Modbus and LoRa V5/V6 columns before
 * deploying this Function node.
 */

let uplink = msg.payload;

if (typeof uplink === "string") {
    try {
        uplink = JSON.parse(uplink);
    } catch (error) {
        node.error("Invalid JSON payload: " + error.message, msg);
        return null;
    }
}

if (!uplink || typeof uplink !== "object" || Array.isArray(uplink)) {
    node.error("Payload is not an object", msg);
    return null;
}

function isObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}

function objectOrEmpty(value) {
    return isObject(value) ? value : {};
}

function firstDefined(...values) {
    for (const value of values) {
        if (value !== null && value !== undefined && value !== "") {
            return value;
        }
    }

    return null;
}

function safeNumber(value) {
    if (value === null || value === undefined || value === "") {
        return null;
    }

    if (typeof value === "string") {
        const normalized = value.trim().replace(",", ".");
        if (normalized === "") {
            return null;
        }

        const number = Number(normalized);
        return Number.isFinite(number) ? number : null;
    }

    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function safeDataRate(value) {
    const numeric = safeNumber(value);
    if (numeric !== null) {
        return numeric;
    }

    if (typeof value === "string") {
        const match = value.trim().match(/^DR\s*(\d+)$/i);
        return match ? safeNumber(match[1]) : null;
    }

    return null;
}

function safeString(value) {
    if (value === null || value === undefined || value === "") {
        return null;
    }

    return String(value);
}

function safeBoolean(value) {
    if (value === null || value === undefined || value === "") {
        return null;
    }

    if (typeof value === "boolean") {
        return value;
    }

    if (typeof value === "string") {
        const normalized = value.trim().toLowerCase();
        if (["true", "yes", "on", "1"].includes(normalized)) {
            return true;
        }
        if (["false", "no", "off", "0"].includes(normalized)) {
            return false;
        }
    }

    if (typeof value === "number") {
        return value !== 0;
    }

    return Boolean(value);
}

function hasValue(value) {
    return value !== null && value !== undefined && value !== "";
}

function valueFrom(decoded, modbus, key, ...aliases) {
    return firstDefined(decoded[key], modbus[key], ...aliases.map(alias => decoded[alias]));
}

function deriveStatusFlags(decoded, status, protocolVersion, valid) {
    const explicit = safeNumber(firstDefined(decoded.status_flags, decoded.status_code));
    if (explicit !== null) {
        return explicit;
    }

    let flags = 0;
    if (valid.temperature) flags |= protocolVersion === 4 ? 1 : 1;
    if (valid.position) flags |= protocolVersion === 4 ? 2 : 2;
    if (protocolVersion !== 4 && valid.weg) flags |= 4;
    if (valid.ina) flags |= protocolVersion === 4 ? 4 : 8;
    if (valid.battery) flags |= protocolVersion === 4 ? 8 : 16;
    if (protocolVersion !== 4 && valid.modbus) flags |= 32;
    if (protocolVersion !== 4 && valid.modbusComplete) flags |= 64;
    return flags;
}

function deriveTiltText(tiltState, explicitText) {
    if (hasValue(explicitText)) {
        return safeString(explicitText);
    }

    const numeric = safeNumber(tiltState);
    if (numeric === null) {
        return null;
    }
    if (numeric === 0) {
        return "upright";
    }
    if (numeric === 1) {
        return "tilted";
    }
    return "unknown";
}

const device = objectOrEmpty(uplink.deviceInfo);
const decodedContainer = objectOrEmpty(uplink.object);
const decoded = isObject(decodedContainer.data) &&
        (decodedContainer.data.payload_version !== undefined || decodedContainer.data.field_mask !== undefined)
    ? decodedContainer.data
    : decodedContainer;
const status = objectOrEmpty(decoded.status);
const modbus = objectOrEmpty(decoded.modbus);
const rx = Array.isArray(uplink.rxInfo) && uplink.rxInfo.length > 0
    ? objectOrEmpty(uplink.rxInfo[0])
    : {};
const tx = objectOrEmpty(uplink.txInfo);
const modulation = objectOrEmpty(tx.modulation);
const lora = objectOrEmpty(modulation.lora);

const timestamp = new Date(uplink.time);
if (Number.isNaN(timestamp.getTime())) {
    node.error("Invalid ChirpStack timestamp: " + uplink.time, msg);
    return null;
}

const protocolVersion = safeNumber(firstDefined(
    decoded.payload_version,
    decoded.protocol_version,
    5
));

const localTemperature = safeNumber(firstDefined(
    decoded.temperature_c,
    decoded.local_temperature_c
));
const localPosition = safeNumber(firstDefined(
    decoded.position_mm,
    decoded.local_position_mm
));
const localWegRaw = safeNumber(firstDefined(
    decoded.weg_raw,
    decoded.local_weg_raw
));
const inaCurrentMa = safeNumber(firstDefined(
    decoded.ina226_current_ma,
    decoded.ina_current_ma
));
const batteryVoltageV = safeNumber(firstDefined(
    decoded.battery_voltage_v,
    decoded.battery_v
));

const modbusAirTemperature = safeNumber(valueFrom(decoded, modbus, "air_temperature_c"));
const modbusHumidity = safeNumber(valueFrom(decoded, modbus, "humidity_pct"));
const modbusPressure = safeNumber(valueFrom(decoded, modbus, "pressure_pa"));
const modbusLight = safeNumber(valueFrom(decoded, modbus, "light_lux"));
const modbusMinWindDirection = safeNumber(valueFrom(decoded, modbus, "min_wind_direction_deg"));
const modbusMaxWindDirection = safeNumber(valueFrom(decoded, modbus, "max_wind_direction_deg"));
const modbusAverageWindDirection = safeNumber(valueFrom(decoded, modbus, "average_wind_direction_deg"));
const modbusMinWindSpeed = safeNumber(valueFrom(decoded, modbus, "min_wind_speed_ms"));
const modbusMaxWindSpeed = safeNumber(valueFrom(decoded, modbus, "max_wind_speed_ms"));
const modbusAverageWindSpeed = safeNumber(valueFrom(decoded, modbus, "average_wind_speed_ms"));
const modbusAccumulatedRainfall = safeNumber(valueFrom(decoded, modbus, "accumulated_rainfall_mm"));
const modbusRainDuration = safeNumber(valueFrom(decoded, modbus, "accumulated_rainfall_duration_s"));
const modbusRainIntensity = safeNumber(valueFrom(decoded, modbus, "rain_intensity_mm_h"));
const modbusMaxRainIntensity = safeNumber(valueFrom(decoded, modbus, "max_rain_intensity_mm_h"));
const modbusHeatingTemperature = safeNumber(valueFrom(decoded, modbus, "heating_temperature_c"));
const modbusTiltState = safeNumber(valueFrom(decoded, modbus, "tilt_state"));
const modbusPm25 = safeNumber(valueFrom(decoded, modbus, "pm25_ug_m3"));
const modbusPm10 = safeNumber(valueFrom(decoded, modbus, "pm10_ug_m3"));
const modbusCo2 = safeNumber(valueFrom(decoded, modbus, "co2_ppm"));
const modbusNoise = safeNumber(valueFrom(decoded, modbus, "noise_db"));
const modbusSolar = safeNumber(valueFrom(decoded, modbus, "solar_radiation_wm2"));
const modbusSunshine = safeNumber(valueFrom(decoded, modbus, "sunshine_duration_h"));

const hasModbusValue = [
    modbusAirTemperature,
    modbusHumidity,
    modbusPressure,
    modbusLight,
    modbusMinWindDirection,
    modbusMaxWindDirection,
    modbusAverageWindDirection,
    modbusMinWindSpeed,
    modbusMaxWindSpeed,
    modbusAverageWindSpeed,
    modbusAccumulatedRainfall,
    modbusRainDuration,
    modbusRainIntensity,
    modbusMaxRainIntensity,
    modbusHeatingTemperature,
    modbusTiltState,
    modbusPm25,
    modbusPm10,
    modbusCo2,
    modbusNoise,
    modbusSolar,
    modbusSunshine
].some(hasValue);
const sisgeoDevices = Array.isArray(decoded.sisgeo_devices)
    ? decoded.sisgeo_devices.filter(value => isObject(value))
    : [];
const hasSisgeoValue = sisgeoDevices.length > 0;

const temperatureValid = safeBoolean(firstDefined(
    decoded.temperature_valid,
    status.local_temperature_valid,
    protocolVersion === 4 && typeof decoded.status === "number"
        ? (decoded.status & 1) !== 0
        : null,
    localTemperature !== null ? true : null
));
const positionValid = safeBoolean(firstDefined(
    decoded.position_valid,
    status.local_position_valid,
    protocolVersion === 4 && typeof decoded.status === "number"
        ? (decoded.status & 2) !== 0
        : null,
    localPosition !== null ? true : null
));
const wegValid = safeBoolean(firstDefined(
    decoded.weg_valid,
    status.local_weg_valid,
    positionValid,
    localWegRaw !== null ? true : null
));
const inaValid = safeBoolean(firstDefined(
    decoded.ina226_valid,
    decoded.ina_valid,
    status.ina_valid,
    protocolVersion === 4 && typeof decoded.status === "number"
        ? (decoded.status & 4) !== 0
        : null,
    inaCurrentMa !== null ? true : null
));
const batteryValid = safeBoolean(firstDefined(
    decoded.battery_valid,
    status.battery_valid,
    protocolVersion === 4 && typeof decoded.status === "number"
        ? (decoded.status & 8) !== 0
        : null,
    batteryVoltageV !== null ? true : null
));
const modbusValid = safeBoolean(firstDefined(
    decoded.modbus_valid,
    modbus.modbus_valid,
    status.modbus_valid,
    hasModbusValue || hasSisgeoValue ? true : null
));
const modbusComplete = safeBoolean(firstDefined(
    decoded.modbus_complete,
    modbus.modbus_complete,
    status.modbus_complete
));

const statusFlags = deriveStatusFlags(decoded, status, protocolVersion, {
    temperature: temperatureValid === true,
    position: positionValid === true,
    weg: wegValid === true,
    ina: inaValid === true,
    battery: batteryValid === true,
    modbus: modbusValid === true,
    modbusComplete: modbusComplete === true
});

const selectedFields = Array.isArray(decoded.selected_fields)
    ? decoded.selected_fields.filter(value => typeof value === "string")
    : [];
const fieldMask = safeNumber(firstDefined(
    decoded.field_mask,
    decoded.lora_field_mask
));
const tiltStateText = deriveTiltText(
    modbusTiltState,
    firstDefined(decoded.tilt_state_text, modbus.tilt_state_text)
);
const controllerId = device.deviceName || device.devEui || "unknown";
const inaCurrentA = safeNumber(firstDefined(
    decoded.ina226_current_a,
    inaCurrentMa === null ? null : inaCurrentMa / 1000
));
const batteryVoltageMv = safeNumber(firstDefined(
    decoded.battery_voltage_mv,
    batteryVoltageV === null ? null : batteryVoltageV * 1000
));

const columns = [
    "timestamp",
    "deduplication_id",
    "controller_id",
    "device_name",
    "dev_eui",
    "dev_addr",
    "frame_counter",
    "f_port",
    "confirmed",
    "adr",
    "data_rate",
    "protocol_version",
    "status",
    "temperature_valid",
    "temperature_c",
    "position_valid",
    "position_mm",
    "weg_raw",
    "ina226_valid",
    "ina226_bus_voltage_v",
    "ina226_shunt_voltage_mv",
    "ina226_current_ma",
    "ina226_current_a",
    "ina226_power_w",
    "battery_valid",
    "battery_voltage_v",
    "battery_voltage_mv",
    "battery_percent",
    "gateway_id",
    "rssi",
    "snr",
    "channel",
    "rf_chain",
    "crc_status",
    "frequency_hz",
    "bandwidth_hz",
    "spreading_factor",
    "code_rate",
    "payload_base64",
    "modbus_valid",
    "modbus_complete",
    "modbus_air_temperature_c",
    "modbus_humidity_pct",
    "modbus_pressure_pa",
    "modbus_light_lux",
    "modbus_min_wind_direction_deg",
    "modbus_max_wind_direction_deg",
    "modbus_average_wind_direction_deg",
    "modbus_min_wind_speed_ms",
    "modbus_max_wind_speed_ms",
    "modbus_average_wind_speed_ms",
    "modbus_accumulated_rainfall_mm",
    "modbus_accumulated_rainfall_duration_s",
    "modbus_rain_intensity_mm_h",
    "modbus_max_rain_intensity_mm_h",
    "modbus_heating_temperature_c",
    "modbus_tilt_state",
    "modbus_tilt_state_text",
    "modbus_pm25_ug_m3",
    "modbus_pm10_ug_m3",
    "modbus_co2_ppm",
    "modbus_noise_db",
    "modbus_solar_radiation_wm2",
    "modbus_sunshine_duration_h",
    "lora_field_mask",
    "lora_selected_fields",
    "lora_sisgeo_devices"
];

const params = [
    timestamp.toISOString(),
    safeString(uplink.deduplicationId),

    safeString(controllerId),
    safeString(device.deviceName),
    safeString(device.devEui),
    safeString(uplink.devAddr),

    safeNumber(uplink.fCnt),
    safeNumber(uplink.fPort),
    safeBoolean(uplink.confirmed),
    safeBoolean(uplink.adr),
    safeDataRate(uplink.dr),

    protocolVersion,
    statusFlags,

    temperatureValid,
    localTemperature,

    positionValid,
    localPosition,
    localWegRaw,

    inaValid,
    safeNumber(firstDefined(decoded.ina226_bus_voltage_v, decoded.ina_bus_v)),
    safeNumber(firstDefined(decoded.ina226_shunt_voltage_mv, decoded.ina_shunt_mv)),
    inaCurrentMa,
    inaCurrentA,
    safeNumber(firstDefined(decoded.ina226_power_w, decoded.ina_power_w)),

    batteryValid,
    batteryVoltageV,
    batteryVoltageMv,
    safeNumber(firstDefined(decoded.battery_percent, decoded.battery_level)),

    safeString(firstDefined(rx.gatewayId, rx.gateway_id)),
    safeNumber(rx.rssi),
    safeNumber(rx.snr),
    safeNumber(firstDefined(rx.channel, rx.channelIndex)),
    safeNumber(firstDefined(rx.rfChain, rx.rf_chain)),
    safeString(rx.crcStatus),

    safeNumber(tx.frequency),
    safeNumber(firstDefined(lora.bandwidth, lora.bandwidthHz)),
    safeNumber(firstDefined(lora.spreadingFactor, lora.spreading_factor)),
    safeString(lora.codeRate),

    safeString(uplink.data),

    modbusValid,
    modbusComplete,
    modbusAirTemperature,
    modbusHumidity,
    modbusPressure,
    modbusLight,
    modbusMinWindDirection,
    modbusMaxWindDirection,
    modbusAverageWindDirection,
    modbusMinWindSpeed,
    modbusMaxWindSpeed,
    modbusAverageWindSpeed,
    modbusAccumulatedRainfall,
    modbusRainDuration,
    modbusRainIntensity,
    modbusMaxRainIntensity,
    modbusHeatingTemperature,
    modbusTiltState,
    tiltStateText,
    modbusPm25,
    modbusPm10,
    modbusCo2,
    modbusNoise,
    modbusSolar,
    modbusSunshine,
    fieldMask,
    JSON.stringify(selectedFields),
    JSON.stringify(sisgeoDevices)
];

const sqlColumns = columns.map(column => `"${column}"`).join(",\n    ");
const placeholders = columns.map((_, index) => `$${index + 1}`).join(",\n    ");
const updateAssignments = columns
    .filter(column => column !== "deduplication_id")
    .map(column => `"${column}" = EXCLUDED."${column}"`)
    .join(",\n    ");

msg.query = `
INSERT INTO public.multiconnect (
    ${sqlColumns}
)
VALUES (
    ${placeholders}
)
ON CONFLICT ("deduplication_id")
DO UPDATE SET
    ${updateAssignments}
RETURNING id;
`;

msg.params = params;
return msg;
