-- MKP MultiConnect database migration for Modbus and LoRa V5/V6.
-- Run once on the PostgreSQL database containing public.multiconnect.
-- ADD COLUMN IF NOT EXISTS makes this safe to run more than once.

ALTER TABLE public.multiconnect
    ADD COLUMN IF NOT EXISTS modbus_valid boolean,
    ADD COLUMN IF NOT EXISTS modbus_complete boolean,
    ADD COLUMN IF NOT EXISTS modbus_air_temperature_c double precision,
    ADD COLUMN IF NOT EXISTS modbus_humidity_pct double precision,
    ADD COLUMN IF NOT EXISTS modbus_pressure_pa double precision,
    ADD COLUMN IF NOT EXISTS modbus_light_lux double precision,
    ADD COLUMN IF NOT EXISTS modbus_min_wind_direction_deg double precision,
    ADD COLUMN IF NOT EXISTS modbus_max_wind_direction_deg double precision,
    ADD COLUMN IF NOT EXISTS modbus_average_wind_direction_deg double precision,
    ADD COLUMN IF NOT EXISTS modbus_min_wind_speed_ms double precision,
    ADD COLUMN IF NOT EXISTS modbus_max_wind_speed_ms double precision,
    ADD COLUMN IF NOT EXISTS modbus_average_wind_speed_ms double precision,
    ADD COLUMN IF NOT EXISTS modbus_accumulated_rainfall_mm double precision,
    ADD COLUMN IF NOT EXISTS modbus_accumulated_rainfall_duration_s double precision,
    ADD COLUMN IF NOT EXISTS modbus_rain_intensity_mm_h double precision,
    ADD COLUMN IF NOT EXISTS modbus_max_rain_intensity_mm_h double precision,
    ADD COLUMN IF NOT EXISTS modbus_heating_temperature_c double precision,
    ADD COLUMN IF NOT EXISTS modbus_tilt_state double precision,
    ADD COLUMN IF NOT EXISTS modbus_tilt_state_text text,
    ADD COLUMN IF NOT EXISTS modbus_pm25_ug_m3 double precision,
    ADD COLUMN IF NOT EXISTS modbus_pm10_ug_m3 double precision,
    ADD COLUMN IF NOT EXISTS modbus_co2_ppm double precision,
    ADD COLUMN IF NOT EXISTS modbus_noise_db double precision,
    ADD COLUMN IF NOT EXISTS modbus_solar_radiation_wm2 double precision,
    ADD COLUMN IF NOT EXISTS modbus_sunshine_duration_h double precision,
    ADD COLUMN IF NOT EXISTS lora_field_mask bigint,
    ADD COLUMN IF NOT EXISTS lora_selected_fields jsonb,
    ADD COLUMN IF NOT EXISTS lora_sisgeo_devices jsonb;
