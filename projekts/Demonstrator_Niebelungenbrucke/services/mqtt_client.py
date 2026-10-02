from __future__ import annotations

import json
import logging
import time
from typing import Any

import paho.mqtt.client as mqtt


class MQTTClient:
    def __init__(
        self,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        topic: str = "",
        client_id: str | None = None,
        keepalive: int = 60,
        qos: int = 0,
        retain: bool = False,
        connect_timeout_s: float = 5.0,
        max_queued_messages: int = 5000,
    ):
        self.host = host
        self.port = int(port)
        self.topic = topic.rstrip("/")
        self.keepalive = int(keepalive)
        self.qos = int(qos)
        self.retain = bool(retain)
        self.connected = False
        self.last_error = ""
        self.last_connect_ts = 0.0
        self.log = logging.getLogger(f"mqtt.{host}:{port}")

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id or "",
        )
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        self.client.max_queued_messages_set(max_queued_messages)
        self.client.max_inflight_messages_set(100)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

        if username:
            self.client.username_pw_set(username, password or "")

        # connect_async returns immediately. MQTT network handling runs in the
        # paho background loop, not in any sensor measurement thread.
        try:
            self.client.connect_async(self.host, self.port, self.keepalive)
            self.client.loop_start()
            self.last_connect_ts = time.time()
            self.log.info("MQTT connecting to %s:%s", self.host, self.port)
        except Exception as exc:
            self.connected = False
            self.last_error = str(exc)
            self.log.exception("MQTT connect failed")

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.connected = True
        self.last_error = ""
        self.log.info("MQTT connected topic=%s reason=%s", self.topic, reason_code)

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.connected = False
        self.last_error = str(reason_code)
        self.log.warning("MQTT disconnected topic=%s reason=%s", self.topic, reason_code)

    def publish_json(
        self,
        payload: dict[str, Any],
        topic: str | None = None,
        retain_override: bool | None = None,
    ) -> None:
        if not self.connected:
            return

        target_topic = topic or self.topic
        message = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str)
        result = self.client.publish(
            target_topic,
            message,
            qos=self.qos,
            retain=self.retain if retain_override is None else bool(retain_override),
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            self.last_error = f"publish rc={result.rc}"

    def close(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            self.log.exception("MQTT close failed")

    # Backward-compatible helpers used by the old app.py.
    def publish(
        self,
        device_id,
        vibration_rms,
        vibration_peak,
        vibration_z,
        vibration_z_rms,
        vibration_z_peak,
        temperature,
    ):
        self.publish_json(
            {
                "device": device_id,
                "type": "mpu9250",
                "vibration_rms": vibration_rms,
                "vibration_peak": vibration_peak,
                "vibration_z_rms": vibration_z_rms,
                "vibration_z_peak": vibration_z_peak,
                "vibration_z": vibration_z,
                "temperature": temperature,
            }
        )

    def publish_ads1220(self, device_id, raw, mv, mv_per_v, strain_um_m, weight_g):
        self.publish_json(
            {
                "device": device_id,
                "type": "ads1220",
                "raw": raw,
                "mv": mv,
                "mv_per_v": mv_per_v,
                "strain_um_m": strain_um_m,
                "weight_g": weight_g,
            }
        )

    def publish_system(self, device_id, cpu_percent, ram_percent, disk_percent, cpu_temp, internet):
        self.publish_json(
            {
                "device": device_id,
                "type": "system",
                "cpu_percent": cpu_percent,
                "ram_percent": ram_percent,
                "disk_percent": disk_percent,
                "cpu_temp": cpu_temp,
                "internet": internet,
            }
        )
