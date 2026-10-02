from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from .safe_runtime import HealthRegistry, TareState, utc_now_iso


def _safe_get(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, Mapping) or key not in value:
            return default
        value = value[key]
    return value


def build_status_payload(config: Mapping[str, Any], health: HealthRegistry, tare_state: Optional[TareState] = None, include_tracebacks: bool = False, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    tare = tare_state.snapshot() if tare_state is not None else {}
    overall = health.overall_status()
    payload: Dict[str, Any] = {
        "type": "status",
        "schema": "nibelung-status-v2",
        "timestamp": utc_now_iso(),
        "device_id": _safe_get(config, "device", "id"),
        "location": _safe_get(config, "device", "location"),
        "overall": overall,
        "tare_active": bool(tare.get("active", False)),
        "tare_requested": bool(tare.get("requested", False)),
        "tare_source": tare.get("source"),
        "tare_last_error": tare.get("last_error"),
        "status": {
            "overall": overall,
            "tare": tare,
            "modules": health.snapshot(include_tracebacks=include_tracebacks),
        },
    }
    if extra:
        payload["extra"] = extra
    return payload
