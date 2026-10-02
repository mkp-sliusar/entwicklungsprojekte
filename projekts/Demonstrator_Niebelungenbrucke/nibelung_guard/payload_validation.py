from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Mapping, Tuple


@dataclass
class SanitizeResult:
    payload: Dict[str, Any]
    dropped_fields: Tuple[str, ...]


def sanitize_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [sanitize_json_value(v) for v in value]
    return str(value)


def safe_json_dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(sanitize_json_value(payload), ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def sanitize_influx_fields(fields: Mapping[str, Any]) -> Tuple[Dict[str, Any], Tuple[str, ...]]:
    clean: Dict[str, Any] = {}
    dropped = []
    for key, value in fields.items():
        field = str(key)
        if value is None:
            dropped.append(field)
        elif isinstance(value, bool):
            clean[field] = value
        elif isinstance(value, int) and not isinstance(value, bool):
            clean[field] = value
        elif isinstance(value, float):
            if math.isfinite(value):
                clean[field] = value
            else:
                dropped.append(field)
        elif isinstance(value, str):
            clean[field] = value
        else:
            dropped.append(field)
    return clean, tuple(dropped)


def sanitize_influx_point(point: Mapping[str, Any]) -> SanitizeResult:
    out = dict(point)
    fields = point.get("fields", {})
    if not isinstance(fields, Mapping):
        return SanitizeResult(payload={}, dropped_fields=("fields",))
    clean_fields, dropped = sanitize_influx_fields(fields)
    if not clean_fields:
        return SanitizeResult(payload={}, dropped_fields=dropped or ("fields",))
    out["fields"] = clean_fields
    tags = point.get("tags")
    if isinstance(tags, Mapping):
        out["tags"] = {str(k): str(v) for k, v in tags.items() if v is not None}
    return SanitizeResult(payload=out, dropped_fields=dropped)
