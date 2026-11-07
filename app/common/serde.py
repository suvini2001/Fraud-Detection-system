"""
Serialization / deserialization (SerDe) for Kafka messages.

Kafka only stores BYTES. This module is the single place that turns
Pydantic objects into bytes (producer side) and bytes back into validated
objects (consumer side).

  Producer:  Pydantic model -> JSON (str) -> UTF-8 bytes  -> Kafka
  Consumer:  Kafka -> UTF-8 bytes -> JSON -> Pydantic validation -> model

Headers carry metadata (schema version, content type, producer name) so a
consumer can decide how to decode a message BEFORE looking at the payload.
"""

from typing import Dict, List, Optional, Tuple, Type, TypeVar

from pydantic import BaseModel, ValidationError

from .schemas import SCHEMA_VERSION

T = TypeVar("T", bound=BaseModel)

CONTENT_TYPE = "application/json"


class DeserializationError(Exception):
    """Raised when a message is not valid UTF-8 / JSON / does not match the schema."""


def serialize_key(key: Optional[str]) -> Optional[bytes]:
    return key.encode("utf-8") if key is not None else None


def serialize_value(obj: BaseModel) -> bytes:
    # mode="json" turns datetimes into ISO-8601 strings
    return obj.model_dump_json().encode("utf-8")


def deserialize_value(raw: Optional[bytes], model: Type[T]) -> T:
    if raw is None:
        raise DeserializationError("empty message value (tombstone)")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DeserializationError(f"value is not valid UTF-8: {exc}") from exc
    try:
        return model.model_validate_json(text)
    except ValidationError as exc:
        # includes JSON syntax errors and schema violations
        raise DeserializationError(f"schema validation failed: {exc.errors()[:3]}") from exc


def build_headers(source: str, extra: Optional[Dict[str, str]] = None) -> List[Tuple[str, bytes]]:
    headers = {
        "content-type": CONTENT_TYPE,
        "schema-version": str(SCHEMA_VERSION),
        "source": source,
    }
    if extra:
        headers.update(extra)
    return [(k, v.encode("utf-8")) for k, v in headers.items()]


def headers_to_dict(headers) -> Dict[str, str]:
    if not headers:
        return {}
    out = {}
    for k, v in headers:
        out[k] = v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else str(v)
    return out
