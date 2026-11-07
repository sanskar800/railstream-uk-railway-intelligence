"""Splitting STOMP frames into TRUST messages.

A frame body is a JSON array of zero or more messages (Open Rail Data wiki; observed: about 27
per frame at peak, never empty in a day of capture). Each message is {"header": ..., "body": ...}.
"""

from __future__ import annotations

import json


class FrameError(ValueError):
    """The frame body is not a JSON array/object of messages."""


def split_frame(body: str) -> list[dict]:
    """Return the messages in a frame. A single object is accepted as a one-message frame."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise FrameError(f"not JSON: {exc.msg} at position {exc.pos}") from exc
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list) or not all(isinstance(m, dict) for m in payload):
        raise FrameError(f"expected an array of objects, got {type(payload).__name__}")
    return payload


def message_key(message: dict) -> str | None:
    """Kafka key for a TRUST message: the train id, so each train's events stay in order."""
    body = message.get("body")
    train_id = body.get("train_id") if isinstance(body, dict) else None
    return train_id or None
