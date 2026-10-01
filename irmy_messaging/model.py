import re
import time
from uuid import UUID
from typing import Annotated, Literal
from pydantic import BaseModel, Field, StringConstraints, AfterValidator


_MAX_EVENT_AGE_SECONDS = 300
_MAX_EVENT_FUTURE_SECONDS = 60


def _validate_timestamp(v: int) -> int:
    current_time = int(time.time())
    if current_time - v > _MAX_EVENT_AGE_SECONDS:
        raise ValueError(f"eventTimestamp is too old. Diff: {current_time - v}s")
    if v - current_time > _MAX_EVENT_FUTURE_SECONDS:
        raise ValueError("eventTimestamp is in the future.")
    return v


def _validate_signature_hex(v: str) -> str:
    if len(v) != 64 or not re.fullmatch(r"^[0-9a-fA-F]+$", v):
        raise ValueError("x-signature must be a valid SHA256 hexdigest")
    return v

_ProducerNameType = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9_-]+$")
]

_TimestampType = Annotated[
    int,
    AfterValidator(_validate_timestamp)
]

_SignatureType = Annotated[
    str,
    AfterValidator(_validate_signature_hex)
]


class MessageHeaders(BaseModel):
    event_version: Literal[1] = Field(alias="eventVersion")
    event_timestamp: _TimestampType = Field(alias="eventTimestamp")
    tracing_id: UUID = Field(alias="tracingId")
    producer_name: _ProducerNameType = Field(alias="producerName")
    producer_id: UUID = Field(alias="producerId")
    x_signature: _SignatureType = Field(alias="x-signature")

    model_config = {
        "populate_by_name": True,
        "extra": "ignore"
    }
