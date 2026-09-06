import hashlib
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .config import Settings, origin

FORBIDDEN_HEADERS = {
    "host", "content-length", "transfer-encoding", "connection", "keep-alive",
    "te", "trailer", "upgrade", "proxy-authorization", "proxy-connection",
}
HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


class NotificationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    url: str = Field(min_length=1, max_length=2048)
    method: Literal["POST", "PUT", "PATCH", "DELETE"]
    headers: dict[str, str] = Field(default_factory=dict)
    body: str = ""

    @field_validator("url")
    @classmethod
    def valid_url(cls, value):
        if any(ord(c) <= 32 or ord(c) == 127 for c in value) or "\\" in value:
            raise ValueError("URL contains forbidden characters")
        origin(value)
        return value

    @field_validator("headers")
    @classmethod
    def valid_headers(cls, headers):
        if len(headers) > 32:
            raise ValueError("At most 32 headers are supported")
        result = {}
        for name, value in headers.items():
            key = name.lower()
            if not HEADER_NAME.fullmatch(name) or key in FORBIDDEN_HEADERS or key in result:
                raise ValueError("Invalid, forbidden or duplicate header name")
            # HTTPX string headers use ASCII. Reject unsupported input before accepting a task.
            if not value.isascii() or any((ord(c) < 32 and c != "\t") or ord(c) == 127 for c in value):
                raise ValueError("Header values must be ASCII without control characters")
            if value != value.strip(" \t"):
                raise ValueError("Header values must not have surrounding whitespace")
            result[key] = value
        if sum(len(k.encode()) + len(v.encode()) for k, v in result.items()) > 16 * 1024:
            raise ValueError("Headers exceed 16 KiB")
        return result

    @field_validator("body")
    @classmethod
    def valid_body(cls, body):
        try:
            encoded = body.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("Body must be valid UTF-8 text") from exc
        if len(encoded) > 256 * 1024:
            raise ValueError("Body exceeds 256 KiB")
        return body

    @model_validator(mode="after")
    def media_type_required(self):
        if self.body and not self.headers.get("content-type"):
            raise ValueError("Nonempty body requires Content-Type")
        return self

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.model_dump(), sort_keys=True,
                                        ensure_ascii=True, separators=(",", ":")).encode()).hexdigest()


def check_target(url: str, settings: Settings):
    if origin(url) not in settings.allowed_origins:
        raise ValueError("Target origin is not allowed")


def check_key(value: str | None):
    if not value or not 1 <= len(value) <= 128 or any(not 32 <= ord(c) <= 126 for c in value):
        raise ValueError("Idempotency-Key must contain 1-128 printable ASCII characters")
    return value
