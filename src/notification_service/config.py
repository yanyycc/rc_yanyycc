from dataclasses import dataclass
import math
import os
from pathlib import Path
from urllib.parse import urlsplit


def origin(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError("Expected an absolute HTTP(S) URL")
    if parts.username is not None or parts.password is not None or "#" in url:
        raise ValueError("URL credentials and fragments are forbidden")
    host = parts.hostname.encode("idna").decode("ascii").lower()
    if ":" in host:
        host = f"[{host}]"
    port = parts.port if parts.port is not None else (443 if parts.scheme == "https" else 80)
    if port == 0:
        raise ValueError("URL port must be between 1 and 65535")
    return f"{parts.scheme}://{host}:{port}"


@dataclass(frozen=True)
class Settings:
    token: str
    database: Path
    allowed_origins: frozenset[str]
    max_attempts: int = 5
    request_timeout: float = 10
    connect_timeout: float = 3
    lease_seconds: float = 30
    poll_seconds: float = 1
    concurrency: int = 5
    retry_base: float = 5
    db_busy_seconds: float = 2

    def __post_init__(self):
        if not self.token or not self.token.isascii():
            raise ValueError("INTERNAL_API_TOKEN must be a nonempty ASCII secret")
        if not self.allowed_origins:
            raise ValueError("ALLOWED_ORIGINS must not be empty")
        for field in (self.max_attempts, self.request_timeout, self.connect_timeout,
                      self.lease_seconds, self.poll_seconds, self.concurrency,
                      self.retry_base, self.db_busy_seconds):
            if not math.isfinite(field) or field <= 0:
                raise ValueError("Timing, count and concurrency settings must be positive and finite")
        if self.lease_seconds <= self.request_timeout + self.db_busy_seconds:
            raise ValueError("Lease must exceed request timeout plus database wait budget")
        if self.connect_timeout > self.request_timeout:
            raise ValueError("Connect timeout must not exceed total request timeout")

    @classmethod
    def from_env(cls):
        raw_origins = [s.strip() for s in os.environ.get("ALLOWED_ORIGINS", "").split(",") if s.strip()]
        for raw in raw_origins:
            p = urlsplit(raw)
            if p.path not in {"", "/"} or p.query:
                raise ValueError("ALLOWED_ORIGINS contains origins, not paths or queries")
        return cls(
            token=os.environ.get("INTERNAL_API_TOKEN", ""),
            database=Path(os.environ.get("DATABASE_PATH", "data/notifications.db")),
            allowed_origins=frozenset(origin(u) for u in raw_origins),
            max_attempts=int(os.environ.get("MAX_ATTEMPTS", "5")),
            request_timeout=float(os.environ.get("REQUEST_TIMEOUT", "10")),
            connect_timeout=float(os.environ.get("CONNECT_TIMEOUT", "3")),
            lease_seconds=float(os.environ.get("LEASE_SECONDS", "30")),
            poll_seconds=float(os.environ.get("POLL_SECONDS", "1")),
            concurrency=int(os.environ.get("WORKER_CONCURRENCY", "5")),
            retry_base=float(os.environ.get("RETRY_BASE_SECONDS", "5")),
            db_busy_seconds=float(os.environ.get("DB_BUSY_SECONDS", "2")),
        )
