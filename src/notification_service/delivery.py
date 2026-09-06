import asyncio
from dataclasses import dataclass, replace
from datetime import timezone
from email.utils import parsedate_to_datetime
import random
import ssl
import time

import httpx

from .config import Settings
from .protocol import check_target


@dataclass(frozen=True)
class Result:
    success: bool = False
    retryable: bool = False
    http_status: int | None = None
    code: str | None = None
    message: str | None = None
    retry_after: str | None = None
    duration_ms: int | None = None


def retry_delay(attempt, base, retry_after=None, now=None, jitter=None):
    delay = min(base * 2 ** min(attempt - 1, 30), 300) * (random.uniform(.8, 1.2) if jitter is None else jitter)
    if retry_after:
        try:
            if retry_after.strip().isdigit():
                seconds = float(retry_after)
            else:
                date = parsedate_to_datetime(retry_after)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                seconds = date.timestamp() - (time.time() if now is None else now)
            delay = max(delay, min(max(seconds, 0), 3600))
        except (ValueError, TypeError, OverflowError):
            pass
    return delay


def certificate_error(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, ssl.SSLCertVerificationError):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


async def deliver(client: httpx.AsyncClient, task, settings: Settings):
    import json
    started = time.monotonic()
    try:
        check_target(task["url"], settings)
    except ValueError:
        return Result(code="target_not_allowed", message="Target origin is not allowed", duration_ms=0)
    try:
        async with asyncio.timeout(settings.request_timeout):
            headers = json.loads(task["headers_json"])
            request = client.build_request(task["method"], task["url"], headers=headers,
                                           content=task["body_text"].encode("utf-8"))
            # A pooled client's cookie jar must not couple independent notifications.
            if "cookie" not in headers:
                request.headers.pop("cookie", None)
            else:
                request.headers["cookie"] = headers["cookie"]
            response = await client.send(request, stream=True, follow_redirects=False)
            try:
                status = response.status_code
                if 200 <= status < 300:
                    result = Result(success=True, http_status=status)
                else:
                    retryable = status in {408, 429} or 500 <= status < 600
                    code = ("upstream_server_error" if status >= 500 else
                            "upstream_rate_limited" if status == 429 else
                            "upstream_timeout" if status == 408 else
                            "upstream_redirect" if 300 <= status < 400 else "upstream_client_error")
                    result = Result(retryable=retryable, http_status=status, code=code,
                                    message=f"Upstream returned HTTP {status}",
                                    retry_after=response.headers.get("retry-after") if status in {429, 503} else None)
            finally:
                await response.aclose()
    except (TimeoutError, httpx.TimeoutException):
        result = Result(retryable=True, code="request_timeout", message="Request timed out; external outcome may be unknown")
    except httpx.TransportError as exc:
        if certificate_error(exc):
            result = Result(code="tls_verification_failed", message="TLS certificate verification failed")
        elif isinstance(exc, (httpx.LocalProtocolError, httpx.UnsupportedProtocol)):
            result = Result(code="request_invalid", message="Request could not be constructed")
        else:
            result = Result(retryable=True, code="network_error", message="Network operation failed; external outcome may be unknown")
    except (ValueError, UnicodeError, httpx.InvalidURL):
        result = Result(code="request_invalid", message="Request could not be constructed")
    return replace(result, duration_ms=round((time.monotonic() - started) * 1000))
