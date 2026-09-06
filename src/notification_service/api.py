import asyncio
import hmac
import json
import logging
import sqlite3
from uuid import UUID

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException

from .config import Settings
from .protocol import NotificationInput, check_key, check_target
from .storage import Conflict, Repository, timestamp

logger = logging.getLogger("notification_service")


class APIError(Exception):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message


def error_response(status, code, message):
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message, "details": []}})


def task_output(row):
    return {
        "id": row["id"], "status": row["status"], "attempt_count": row["attempt_count"],
        "max_attempts": row["max_attempts"], "next_attempt_at": timestamp(row["next_attempt_at"]),
        "last_http_status": row["last_http_status"],
        "last_error": {"code": row["last_error_code"], "message": row["last_error_message"]} if row["last_error_code"] else None,
        "created_at": timestamp(row["created_at"]), "updated_at": timestamp(row["updated_at"]),
        "completed_at": timestamp(row["completed_at"]),
    }


def create_app(settings=None):
    settings = settings or Settings.from_env()
    repo = Repository(settings)
    app = FastAPI(title="HTTP Notification Service", version="0.1.0", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.repository = repo

    @app.exception_handler(APIError)
    async def known_error(request, exc):
        return error_response(exc.status, exc.code, exc.message)

    @app.exception_handler(sqlite3.Error)
    async def database_error(request, exc):
        logger.warning("database_unavailable")
        return error_response(503, "database_unavailable", "Database unavailable; retry with the same idempotency key")

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return error_response(422, "validation_error", "Invalid request parameters")

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return error_response(exc.status_code, "http_error", "HTTP request was rejected")

    @app.exception_handler(Exception)
    async def unexpected_error(request, exc):
        logger.error("internal_error")
        return error_response(500, "internal_error", "Internal error; retry with the same idempotency key")

    async def authenticate(request: Request):
        values = request.headers.getlist("authorization")
        expected = f"Bearer {settings.token}".encode("ascii")
        if len(values) != 1 or not hmac.compare_digest(values[0].encode("utf-8"), expected):
            raise APIError(401, "unauthorized", "Valid internal bearer token is required")

    async def get_task(task_id):
        task = await asyncio.to_thread(repo.get, str(task_id))
        if task is None:
            raise APIError(404, "not_found", "Notification not found")
        return task

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready", dependencies=[Depends(authenticate)])
    async def ready():
        if not await asyncio.to_thread(repo.ready):
            raise APIError(503, "not_ready", "Database schema is not ready")
        return {"status": "ready"}

    @app.post("/v1/notifications", dependencies=[Depends(authenticate)])
    async def create(request: Request):
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise APIError(415, "unsupported_media_type", "Content-Type must be application/json")
        if request.headers.get("content-encoding", "identity").lower() != "identity":
            raise APIError(415, "unsupported_encoding", "Compressed request bodies are not supported")
        keys = request.headers.getlist("idempotency-key")
        try:
            key = check_key(keys[0] if len(keys) == 1 else None)
        except ValueError:
            raise APIError(422, "invalid_idempotency_key", "One valid Idempotency-Key is required")
        payload = bytearray()
        async for chunk in request.stream():
            if len(payload) + len(chunk) > 1024 * 1024:
                raise APIError(413, "request_too_large", "Request exceeds 1 MiB")
            payload.extend(chunk)
        def unique_object(pairs):
            result = {}
            for k, v in pairs:
                if k in result:
                    raise ValueError("Duplicate JSON key")
                result[k] = v
            return result
        try:
            raw = json.loads(payload.decode("utf-8"), object_pairs_hook=unique_object,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Invalid constant")))
        except (ValueError, UnicodeError, RecursionError):
            raise APIError(400, "invalid_json", "Body must be valid UTF-8 JSON without duplicate keys")
        try:
            data = NotificationInput.model_validate(raw)
            check_target(data.url, settings)
        except (ValidationError, ValueError):
            raise APIError(422, "validation_error", "Invalid notification fields or target; see protocol documentation")
        try:
            row, deduplicated = await asyncio.to_thread(repo.create, key, data)
        except Conflict:
            raise APIError(409, "idempotency_conflict", "Idempotency key is already used for a different request")
        return JSONResponse(status_code=200 if deduplicated else 202,
                            headers={"Location": f"/v1/notifications/{row['id']}"},
                            content={"id": row["id"], "status": row["status"], "deduplicated": deduplicated,
                                     "created_at": timestamp(row["created_at"])})

    @app.get("/v1/notifications/{task_id}", dependencies=[Depends(authenticate)])
    async def status(task_id: UUID):
        return task_output(await get_task(task_id))

    @app.get("/v1/notifications/{task_id}/attempts", dependencies=[Depends(authenticate)])
    async def attempts(task_id: UUID):
        await get_task(task_id)
        rows = await asyncio.to_thread(repo.attempts, str(task_id))
        output = []
        for row in rows:
            output.append({**{k: row[k] for k in ("id", "attempt_number", "outcome", "duration_ms", "http_status", "error_code", "error_message")},
                           "started_at": timestamp(row["started_at"]), "finished_at": timestamp(row["finished_at"])})
        return {"notification_id": str(task_id), "items": output}

    return app
