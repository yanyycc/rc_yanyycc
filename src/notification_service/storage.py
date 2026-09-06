"""All transactions stay within the calling thread; no connection crosses threads."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import sqlite3
import time
import uuid

from .config import Settings

SCHEMA_VERSION = 1
SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, request_hash TEXT NOT NULL,
    url TEXT NOT NULL, method TEXT NOT NULL, headers_json TEXT NOT NULL, body_text TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','processing','retry_wait','succeeded','failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
    next_attempt_at REAL, lease_token TEXT, lease_expires_at REAL,
    last_http_status INTEGER, last_error_code TEXT, last_error_message TEXT,
    created_at REAL NOT NULL, updated_at REAL NOT NULL, completed_at REAL
);
CREATE INDEX IF NOT EXISTS notification_due ON notifications(status,next_attempt_at);
CREATE INDEX IF NOT EXISTS notification_lease ON notifications(status,lease_expires_at);
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY, notification_id TEXT NOT NULL REFERENCES notifications(id),
    attempt_number INTEGER NOT NULL, lease_token TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('running','succeeded','retryable_failure','permanent_failure','interrupted')),
    started_at REAL NOT NULL, finished_at REAL, duration_ms INTEGER,
    http_status INTEGER, error_code TEXT, error_message TEXT,
    UNIQUE(notification_id, attempt_number)
);
PRAGMA user_version=1;
"""


class Conflict(Exception):
    pass


def timestamp(value):
    return None if value is None else datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Repository:
    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self):
        c = sqlite3.connect(self.settings.database, timeout=self.settings.db_busy_seconds, isolation_level=None)
        c.row_factory = sqlite3.Row
        try:
            c.execute("PRAGMA foreign_keys=ON")
            c.execute("PRAGMA synchronous=FULL")
            return c
        except BaseException:
            c.close()
            raise

    @contextmanager
    def transaction(self):
        c = self.connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def initialize(self):
        self.settings.database.parent.mkdir(parents=True, exist_ok=True)
        c = self.connect()
        try:
            version = c.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, SCHEMA_VERSION):
                raise RuntimeError("Unsupported database schema")
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + "\nCOMMIT;")
        finally:
            c.close()

    def ready(self):
        if not self.settings.database.is_file():
            return False
        c = self.connect()
        try:
            return (c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
                    and c.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
                    and c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('notifications','delivery_attempts')").fetchone()[0] == 2)
        finally:
            c.close()

    def create(self, key, data, now=None):
        now = time.time() if now is None else now
        fingerprint = data.fingerprint()
        with self.transaction() as c:
            existing = c.execute("SELECT * FROM notifications WHERE idempotency_key=?", (key,)).fetchone()
            if existing:
                if existing["request_hash"] != fingerprint:
                    raise Conflict()
                return dict(existing), True
            task_id = str(uuid.uuid4())
            c.execute("""INSERT INTO notifications
                (id,idempotency_key,request_hash,url,method,headers_json,body_text,status,max_attempts,next_attempt_at,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,'pending',?,?,?,?)""",
                      (task_id, key, fingerprint, data.url, data.method, json.dumps(data.headers), data.body,
                       self.settings.max_attempts, now, now, now))
            return dict(c.execute("SELECT * FROM notifications WHERE id=?", (task_id,)).fetchone()), False

    def get(self, task_id):
        c = self.connect()
        try:
            row = c.execute("SELECT * FROM notifications WHERE id=?", (task_id,)).fetchone()
            return dict(row) if row else None
        finally:
            c.close()

    def attempts(self, task_id):
        c = self.connect()
        try:
            return [dict(r) for r in c.execute("SELECT * FROM delivery_attempts WHERE notification_id=? ORDER BY attempt_number", (task_id,))]
        finally:
            c.close()

    def claim(self, now=None):
        now = time.time() if now is None else now
        with self.transaction() as c:
            row = c.execute("""SELECT * FROM notifications WHERE status IN ('pending','retry_wait')
                AND next_attempt_at<=? AND attempt_count<max_attempts
                ORDER BY next_attempt_at,created_at,id LIMIT 1""", (now,)).fetchone()
            if not row:
                return None
            token = str(uuid.uuid4())
            number = row["attempt_count"] + 1
            c.execute("""UPDATE notifications SET status='processing',attempt_count=?,next_attempt_at=NULL,
                lease_token=?,lease_expires_at=?,updated_at=? WHERE id=?""",
                      (number, token, now + self.settings.lease_seconds, now, row["id"]))
            c.execute("""INSERT INTO delivery_attempts
                (id,notification_id,attempt_number,lease_token,outcome,started_at)
                VALUES (?,?,?,?,'running',?)""", (str(uuid.uuid4()), row["id"], number, token, now))
            return dict(c.execute("SELECT * FROM notifications WHERE id=?", (row["id"],)).fetchone())

    def finish(self, task, result, delay=None, now=None):
        now = time.time() if now is None else now
        with self.transaction() as c:
            row = c.execute("""SELECT * FROM notifications WHERE id=? AND status='processing'
                AND lease_token=? AND lease_expires_at>?""", (task["id"], task["lease_token"], now)).fetchone()
            if not row:
                return False
            status = "succeeded" if result.success else "failed"
            code, message = result.code, result.message
            if result.retryable and row["attempt_count"] < row["max_attempts"]:
                status = "retry_wait"
            elif result.retryable:
                code = "attempts_exhausted"
                message = "Attempt limit reached; see attempt history"
            c.execute("""UPDATE notifications SET status=?,next_attempt_at=?,lease_token=NULL,lease_expires_at=NULL,
                last_http_status=?,last_error_code=?,last_error_message=?,updated_at=?,completed_at=? WHERE id=?""",
                      (status, now + delay if status == "retry_wait" else None, result.http_status, code, message,
                       now, now if status in {"succeeded", "failed"} else None, task["id"]))
            outcome = "succeeded" if result.success else "retryable_failure" if result.retryable else "permanent_failure"
            c.execute("""UPDATE delivery_attempts SET outcome=?,finished_at=?,duration_ms=?,http_status=?,error_code=?,error_message=?
                WHERE notification_id=? AND lease_token=? AND outcome='running'""",
                      (outcome, now, result.duration_ms, result.http_status, result.code, result.message, task["id"], task["lease_token"]))
            return True

    def recover(self, now=None):
        now = time.time() if now is None else now
        with self.transaction() as c:
            rows = c.execute("SELECT * FROM notifications WHERE status='processing' AND lease_expires_at<=? LIMIT 100", (now,)).fetchall()
            for row in rows:
                terminal = row["attempt_count"] >= row["max_attempts"]
                c.execute("""UPDATE delivery_attempts SET outcome='interrupted',finished_at=?,error_code='worker_interrupted',
                    error_message='Lease expired; external outcome is unknown' WHERE notification_id=? AND lease_token=? AND outcome='running'""",
                          (now, row["id"], row["lease_token"]))
                c.execute("""UPDATE notifications SET status=?,next_attempt_at=?,lease_token=NULL,lease_expires_at=NULL,
                    last_http_status=NULL,last_error_code=?,last_error_message='Lease expired; external outcome is unknown',
                    updated_at=?,completed_at=? WHERE id=?""",
                          ("failed" if terminal else "retry_wait", None if terminal else now,
                           "attempts_exhausted" if terminal else "worker_interrupted", now, now if terminal else None, row["id"]))
            return len(rows)


def main():
    Repository(Settings.from_env()).initialize()
    print("Database schema initialized")


if __name__ == "__main__":
    main()
