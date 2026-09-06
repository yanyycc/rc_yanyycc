"""Real loopback HTTP + forcibly terminated API/Worker processes."""
from collections import defaultdict
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

import httpx
import pytest

from notification_service.config import origin
from notification_service.protocol import NotificationInput
from notification_service.storage import Repository

ROOT = Path(__file__).resolve().parents[1]


def wait_until(callback, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = callback()
        if result:
            return result
        time.sleep(.03)
    raise AssertionError("Timed out waiting for expected system state")


@pytest.fixture
def supplier():
    class State:
        counts = defaultdict(int)
        bodies = []
        headers = []
        lock = threading.Lock()
        received = threading.Event()
        release = threading.Event()

    state = State()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with state.lock:
                state.counts[self.path] += 1
                count = state.counts[self.path]
                state.bodies.append(body)
                state.headers.append(dict(self.headers))
            # Treat recording the count as a non-idempotent external side effect.
            if self.path == "/hold" and count == 1:
                state.received.set()
                state.release.wait(10)
            if self.path == "/timeout":
                time.sleep(.8)
            status = 503 if self.path == "/flaky" and count == 1 else 400 if self.path == "/bad" else 302 if self.path == "/redirect" else 204
            try:
                self.send_response(status)
                if status == 302:
                    self.send_header("Location", "/must-not-follow")
                self.send_header("Content-Length", "0")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", state
    state.release.set()
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def process_env(settings):
    return {**os.environ, "PYTHONPATH": str(ROOT / "src"),
            "INTERNAL_API_TOKEN": settings.token, "DATABASE_PATH": str(settings.database),
            "ALLOWED_ORIGINS": ",".join(settings.allowed_origins),
            "REQUEST_TIMEOUT": str(settings.request_timeout), "CONNECT_TIMEOUT": str(settings.connect_timeout),
            "LEASE_SECONDS": str(settings.lease_seconds), "DB_BUSY_SECONDS": str(settings.db_busy_seconds),
            "POLL_SECONDS": str(settings.poll_seconds), "RETRY_BASE_SECONDS": str(settings.retry_base),
            "WORKER_CONCURRENCY": str(settings.concurrency), "MAX_ATTEMPTS": str(settings.max_attempts)}


@pytest.fixture
def processes():
    children = []
    def start(args, settings):
        child = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=process_env(settings),
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        children.append(child)
        return child
    yield start
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)


def test_api_restart_and_real_worker_delivery(settings, supplier, processes):
    target, state = supplier
    settings = replace(settings, allowed_origins=frozenset({origin(target)}),
                       request_timeout=.3, lease_seconds=2)
    repo = Repository(settings)
    repo.initialize()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    args = ["-m", "uvicorn", "notification_service.api:create_app", "--factory", "--host", "127.0.0.1", "--port", str(port), "--no-access-log"]
    api = processes(args, settings)
    with httpx.Client(base_url=f"http://127.0.0.1:{port}", headers={"Authorization": f"Bearer {settings.token}"}, timeout=2, trust_env=False) as client:
        def ready():
            try:
                return client.get("/health/ready").status_code == 200
            except httpx.TransportError:
                return False
        wait_until(ready)
        ids = {}
        for mode in ("success", "flaky", "bad", "redirect", "timeout"):
            response = client.post("/v1/notifications", headers={"Idempotency-Key": mode}, json={
                "url": f"{target}/{mode}", "method": "POST",
                "headers": {"Content-Type": "application/json", "Authorization": "Bearer external-secret"},
                "body": '{"original":"你好", "keep_spaces": true}',
            })
            assert response.status_code == 202
            ids[mode] = response.json()["id"]
        # No Worker running yet. Kill API after 202, then restart from the same DB.
        api.kill()
        api.wait(timeout=5)
        processes(args, settings)
        wait_until(ready)
        for task_id in ids.values():
            assert client.get(f"/v1/notifications/{task_id}").json()["status"] == "pending"
        processes(["-m", "notification_service.worker"], settings)
        def complete():
            rows = {mode: repo.get(task_id) for mode, task_id in ids.items()}
            return rows if all(r["status"] in {"succeeded", "failed"} for r in rows.values()) else None
        rows = wait_until(complete)
        assert rows["success"]["status"] == "succeeded"
        assert rows["flaky"]["status"] == "succeeded" and rows["flaky"]["attempt_count"] == 2
        assert rows["bad"]["status"] == "failed" and rows["bad"]["attempt_count"] == 1
        assert rows["redirect"]["attempt_count"] == 1
        assert state.counts["/must-not-follow"] == 0
        assert rows["timeout"]["attempt_count"] == 5 and rows["timeout"]["status"] == "failed"
        assert all(b == '{"original":"你好", "keep_spaces": true}'.encode() for b in state.bodies)
        assert all(h.get("authorization") == "Bearer external-secret" for h in state.headers)
        attempts = client.get(f"/v1/notifications/{ids['flaky']}/attempts")
        assert [a["http_status"] for a in attempts.json()["items"]] == [503, 204]
        assert "external-secret" not in attempts.text


def test_worker_killed_after_external_side_effect_recovers_with_possible_duplicate(settings, supplier, processes):
    target, state = supplier
    settings = replace(settings, allowed_origins=frozenset({origin(target)}),
                       request_timeout=2, lease_seconds=3, concurrency=1)
    repo = Repository(settings)
    repo.initialize()
    row, _ = repo.create("uncertain", NotificationInput(url=target + "/hold", method="POST"))
    worker = processes(["-m", "notification_service.worker"], settings)
    assert state.received.wait(8), "Worker did not reach supplier"
    assert repo.get(row["id"])["status"] == "processing"
    worker.kill()
    worker.wait(timeout=5)
    state.release.set()
    processes(["-m", "notification_service.worker"], settings)
    wait_until(lambda: repo.get(row["id"])["status"] == "succeeded")
    assert state.counts["/hold"] == 2
    assert [a["outcome"] for a in repo.attempts(row["id"])] == ["interrupted", "succeeded"]


def test_worker_killed_after_claim_before_send(settings, supplier, processes):
    target, state = supplier
    settings = replace(settings, allowed_origins=frozenset({origin(target)}))
    repo = Repository(settings)
    repo.initialize()
    row, _ = repo.create("pre-send", NotificationInput(url=target + "/success", method="POST"))
    # Fault injection at the boundary between the claim transaction and network send.
    code = "from notification_service.config import Settings; from notification_service.storage import Repository; import time; Repository(Settings.from_env()).claim(); time.sleep(60)"
    child = processes(["-c", code], settings)
    wait_until(lambda: repo.get(row["id"])["status"] == "processing")
    child.kill()
    child.wait(timeout=5)
    assert state.counts["/success"] == 0
    processes(["-m", "notification_service.worker"], settings)
    wait_until(lambda: repo.get(row["id"])["status"] == "succeeded")
    assert state.counts["/success"] == 1
    assert [a["outcome"] for a in repo.attempts(row["id"])] == ["interrupted", "succeeded"]
