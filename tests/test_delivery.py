import asyncio
from dataclasses import replace
from email.utils import formatdate
import ssl

import httpx
import pytest

from notification_service.delivery import deliver, retry_delay
from notification_service.worker import Worker


@pytest.mark.parametrize("status,retryable,success", [(204, False, True), (200, False, True),
    (302, False, False), (400, False, False), (401, False, False), (408, True, False),
    (429, True, False), (503, True, False)])
def test_status_classification(settings, repo, notification, status, retryable, success):
    repo.create("key", notification)
    task = repo.claim()
    calls = []
    def handler(request):
        calls.append(request)
        assert request.content == notification.body.encode("utf-8")
        assert request.headers["authorization"] == "Bearer supplier-secret"
        return httpx.Response(status, headers={"Location": "http://elsewhere.test", "Retry-After": "100"})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await deliver(client, task, settings)
    result = asyncio.run(run())
    assert (result.retryable, result.success) == (retryable, success)
    assert len(calls) == 1
    assert result.retry_after == ("100" if status in {429, 503} else None)


def test_retry_after_and_jitter():
    assert retry_delay(1, 5, jitter=1) == 5
    assert retry_delay(4, 5, jitter=1) == 40
    assert retry_delay(1, 5, "120", jitter=1) == 120
    assert retry_delay(1, 5, formatdate(1200, usegmt=True), now=1000, jitter=1) == 200
    assert retry_delay(1, 5, "broken", jitter=1) == 5
    assert retry_delay(1, 5, "99999", jitter=1) == 3600


def test_total_timeout(settings, repo, notification):
    repo.create("key", notification)
    task = repo.claim()
    async def slow(request):
        await asyncio.sleep(1)
        return httpx.Response(200)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
            return await deliver(client, task, settings)
    result = asyncio.run(run())
    assert result.retryable and result.code == "request_timeout"


def test_tls_not_retried(settings, repo, notification):
    repo.create("key", notification)
    def handler(request):
        raise httpx.ConnectError("secret url") from ssl.SSLCertVerificationError("secret certificate")
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await deliver(client, repo.claim(), settings)
    result = asyncio.run(run())
    assert not result.retryable and result.code == "tls_verification_failed"
    assert "secret" not in result.message


def test_target_rechecked_after_configuration_change(settings, repo, notification):
    repo.create("key", notification)
    restricted = replace(settings, allowed_origins=frozenset({"http://another.test:80"}))
    def unexpected(request):
        pytest.fail("Disallowed target was requested")
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
            return await deliver(client, repo.claim(), restricted)
    assert asyncio.run(run()).code == "target_not_allowed"


def test_worker_retries_then_succeeds(settings, repo, notification, caplog):
    row, _ = repo.create("key", notification)
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) == 1 else 204)
    async def run():
        stop = asyncio.Event()
        worker = asyncio.create_task(Worker(settings, transport=httpx.MockTransport(handler)).run(stop))
        try:
            async with asyncio.timeout(3):
                while (await asyncio.to_thread(repo.get, row["id"]))["status"] != "succeeded":
                    await asyncio.sleep(.02)
        finally:
            stop.set()
            await worker
    asyncio.run(run())
    assert len(calls) == 2
    assert [r["outcome"] for r in repo.attempts(row["id"])] == ["retryable_failure", "succeeded"]
    assert "supplier-secret" not in caplog.text


def test_supplier_response_cookies_do_not_leak_between_tasks(settings, repo, notification):
    repo.create("first", notification)
    repo.create("second", notification)
    seen = []
    def handler(request):
        seen.append(request.headers.get("cookie"))
        return httpx.Response(204, headers={"Set-Cookie": "session=other-customer; Path=/"})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for _ in range(2):
                task = repo.claim()
                result = await deliver(client, task, settings)
                repo.finish(task, result)
    asyncio.run(run())
    assert seen == [None, None]


def test_response_body_is_not_read_and_is_closed(settings, repo, notification):
    class UnboundedBody(httpx.AsyncByteStream):
        closed = False
        async def __aiter__(self):
            raise AssertionError("Response body must not be consumed")
            yield b""
        async def aclose(self):
            self.closed = True
    body = UnboundedBody()
    repo.create("key", notification)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=body))) as client:
            return await deliver(client, repo.claim(), settings)
    assert asyncio.run(run()).success
    assert body.closed


def test_result_write_failure_leaves_lease_for_recovery(settings, repo, notification, monkeypatch):
    import sqlite3
    row, _ = repo.create("key", notification)
    task = repo.claim()
    worker = Worker(settings)
    calls = []
    def fail_write(*args):
        raise sqlite3.OperationalError("busy")
    monkeypatch.setattr(worker.repo, "finish", fail_write)
    def handler(request):
        calls.append(request)
        return httpx.Response(204)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await worker.process(client, task)
    asyncio.run(run())
    assert len(calls) == 1
    assert repo.get(row["id"])["status"] == "processing"
    repo.recover(now=task["lease_expires_at"] + 1)
    assert repo.get(row["id"])["status"] == "retry_wait"
