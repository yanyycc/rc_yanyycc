from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from notification_service.delivery import Result
from notification_service.storage import Repository


def test_atomic_claim_and_restart(repo, settings, notification):
    row, _ = repo.create("key", notification, now=100)
    fresh = Repository(settings)
    assert fresh.get(row["id"])["status"] == "pending"
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: fresh.claim(now=101), range(8)))
    assert sum(c is not None for c in claims) == 1
    assert len(repo.attempts(row["id"])) == 1


def test_expired_lease_and_stale_write(repo, notification):
    row, _ = repo.create("key", notification, now=100)
    old = repo.claim(now=100)
    assert not repo.finish(old, Result(success=True, http_status=200), now=101.1)
    assert repo.recover(now=101.1) == 1
    new = repo.claim(now=101.2)
    assert not repo.finish(old, Result(success=True, http_status=200), now=101.3)
    assert repo.finish(new, Result(success=True, http_status=204), now=101.3)
    assert repo.get(row["id"])["status"] == "succeeded"
    assert [a["outcome"] for a in repo.attempts(row["id"])] == ["interrupted", "succeeded"]


def test_retry_exhaustion_and_http_status_reset(repo, notification):
    row, _ = repo.create("key", notification, now=100)
    for n in range(5):
        task = repo.claim(now=100 + n * .1)
        result = Result(retryable=True, http_status=503 if n == 0 else None, code="request_timeout", message="timeout")
        repo.finish(task, result, delay=0, now=100 + n * .1)
    final = repo.get(row["id"])
    assert final["status"] == "failed"
    assert final["attempt_count"] == 5
    assert final["last_error_code"] == "attempts_exhausted"
    assert final["last_http_status"] is None
    assert repo.claim(now=200) is None
    duplicate, deduplicated = repo.create("key", notification, now=200)
    assert deduplicated and duplicate["status"] == "failed"


def test_last_interrupted_attempt_is_terminal(settings, notification):
    repo = Repository(replace(settings, max_attempts=1))
    repo.initialize()
    row, _ = repo.create("key", notification, now=100)
    repo.claim(now=100)
    repo.recover(now=102)
    assert repo.get(row["id"])["status"] == "failed"
    assert repo.claim(now=103) is None
