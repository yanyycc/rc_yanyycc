from concurrent.futures import ThreadPoolExecutor
import json
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from notification_service.api import create_app


@pytest.fixture
def client(settings, repo):
    with TestClient(create_app(settings)) as c:
        c.headers.update({"Authorization": f"Bearer {settings.token}"})
        yield c


def test_create_query_and_secret_redaction(client, notification):
    response = client.post("/v1/notifications", headers={"Idempotency-Key": "one"}, json=notification.model_dump())
    assert response.status_code == 202
    assert response.json()["deduplicated"] is False
    location = response.headers["location"]
    status = client.get(location)
    assert status.status_code == 200
    assert status.json()["status"] == "pending"
    assert "supplier-secret" not in status.text
    assert "headers" not in status.json()
    assert client.get(location + "/attempts").json()["items"] == []
    assert client.get(f"/v1/notifications/{uuid4()}").status_code == 404


def test_duplicate_and_conflict(client, notification):
    first = client.post("/v1/notifications", headers={"Idempotency-Key": "one"}, json=notification.model_dump())
    data = notification.model_dump()
    data["headers"] = {k.upper(): v for k, v in data["headers"].items()}
    second = client.post("/v1/notifications", headers={"Idempotency-Key": "one"}, json=data)
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["deduplicated"] is True
    data["body"] = "changed"
    assert client.post("/v1/notifications", headers={"Idempotency-Key": "one"}, json=data).status_code == 409


def test_concurrent_submission(client, notification):
    def submit(_):
        return client.post("/v1/notifications", headers={"Idempotency-Key": "concurrent"}, json=notification.model_dump())
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(submit, range(16)))
    assert sum(r.status_code == 202 for r in responses) == 1
    assert all(r.status_code in {200, 202} for r in responses)
    assert len({r.json()["id"] for r in responses}) == 1


@pytest.mark.parametrize("patch", [
    {"url": "http://unapproved.test/"}, {"url": "http://user:password@supplier.test/"},
    {"url": "http://supplier.test/#fragment"}, {"url": "http://supplier.test:0/"}, {"method": "GET"}, {"body": {}},
    {"headers": {"Host": "x"}}, {"headers": {"x": "a\r\nb"}},
    {"headers": {"X": "a", "x": "b"}}, {"headers": {"x": "中文"}},
    {"headers": {}}, {"unknown": True}, {"body": "x" * (256 * 1024 + 1)},
])
def test_invalid_payloads_rejected(client, notification, patch):
    response = client.post("/v1/notifications", headers={"Idempotency-Key": "invalid"}, json={**notification.model_dump(), **patch})
    assert response.status_code == 422
    assert "supplier-secret" not in response.text


def test_http_protocol_errors(client):
    assert client.get("/health/live", headers={"Authorization": "bad"}).status_code == 200
    assert client.get("/health/ready", headers={"Authorization": "bad"}).status_code == 401
    assert client.get("/health/ready").status_code == 200
    assert client.post("/v1/notifications", content="{}").status_code == 415
    headers = {"Idempotency-Key": "x", "Content-Type": "application/json"}
    assert client.post("/v1/notifications", headers=headers, content="{").status_code == 400
    assert client.post("/v1/notifications", headers=headers, content='{"url":"a","url":"b"}').status_code == 400
    assert client.post("/v1/notifications", headers=headers, content=b"x" * (1024 * 1024 + 1)).status_code == 413
    assert client.post("/v1/notifications", json={}).status_code == 422


def test_database_locked_does_not_ack(client, repo, notification):
    with repo.transaction():
        response = client.post("/v1/notifications", headers={"Idempotency-Key": "locked"}, json=notification.model_dump())
        assert response.status_code == 503
    assert client.post("/v1/notifications", headers={"Idempotency-Key": "locked"}, json=notification.model_dump()).status_code == 202


def test_not_ready_without_initialized_schema(settings):
    with TestClient(create_app(settings)) as client:
        assert client.get("/health/ready", headers={"Authorization": f"Bearer {settings.token}"}).status_code == 503
