import pytest
from notification_service.config import Settings, origin
from notification_service.protocol import NotificationInput
from notification_service.storage import Repository


@pytest.fixture
def settings(tmp_path):
    return Settings(token="test-internal-secret", database=tmp_path / "test.db",
                    allowed_origins=frozenset({origin("http://supplier.test")}),
                    request_timeout=.2, connect_timeout=.1, lease_seconds=1,
                    db_busy_seconds=.1, poll_seconds=.01, retry_base=.01)


@pytest.fixture
def repo(settings):
    repository = Repository(settings)
    repository.initialize()
    return repository


@pytest.fixture
def notification():
    return NotificationInput(url="http://supplier.test/notify", method="POST",
                             headers={"Content-Type": "application/json", "Authorization": "Bearer supplier-secret"},
                             body='{"message":"你好"}')
