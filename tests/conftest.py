import pytest
from starlette.testclient import TestClient
from src.main import app

@pytest.fixture
def client(tmp_path, monkeypatch):
    # db terpisah untuk test
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test-store.db"))
    monkeypatch.setenv("CONSUMER_WORKERS", "2")
    with TestClient(app) as c:
        yield c