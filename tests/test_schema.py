def test_invalid_schema_rejected(client):
    bad = {"topic": "x", "timestamp": "2024-01-01T00:00:00Z", "source": "t", "payload": {}}
    r = client.post("/publish", json=bad)
    assert r.status_code == 422
