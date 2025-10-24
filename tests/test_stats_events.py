import time

def test_events_and_stats_consistent(client):
    batch = [
        {"topic": "t2", "event_id": "a", "timestamp": "2024-01-01T00:00:00Z", "source": "test", "payload": {}},
        {"topic": "t2", "event_id": "b", "timestamp": "2024-01-01T00:00:00Z", "source": "test", "payload": {}},
        {"topic": "t2", "event_id": "b", "timestamp": "2024-01-01T00:00:00Z", "source": "test", "payload": {}}, # dupe
    ]
    client.post("/publish", json=batch)

    t0 = time.time()
    while time.time() - t0 < 3.0:
        s = client.get("/stats").json()
        if s["unique_processed"] >= 2:
            break
        time.sleep(0.02)

    events = client.get("/events", params={"topic": "t2"}).json()
    assert len(events) >= 2
    stats = client.get("/stats").json()
    assert stats["unique_processed"] >= 2
    assert stats["duplicate_dropped"] >= 1
    assert "t2" in stats["topics"]
