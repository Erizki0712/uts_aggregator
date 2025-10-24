import time

def wait_until(client, cond, timeout=3.0):
    start = time.time()
    while time.time() - start < timeout:
        if cond():
            return True
        time.sleep(0.02)
    return False

def test_dedup_only_once_processed(client):
    evt = {
        "topic": "t1",
        "event_id": "same-1",
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "test",
        "payload": {"k": 1}
    }
    client.post("/publish", json=evt)
    client.post("/publish", json=evt)  # duplikat

    ok = wait_until(client, lambda: client.get("/stats").json()["unique_processed"] >= 1)
    assert ok, "consumer didn't process in time"

    stats = client.get("/stats").json()
    assert stats["received"] >= 2
    assert stats["unique_processed"] >= 1
    assert stats["duplicate_dropped"] >= 1
