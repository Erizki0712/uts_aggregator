import time

def test_stress_batch_5000_with_duplicates(client):
    uniq = 4000
    dup = 1000
    base = [{
        "topic": "load",
        "event_id": f"e-{i}",
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "test",
        "payload": {"i": i}
    } for i in range(uniq)]
    batch = base + base[:dup]
    client.post("/publish", json=batch)

    t0 = time.time()
    deadline = 10.0
    while time.time() - t0 < deadline:
        s = client.get("/stats").json()
        if s["unique_processed"] >= uniq:
            break
        time.sleep(0.02)

    s = client.get("/stats").json()
    assert s["unique_processed"] >= uniq
    assert s["duplicate_dropped"] >= dup
