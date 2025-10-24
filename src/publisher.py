import os
import random
import time
from typing import Any, Dict, List
import httpx
from datetime import datetime, timezone

AGG_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")
COUNT = int(os.getenv("PUBLISH_COUNT", "6000"))
DUP_RATIO = float(os.getenv("DUP_RATIO", "0.2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
TOPICS = os.getenv("TOPICS", "alpha,beta,gamma").split(",")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def gen_unique_events(n: int) -> List[Dict[str, Any]]:
    evts = []
    for i in range(n):
        topic = random.choice(TOPICS)
        event_id = f"{topic}-{int(time.time()*1000)}-{i}-{random.randint(0,99999)}"
        evts.append({
            "topic": topic,
            "event_id": event_id,
            "timestamp": now_iso(),
            "source": "publisher-sim",
            "payload": {"i": i, "rand": random.random()}
        })
    return evts

def make_with_duplicates(base: List[Dict[str, Any]], dup_ratio: float) -> List[Dict[str, Any]]:
    dup_count = int(len(base) * dup_ratio)
    dups = []
    for _ in range(dup_count):
        e = random.choice(base)
        # duplicate
        dups.append(e.copy())
    return base + dups

def chunks(arr, size):
    for i in range(0, len(arr), size):
        yield arr[i:i+size]

def main():
    base = gen_unique_events(int(COUNT * (1.0 - DUP_RATIO)))
    data = make_with_duplicates(base, DUP_RATIO)
    random.shuffle(data)
    print(f"Publishing {len(data)} events (dup ratio ~{DUP_RATIO:.2f}) to {AGG_URL}/publish")

    with httpx.Client(timeout=10) as client:
        total = 0
        for batch in chunks(data, BATCH_SIZE):
            r = client.post(f"{AGG_URL}/publish", json=batch)
            r.raise_for_status()
            total += len(batch)
            if total % (BATCH_SIZE*5) == 0:
                print(f"sent {total}...")
        print(f"Done. Sent: {total}")

if __name__ == "__main__":
    main()
