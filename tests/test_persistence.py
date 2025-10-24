import os
import tempfile
from src.store import EventStore

def test_event_and_dedup_persist_across_reopen():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "store.db")
        s1 = EventStore(db)
        evt = {
            "topic": "topicA",
            "event_id": "id-1",
            "timestamp": "2024-01-01T00:00:00Z",
            "source": "test",
            "payload": {"k": 1}
        }
        assert s1.insert_if_absent(evt) is True
        s1.close()

        s2 = EventStore(db)
        # key sama dianggap duplikat
        assert s2.insert_if_absent(evt) is False
        assert s2.contains("topicA", "id-1") is True
        events = s2.get_events(topic="topicA")
        assert any(e["event_id"] == "id-1" for e in events)
        assert s2.count_unique() >= 1
        s2.close()
