import os
import json
import sqlite3
import threading
from typing import Any, Dict, List, Optional

class EventStore:
    # persistent store + dedup
    def __init__(self, db_path: str):
        self.db_path = db_path
        d = os.path.dirname(db_path)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS events(
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,   -- ISO8601 string
                    source TEXT NOT NULL,
                    payload TEXT NOT NULL,     -- JSON serialized
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(topic, event_id)
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_processed_at ON events(processed_at)"
            )

    def insert_if_absent(self, evt: Dict[str, Any]) -> bool:
        # insert event
        topic = evt["topic"]
        event_id = evt["event_id"]
        ts = str(evt["timestamp"])
        src = evt["source"]
        payload_json = json.dumps(evt.get("payload", {}), ensure_ascii=False, separators=(",", ":"))

        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO events(topic,event_id,timestamp,source,payload) VALUES(?,?,?,?,?)",
                    (topic, event_id, ts, src, payload_json),
                )
                self._conn.commit()
                return cur.rowcount == 1
            finally:
                cur.close()

    def contains(self, topic: str, event_id: str) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT 1 FROM events WHERE topic=? AND event_id=? LIMIT 1", (topic, event_id))
                return cur.fetchone() is not None
            finally:
                cur.close()

    def count_unique(self) -> int:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM events")
                (n,) = cur.fetchone()
                return int(n)
            finally:
                cur.close()

    def get_topics(self) -> List[str]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT DISTINCT topic FROM events ORDER BY topic ASC")
                return [row[0] for row in cur.fetchall()]
            finally:
                cur.close()

    def get_events(self, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                if topic:
                    cur.execute(
                        "SELECT topic,event_id,timestamp,source,payload,processed_at "
                        "FROM events WHERE topic=? ORDER BY processed_at ASC",
                        (topic,),
                    )
                else:
                    cur.execute(
                        "SELECT topic,event_id,timestamp,source,payload,processed_at "
                        "FROM events ORDER BY processed_at ASC"
                    )
                rows = cur.fetchall()
            finally:
                cur.close()

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "topic": r[0],
                "event_id": r[1],
                "timestamp": r[2],
                "source": r[3],
                "payload": json.loads(r[4]) if r[4] else {},
                "processed_at": r[5],
            })
        return out

    def close(self):
        with self._lock:
            self._conn.close()