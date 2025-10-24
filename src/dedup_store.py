import os
import sqlite3
import threading
from typing import Tuple

class DedupStore:
    # dedup store key topic, event_id
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS dedup(
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(topic, event_id)
            )
        """)
        self._conn.commit()

    def insert_if_absent(self, topic: str, event_id: str) -> bool:
        # duplicate checker
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO dedup(topic, event_id) VALUES(?, ?)",
                    (topic, event_id),
                )
                self._conn.commit()
                return cur.rowcount == 1
            finally:
                cur.close()

    def contains(self, key: Tuple[str, str]) -> bool:
        with self._lock:
            (topic, event_id) = key
            cur = self._conn.cursor()
            try:
                cur.execute(
                    "SELECT 1 FROM dedup WHERE topic=? AND event_id=?",
                    (topic, event_id),
                )
                return cur.fetchone() is not None
            finally:
                cur.close()

    def count_unique(self) -> int:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM dedup")
                (c,) = cur.fetchone()
                return int(c)
            finally:
                cur.close()

    def close(self):
        with self._lock:
            self._conn.close()
