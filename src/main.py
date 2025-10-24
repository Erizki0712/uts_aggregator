import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from .models import Event
from .store import EventStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aggregator")
class AppState:
    def __init__(self, db_path: str, workers: int = 2):
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self.store = EventStore(db_path)
        self.received: int = 0
        self.duplicate_dropped: int = 0
        self.start_time = time.time()
        self.workers = workers
        self.consumer_tasks: List[asyncio.Task] = []


state: Optional[AppState] = None

# workers

async def consumer_worker(name: str, st: AppState):
    logger.info(f"Consumer {name} started")
    while True:
        evt = await st.queue.get()
        try:
            is_new = st.store.insert_if_absent(evt)  # persist + idempotent
            if not is_new:
                st.duplicate_dropped += 1
                logger.info(f"[DUP] drop {evt['topic']}/{evt['event_id']}")
        except Exception:
            logger.exception("Consumer error")
        finally:
            st.queue.task_done()


def _resolve_db_path() -> str:
    return os.getenv("DB_PATH") or os.getenv("DEDUP_DB_PATH") or os.path.join(".", "data", "store.db")


async def _start_workers(st: AppState):
    for i in range(st.workers):
        t = asyncio.create_task(consumer_worker(f"w{i+1}", st))
        st.consumer_tasks.append(t)


def ensure_state() -> AppState:
    global state
    if state is None:
        db_path = _resolve_db_path()
        workers = int(os.getenv("CONSUMER_WORKERS", "2"))
        state = AppState(db_path=db_path, workers=workers)
        asyncio.create_task(_start_workers(state))
    return state

# app session
@asynccontextmanager
async def app_lifespan(app: FastAPI):
    global state
    db_path = _resolve_db_path()
    workers = int(os.getenv("CONSUMER_WORKERS", "2"))
    state = AppState(db_path=db_path, workers=workers)
    await _start_workers(state)
    try:
        yield
    finally:
        for t in state.consumer_tasks:
            t.cancel()
        state.store.close()
        state = None


app = FastAPI(title="Pub-Sub Log Aggregator (Persistent)", version="3.0.0", lifespan=app_lifespan)


# helpers

def _coerce_events(body: Any) -> List[Event]:
    # validasi
    if isinstance(body, dict):
        body = [body]
    if not isinstance(body, list):
        raise HTTPException(status_code=400, detail="Body must be JSON object or array")
    events: List[Event] = []
    for item in body:
        try:
            ev = Event.model_validate(item)
            events.append(ev)
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Invalid event: {e}")
    return events


# routes
@app.post("/publish")
async def publish(request: Request):
    st = state or ensure_state()
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    events = _coerce_events(body)
    for ev in events:
        await st.queue.put(ev.model_dump())
    st.received += len(events)
    return JSONResponse({"accepted": len(events), "queued": st.queue.qsize()})


@app.get("/events")
async def get_events(topic: Optional[str] = Query(default=None)):
    st = state or ensure_state()
    return st.store.get_events(topic=topic)


@app.get("/stats")
async def get_stats():
    st = state or ensure_state()
    uptime = time.time() - st.start_time
    topics = st.store.get_topics()
    return {
        "received": st.received,
        "unique_processed": st.store.count_unique(),
        "duplicate_dropped": st.duplicate_dropped,
        "topics": topics,
        "uptime_seconds": round(uptime, 3),
        "queue_depth": st.queue.qsize(),
        "workers": st.workers,
        "db_path": _resolve_db_path(),
    }


@app.get("/healthz")
async def health():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=False, log_level="info")