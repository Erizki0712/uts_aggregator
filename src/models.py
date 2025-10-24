from typing import Any, Dict
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

    model_config = ConfigDict(extra="forbid")
