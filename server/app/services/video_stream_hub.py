from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FrameEvent:
    camera_id: str
    frame_bytes: bytes
    timestamp: float


_loop: asyncio.AbstractEventLoop | None = None
_lock = threading.Lock()
_latest_frames: dict[str, FrameEvent] = {}
_subscribers: dict[str, set[asyncio.Queue[FrameEvent]]] = defaultdict(set)


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _loop
    _loop = loop


def get_latest_frame(camera_id: str) -> FrameEvent | None:
    with _lock:
        return _latest_frames.get(camera_id)


async def subscribe(camera_id: str) -> asyncio.Queue[FrameEvent]:
    queue: asyncio.Queue[FrameEvent] = asyncio.Queue(maxsize=2)
    with _lock:
        _subscribers[camera_id].add(queue)
    return queue


async def unsubscribe(camera_id: str, queue: asyncio.Queue[FrameEvent]) -> None:
    with _lock:
        subscribers = _subscribers.get(camera_id)
        if not subscribers:
            return
        subscribers.discard(queue)
        if not subscribers:
            _subscribers.pop(camera_id, None)


def publish_frame(camera_id: str, frame_bytes: bytes) -> None:
    event = FrameEvent(camera_id=camera_id, frame_bytes=frame_bytes, timestamp=time.time())
    with _lock:
        _latest_frames[camera_id] = event
        subscriber_queues = list(_subscribers.get(camera_id, set()))

    if not subscriber_queues:
        return

    if _loop is None:
        logger.warning("VideoStreamHub loop is not set; skipping push for camera_id=%s", camera_id)
        return

    for queue in subscriber_queues:
        _loop.call_soon_threadsafe(_push_non_blocking, queue, event)


def _push_non_blocking(queue: asyncio.Queue[FrameEvent], event: FrameEvent) -> None:
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        queue.put_nowait(event)
    except asyncio.QueueFull:
        # Queue size is intentionally tiny; dropping old frame is acceptable.
        pass
