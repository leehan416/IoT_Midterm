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


class VideoStreamHub:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()
        self._latest_frames: dict[str, FrameEvent] = {}
        self._subscribers: dict[str, set[asyncio.Queue[FrameEvent]]] = defaultdict(set)

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def get_latest_frame(self, camera_id: str) -> FrameEvent | None:
        with self._lock:
            return self._latest_frames.get(camera_id)

    async def subscribe(self, camera_id: str) -> asyncio.Queue[FrameEvent]:
        queue: asyncio.Queue[FrameEvent] = asyncio.Queue(maxsize=2)
        with self._lock:
            self._subscribers[camera_id].add(queue)
        return queue

    async def unsubscribe(self, camera_id: str, queue: asyncio.Queue[FrameEvent]) -> None:
        with self._lock:
            subscribers = self._subscribers.get(camera_id)
            if not subscribers:
                return
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(camera_id, None)

    def publish_frame(self, camera_id: str, frame_bytes: bytes) -> None:
        event = FrameEvent(camera_id=camera_id, frame_bytes=frame_bytes, timestamp=time.time())
        with self._lock:
            self._latest_frames[camera_id] = event
            subscriber_queues = list(self._subscribers.get(camera_id, set()))

        if not subscriber_queues:
            return

        if self._loop is None:
            logger.warning("VideoStreamHub loop is not set; skipping push for camera_id=%s", camera_id)
            return

        for queue in subscriber_queues:
            self._loop.call_soon_threadsafe(self._push_non_blocking, queue, event)

    @staticmethod
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


video_stream_hub = VideoStreamHub()
