from __future__ import annotations

import asyncio
import base64
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass

from fastapi import WebSocket, WebSocketDisconnect

from app.config.settings import settings

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
    """비디오 스트림 전파에 사용할 이벤트 루프를 설정하는 함수.
    MQTT 콜백 스레드에서 안전하게 큐 이벤트를 전달할 때 사용한다.
    """
    global _loop
    _loop = loop


def get_latest_frame(camera_id: str) -> FrameEvent | None:
    """특정 카메라의 최신 프레임 이벤트를 조회하는 함수.
    저장된 최신 프레임이 없으면 `None`을 반환한다.
    """
    with _lock:
        return _latest_frames.get(camera_id)


async def subscribe(camera_id: str) -> asyncio.Queue[FrameEvent]:
    """카메라 스트림 구독용 큐를 생성하고 등록하는 함수.
    호출자는 반환된 큐를 통해 프레임 이벤트를 비동기로 수신한다.
    """
    queue: asyncio.Queue[FrameEvent] = asyncio.Queue(maxsize=2)
    with _lock:
        _subscribers[camera_id].add(queue)
    return queue


async def unsubscribe(camera_id: str, queue: asyncio.Queue[FrameEvent]) -> None:
    """카메라 스트림 구독 큐를 해제하는 함수.
    마지막 구독자가 제거되면 해당 카메라의 구독 목록도 정리한다.
    """
    with _lock:
        subscribers = _subscribers.get(camera_id)
        if not subscribers:
            return
        subscribers.discard(queue)
        if not subscribers:
            _subscribers.pop(camera_id, None)


def publish_frame(camera_id: str, frame_bytes: bytes) -> None:
    """새 프레임을 최신 상태로 저장하고 구독자에게 전파하는 함수.
    이벤트 루프가 설정된 경우 각 구독 큐에 non-blocking 방식으로 전달한다.
    """
    event = FrameEvent(camera_id=camera_id, frame_bytes=frame_bytes, timestamp=time.time())
    with _lock:
        _latest_frames[camera_id] = event
        subscriber_queues = list(_subscribers.get(camera_id, set()))

    if not subscriber_queues:
        return
    if _loop is None:
        logger.warning("Video stream loop is not set; skipping push for camera_id=%s", camera_id)
        return

    for queue in subscriber_queues:
        _loop.call_soon_threadsafe(_push_non_blocking, queue, event)


def _push_non_blocking(queue: asyncio.Queue[FrameEvent], event: FrameEvent) -> None:
    """큐가 가득 찬 경우 오래된 프레임을 버리고 새 프레임을 넣는 함수.
    실시간성을 위해 손실을 허용하는 방식으로 큐 적재를 시도한다.
    """
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


async def stream_video_websocket(websocket: WebSocket, publisher_id: str) -> None:
    """웹소켓으로 퍼블리셔 영상 프레임을 스트리밍하는 함수.
    최신 프레임 즉시 전송 후, 신규 프레임 또는 heartbeat 메시지를 지속 전송한다.
    """
    await websocket.accept()
    queue = await subscribe(publisher_id)
    try:
        latest_frame = get_latest_frame(publisher_id)
        if latest_frame is not None:
            await websocket.send_json(
                {
                    "type": "frame",
                    "publisher_id": publisher_id,
                    "timestamp": latest_frame.timestamp,
                    "image": base64.b64encode(latest_frame.frame_bytes).decode("utf-8"),
                }
            )

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=settings.ws_heartbeat_seconds)
                await websocket.send_json(
                    {
                        "type": "frame",
                        "publisher_id": publisher_id,
                        "timestamp": event.timestamp,
                        "image": base64.b64encode(event.frame_bytes).decode("utf-8"),
                    }
                )
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "heartbeat", "timestamp": time.time()})
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected publisher_id=%s", publisher_id)
    except Exception:
        logger.exception("WebSocket streaming error publisher_id=%s", publisher_id)
    finally:
        await unsubscribe(publisher_id, queue)
