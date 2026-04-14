import asyncio
import base64
import logging
import time

from fastapi import WebSocket, WebSocketDisconnect

from app.config.settings import settings
from app.services.video_stream_hub import video_stream_hub

logger = logging.getLogger(__name__)


async def stream_video_websocket(websocket: WebSocket, publisher_id: str) -> None:
    await websocket.accept()
    queue = await video_stream_hub.subscribe(publisher_id)
    try:
        latest_frame = video_stream_hub.get_latest_frame(publisher_id)
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
        await video_stream_hub.unsubscribe(publisher_id, queue)
