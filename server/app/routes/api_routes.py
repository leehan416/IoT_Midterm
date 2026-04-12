import asyncio
import base64
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.schemas.comon_schemas import *
from app.schemas.mqtt_schemas import *

import app.services.comon_service as comon_service
import app.services.mqtt_service as mqtt_service
from app.config.settings import settings
from app.services.video_stream_hub import video_stream_hub

router = APIRouter(prefix="/api", tags=["api"])
logger = logging.getLogger(__name__)


########################################################################################################################
# 기본 api
@router.get("/health")
async def healthcheck_api() -> HealthCheckResponse:
    return await comon_service.check_health()


########################################################################################################################
# mqtt

@router.get("/mqtt")
async def get_mqtt_broker_data_api() -> MQTTDataResponse:
    return await mqtt_service.get_mqtt_broker_data()


@router.post("/mqtt")
async def set_mqtt_broker_data_api(
        request_data: MQTTConnectedDataRequest) -> MQTTStatusResponse:
    return await mqtt_service.set_mqtt_broker_data(request_data)


@router.get("/mqtt/status")
async def get_mqtt_broker_status_api() -> list[MQTTStatusResponse]:
    return await mqtt_service.get_mqtt_status()


@router.patch("/mqtt/active")
async def set_mqtt_broker_active_api(
        request_data: MQTTActiveRequest) -> MQTTStatusResponse:
    return await mqtt_service.set_mqtt_active(request_data)

########################################################################################################################
# video streaming

@router.websocket("/ws/video/{camera_id}")
async def video_ws_api(websocket: WebSocket, camera_id: str):
    await websocket.accept()
    queue = await video_stream_hub.subscribe(camera_id)

    try:
        latest_frame = video_stream_hub.get_latest_frame(camera_id)
        if latest_frame is not None:
            await websocket.send_json(
                {
                    "type": "frame",
                    "camera_id": camera_id,
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
                        "camera_id": camera_id,
                        "timestamp": event.timestamp,
                        "image": base64.b64encode(event.frame_bytes).decode("utf-8"),
                    }
                )
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "heartbeat", "timestamp": time.time()})
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected camera_id=%s", camera_id)
    except Exception:
        logger.exception("WebSocket streaming error camera_id=%s", camera_id)
    finally:
        await video_stream_hub.unsubscribe(camera_id, queue)

########################################################################################################################
