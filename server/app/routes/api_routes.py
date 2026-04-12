from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.schemas.comon_schemas import *
from app.schemas.mqtt_schemas import *

import app.services.comon_service as comon_service
import app.services.mqtt_service as mqtt_service
import asyncio
from app.config.redis import get_redis_binary_client

router = APIRouter(prefix="/api", tags=["api"])


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

async def video_frame_generator(camera_id: str):
    """Subscribe to Redis Pub/Sub directly to receive C/C++ MQTT frames."""
    redis_client = get_redis_binary_client()
    pubsub = redis_client.pubsub()
    channel_name = f"video:stream:{camera_id}"
    
    await pubsub.subscribe(channel_name)
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                frame_data = message["data"]
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_data + b'\r\n')
    finally:
        await pubsub.unsubscribe(channel_name)
        await pubsub.close()

@router.get("/video/stream/{camera_id}")
async def video_stream_api(camera_id: str):
    return StreamingResponse(
        video_frame_generator(camera_id),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )

########################################################################################################################
