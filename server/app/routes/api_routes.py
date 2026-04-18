from fastapi import APIRouter, Request, WebSocket

from app.schemas.comon_schemas import *
from app.schemas.mqtt_schemas import *

import app.services.comon_service as comon_service
import app.services.mqtt_service as mqtt_service
from app.services.video_stream_service import stream_video_websocket

router = APIRouter(prefix="/api", tags=["api"])


########################################################################################################################
# 기본 api
@router.get("/health")
async def healthcheck_api() -> HealthCheckResponse:
    return await comon_service.check_health()


########################################################################################################################
# publisher

@router.get("/publisher")
@router.get("/publisher/all")
async def get_all_publisher_data_api() -> list[PublisherResponse]:
    return await mqtt_service.get_all_publishers()
########################################################################################################################

@router.post("/publisher")
async def set_mqtt_broker_data_api(
        request_data: MQTTConnectedDataRequest) -> MQTTStatusResponse:
    return await mqtt_service.set_mqtt_broker_data(request_data)


########################################################################################################################
# mqtt

@router.get("/mqtt")
async def get_mqtt_broker_data_api(request: Request) -> MQTTDataResponse:
    request_host = request.headers.get("x-forwarded-host") or request.url.hostname
    if request_host and ":" in request_host:
        request_host = request_host.split(":", 1)[0]
    return await mqtt_service.get_mqtt_broker_data(request_host=request_host)


@router.post("/mqtt")
async def set_mqtt_broker_data_api(
        request_data: MQTTConnectedDataRequest) -> MQTTStatusResponse:
    return await mqtt_service.set_mqtt_broker_data(request_data)

@router.post("/mqtt/add")
async def add_mqtt_broker_api(
        request_data: MQTTAddRequest) -> MQTTDataResponse:
    return await mqtt_service.add_mqtt_broker(request_data)




@router.get("/mqtt/status")
async def get_mqtt_broker_status_api(request: Request) -> list[MQTTStatusResponse]:
    request_host = request.headers.get("x-forwarded-host") or request.url.hostname
    if request_host and ":" in request_host:
        request_host = request_host.split(":", 1)[0]
    return await mqtt_service.get_mqtt_status(request_host=request_host)


@router.patch("/mqtt/active")
async def set_mqtt_broker_active_api(
        request_data: MQTTActiveRequest) -> MQTTStatusResponse:
    return await mqtt_service.set_mqtt_active(request_data)


@router.delete("/mqtt/{broker_id}")
async def delete_mqtt_broker_api(broker_id: int) -> MQTTStatusResponse:
    return await mqtt_service.delete_mqtt_broker(broker_id)

########################################################################################################################
# video streaming

@router.websocket("/ws/video/{publisher_id}")
async def video_ws_api(websocket: WebSocket, publisher_id: str):
    await stream_video_websocket(websocket, publisher_id)

########################################################################################################################
