from fastapi import APIRouter

from app.schemas.comon_schemas import *
from app.schemas.mqtt_schemas import *

import app.services.comon_service as comon_service
import app.services.mqtt_service as mqtt_service

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
