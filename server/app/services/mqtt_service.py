import asyncio
import logging

from fastapi import HTTPException

from app.models.mqtt_broker import MQTTBroker
import app.repository.mqtt_repository as mqtt_repository
from app.schemas.mqtt_schemas import MQTTActiveRequest, MQTTAddRequest, MQTTDataResponse, MQTTStatusResponse
from app.services import publisher_service

logger = logging.getLogger(__name__)


########################################################################################################################
# crud
async def add_mqtt_broker(request_data: MQTTAddRequest) -> MQTTDataResponse:
    """새 MQTT 브로커를 등록하는 함수.
    중복 브로커를 검사한 뒤 상태 체크 결과와 함께 저장한다.
    """
    broker: MQTTBroker = MQTTBroker(
        host=request_data.mqtt_host,
        port=request_data.mqtt_port,
        is_active=await check_broker_status(request_data.mqtt_host, request_data.mqtt_port),
    )
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    for mqtt in mqtt_list:
        if mqtt.host == request_data.mqtt_host and mqtt.port == request_data.mqtt_port:
            raise HTTPException(409, "broker already exist")
    await mqtt_repository.save_mqtt_data(broker)
    return MQTTDataResponse.model_validate(broker)


async def get_mqtt_broker_data(request_host: str | None = None) -> MQTTDataResponse:
    """활성 MQTT 브로커 중 연결 가능한 브로커 정보를 반환하는 함수.
    실시간 포트 체크를 통과한 첫 브로커를 응답으로 반환한다.
    """
    del request_host
    await publisher_service.sync_connected_publisher_counts()
    await sync_mqtt_broker_statuses(timeout=0.5)
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    active_brokers = [broker for broker in mqtt_list if broker.is_active]
    if not active_brokers:
        raise HTTPException(status_code=404, detail="No active MQTT broker data found")
    active_brokers.sort(key=lambda broker: broker.connected_publisher)
    return MQTTDataResponse.model_validate(active_brokers[0])


async def delete_mqtt_broker(broker_id: int) -> MQTTStatusResponse:
    """MQTT 브로커를 삭제하는 함수."""
    broker = await mqtt_repository.get_mqtt_data_by_id(broker_id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")

    await mqtt_repository.delete_mqtt_data_by_id(broker_id)
    return MQTTStatusResponse.model_validate(broker)


########################################################################################################################
# status

async def get_mqtt_status() -> list[MQTTStatusResponse]:
    """전체 MQTT 브로커 상태 목록을 조회하는 함수.
    브로커별 연결 퍼블리셔 수를 동기화한 뒤 정렬된 상태를 반환한다.
    """
    response_items: list[MQTTStatusResponse] = []
    await publisher_service.sync_connected_publisher_counts()
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    mqtt_list.sort(key=lambda broker: broker.id)
    for mqtt in mqtt_list:
        response_items.append(MQTTStatusResponse.model_validate(mqtt))
    return response_items


async def check_broker_status(host: str, port: int, timeout: float = 2.0) -> bool:
    """상대방의 포트가 열려있는지 확인하는 함수
        연결이 성공하면 true를 반환하고, 실패하면 false를 반환한다.
        기본 timeout은 2초로 설정됨.
    """
    try:
        _reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def set_mqtt_active(request_data: MQTTActiveRequest) -> MQTTStatusResponse:
    """특정 MQTT 브로커의 활성화 상태를 변경하는 함수.
    요청된 `is_active` 값으로 상태를 저장하고 최신 상태를 반환한다.
    """
    broker = await mqtt_repository.get_mqtt_data_by_id(request_data.id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")
    broker.is_active = request_data.is_active
    return MQTTStatusResponse.model_validate(await mqtt_repository.save_mqtt_data(broker))


async def sync_mqtt_broker_statuses(timeout: float = 2.0) -> None:
    """모든 MQTT 브로커의 실시간 연결 상태를 동기화하는 함수.
    상태가 변경된 브로커만 저장하고 변경 로그를 남긴다.
    """
    brokers = await mqtt_repository.get_all_mqtt_datas()
    for broker in brokers:
        is_active = await check_broker_status(broker.host, broker.port, timeout=timeout)
        if broker.is_active == is_active:
            continue
        logger.info(
            "Broker %s (%s:%s) changed status: %s -> %s",
            broker.id,
            broker.host,
            broker.port,
            broker.is_active,
            is_active,
        )
        broker.is_active = is_active
        await mqtt_repository.save_mqtt_data(broker)
