from app.models.mqtt_broker import MQTTBroker
from app.models.publisher import Publisher
from app.repository import publisher_repository
from app.schemas.mqtt_schemas import *
from app.config.settings import settings
from fastapi import HTTPException
import asyncio
import logging

import app.repository.mqtt_repository as mqtt_repository
from app.scheduler.mqtt_checker import check_broker_status
from app.services.mqtt_subscriber_service import mqtt_subscriber_service

logger = logging.getLogger(__name__)


async def get_mqtt_broker_data(request_host: str | None = None) -> MQTTDataResponse:
    await _sync_connected_publisher_counts()
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    active_brokers = [broker for broker in mqtt_list if broker.is_active]
    if not active_brokers:
        raise HTTPException(status_code=404, detail="No active MQTT broker data found")
    active_brokers.sort(key=lambda broker: broker.connected_publisher)

    # broker list를 반환하기 전에 status check
    for broker in active_brokers:
        is_truly_active = await check_broker_status(broker.check_host, broker.check_port, timeout=0.5)
        if is_truly_active:
            if broker.host == "localhost":
                broker = broker.model_copy(update={"host": _resolve_response_host(request_host)})
            return MQTTDataResponse.model_validate(broker)
        else:
            # 만약에 broker가 죽었다면, db update
            broker.is_active = False
            await mqtt_repository.save_mqtt_data(broker)

    # 모든 broker가 죽었다면 404 error 반환
    raise HTTPException(status_code=404, detail="No active MQTT broker data found after real-time status check")


async def set_mqtt_broker_data(
        request_data: MQTTConnectedDataRequest) -> MQTTStatusResponse:
    broker = await mqtt_repository.get_mqtt_data_by_id(request_data.broker_id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")
    if not broker.is_active:
        raise HTTPException(status_code=409, detail="MQTT broker is inactive")
    normalized_topic = request_data.topic.strip()
    if not normalized_topic:
        raise HTTPException(status_code=400, detail="topic is required")
    publishers: list[Publisher] = await publisher_repository.get_all_publisher_data()
    for pub in publishers:
        if pub.topic == normalized_topic:
            raise HTTPException(status_code=409, detail="topic already using")
    publisher = Publisher(
        broker_id=broker.id,
        host=request_data.publisher_host or "unknown",
        topic=normalized_topic,
    )
    await publisher_repository.save_publisher_data(publisher)
    mqtt_subscriber_service.register_publisher_topic(publisher.id, normalized_topic)
    broker.connected_publisher += 1
    saved = await mqtt_repository.save_mqtt_data(broker)
    return MQTTStatusResponse.model_validate(saved)


async def get_mqtt_status(request_host: str | None = None) -> list[MQTTStatusResponse]:
    await _sync_connected_publisher_counts()
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    mqtt_list.sort(key=lambda broker: broker.id)
    response_items: list[MQTTStatusResponse] = []
    for broker in mqtt_list:
        if broker.host == "localhost":
            broker = broker.model_copy(update={"host": _resolve_response_host(request_host)})
        response_items.append(MQTTStatusResponse.model_validate(broker))
    return response_items


async def set_mqtt_active(request_data: MQTTActiveRequest) -> MQTTStatusResponse:
    broker = await mqtt_repository.get_mqtt_data_by_id(request_data.id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")
    broker.is_active = request_data.is_active
    return MQTTStatusResponse.model_validate(await mqtt_repository.save_mqtt_data(broker))


async def get_all_publishers() -> list[PublisherResponse]:
    publishers = await publisher_repository.get_all_publisher_data()
    brokers = await mqtt_repository.get_all_mqtt_datas()
    broker_map = {broker.id: broker for broker in brokers}
    return [
        PublisherResponse(
            id=pub.id,
            broker_id=pub.broker_id,
            host=pub.host,
            topic=pub.topic,
            broker_host=broker_map[pub.broker_id].host if pub.broker_id in broker_map else "unknown",
            broker_port=broker_map[pub.broker_id].port if pub.broker_id in broker_map else -1,
            broker_is_active=broker_map[pub.broker_id].is_active if pub.broker_id in broker_map else False,
        )
        for pub in publishers
    ]


async def restore_publisher_subscriptions() -> None:
    publishers = await publisher_repository.get_all_publisher_data()
    for publisher in publishers:
        mqtt_subscriber_service.register_publisher_topic(publisher.id, publisher.topic)


async def publisher_subscription_sync_worker(interval: int = 3) -> None:
    while True:
        try:
            await restore_publisher_subscriptions()
        except Exception as e:
            logger.error("publisher subscription sync failed: %s", e)
        await asyncio.sleep(interval)


async def _sync_connected_publisher_counts() -> None:
    brokers = await mqtt_repository.get_all_mqtt_datas()
    publishers = await publisher_repository.get_all_publisher_data()

    broker_counts: dict[int, int] = {}
    for pub in publishers:
        broker_counts[pub.broker_id] = broker_counts.get(pub.broker_id, 0) + 1

    for broker in brokers:
        actual_count = broker_counts.get(broker.id, 0)
        if broker.connected_publisher != actual_count:
            broker.connected_publisher = actual_count
            await mqtt_repository.save_mqtt_data(broker)


def _parse_mqtt_brokers() -> list[tuple[str, int]]:
    if settings.mqtt_brokers.strip():
        brokers: list[tuple[str, int]] = []
        for item in settings.mqtt_brokers.split(","):
            host, port = item.strip().split(":")
            brokers.append((host.strip(), int(port.strip())))
        return brokers

    if settings.mqtt_broker_count <= 1:
        return [(_resolve_advertised_host(), settings.mqtt_advertised_port_start)]

    return [
        (
            _resolve_advertised_host(),
            settings.mqtt_advertised_port_start + idx - 1,
        )
        for idx in range(1, settings.mqtt_broker_count + 1)
    ]


def _resolve_advertised_host() -> str:
    if settings.mqtt_advertised_host == "localhost" and settings.mqtt_default_host.strip():
        return settings.mqtt_default_host.strip()
    return settings.mqtt_advertised_host


def _resolve_response_host(request_host: str | None) -> str:
    if settings.mqtt_default_host.strip():
        return settings.mqtt_default_host.strip()
    if request_host:
        return request_host
    return "localhost"


async def register_mqtt_brokers() -> None:
    await mqtt_repository.clear_mqtt_datas()

    advertised_brokers = _parse_mqtt_brokers()
    for idx, (host, port) in enumerate(advertised_brokers, start=1):
        check_host = (
            settings.mqtt_host
            if settings.mqtt_broker_count <= 1
            else f"{settings.mqtt_broker_name_prefix}-{idx}"
        )
        broker = MQTTBroker(
            id=idx,
            host=host,
            port=port,
            check_host=check_host,
            check_port=settings.mqtt_port,
            is_active=True,
            connected_publisher=0,
        )
        await mqtt_repository.save_mqtt_data(broker)


async def add_mqtt_broker(request_data: MQTTAddRequest) -> MQTTDataResponse:
    broker: MQTTBroker = MQTTBroker(
        host=request_data.mqtt_host,
        check_host=request_data.mqtt_host,
        port=request_data.mqtt_port,
        check_port=request_data.mqtt_port,
    )
    broker.is_active = await check_broker_status(broker.host, broker.port)
    await mqtt_repository.save_mqtt_data(broker)
    return MQTTDataResponse.from_orm(broker)


async def check_broker_status(host: str, port: int, timeout: float = 2.0) -> bool:
    """상대방의 포트가 열려있는지 확인하는 함수
        연결이 성공하면 true를 반환하고, 실패하면 false를 반환한다.
        기본 timeout은 2초로 설정됨.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False
