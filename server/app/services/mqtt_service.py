from app.models.mqtt_broker import MQTTBroker
from app.schemas.mqtt_schemas import *
from app.config.settings import settings
from fastapi import HTTPException

import app.repository.mqtt_repository as mqtt_repository


async def get_mqtt_broker_data() -> MQTTDataResponse:
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    active_brokers = [broker for broker in mqtt_list if broker.is_active]
    if not active_brokers:
        raise HTTPException(status_code=404, detail="No active MQTT broker data found")
    active_brokers.sort(key=lambda broker: broker.connected_publisher, reverse=True)
    return MQTTDataResponse.model_validate(active_brokers[0])


async def set_mqtt_broker_data(
        request_data: MQTTConnectedDataRequest) -> MQTTStatusResponse:
    broker = await mqtt_repository.get_mqtt_data_by_id(request_data.id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")
    if not broker.is_active:
        raise HTTPException(status_code=409, detail="MQTT broker is inactive")
    broker.connected_publisher += 1
    return MQTTStatusResponse.model_validate(await mqtt_repository.save_mqtt_data(broker))


async def get_mqtt_status() -> list[MQTTStatusResponse]:
    mqtt_list: list[MQTTBroker] = await mqtt_repository.get_all_mqtt_datas()
    mqtt_list.sort(key=lambda broker: broker.id)
    return [MQTTStatusResponse.model_validate(broker) for broker in mqtt_list]


async def set_mqtt_active(request_data: MQTTActiveRequest) -> MQTTStatusResponse:
    broker = await mqtt_repository.get_mqtt_data_by_id(request_data.id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")
    broker.is_active = request_data.is_active
    return MQTTStatusResponse.model_validate(await mqtt_repository.save_mqtt_data(broker))


def _parse_mqtt_brokers() -> list[tuple[str, int]]:
    if settings.mqtt_brokers.strip():
        brokers: list[tuple[str, int]] = []
        for item in settings.mqtt_brokers.split(","):
            host, port = item.strip().split(":")
            brokers.append((host.strip(), int(port.strip())))
        return brokers

    if settings.mqtt_broker_count <= 1:
        return [(settings.mqtt_host, settings.mqtt_port)]

    return [
        (f"{settings.mqtt_broker_name_prefix}-{idx}", settings.mqtt_port)
        for idx in range(1, settings.mqtt_broker_count + 1)
    ]


async def register_mqtt_brokers() -> None:
    await mqtt_repository.clear_mqtt_datas()

    for idx, (host, port) in enumerate(_parse_mqtt_brokers(), start=1):
        broker = MQTTBroker(
            id=idx,
            host=host,
            port=port,
            is_active=True,
            connected_publisher=0,
        )
        await mqtt_repository.save_mqtt_data(broker)
