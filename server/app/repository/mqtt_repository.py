from app.config.redis import get_redis_client
from app.models.mqtt_broker import MQTTBroker


REDIS_MQTT_DB = 1


def _build_broker(data: dict[str, str]) -> MQTTBroker:
    return MQTTBroker(
        id=int(data.get("id", 0)),
        connected_publisher=int(data.get("connected_publisher", 0)),
        host=data.get("host", "localhost"),
        port=int(data.get("port", 1883)),
        check_host=data.get("check_host", data.get("host", "localhost")),
        check_port=int(data.get("check_port", data.get("port", 1883))),
        is_active=bool(int(data.get("is_active", 1))),
    )


def _mqtt_key(broker_id: int) -> str:
    return f"mqtt_broker:{broker_id}"


async def get_mqtt_data_by_id(broker_id: int) -> MQTTBroker | None:
    redis_client = get_redis_client(REDIS_MQTT_DB)
    data = await redis_client.hgetall(_mqtt_key(broker_id))
    return _build_broker(data) if data else None


async def save_mqtt_data(mqtt_broker: MQTTBroker) -> MQTTBroker:
    redis_client = get_redis_client(REDIS_MQTT_DB)
    mqtt_broker.touch()
    await redis_client.hset(
        _mqtt_key(mqtt_broker.id),
        mapping={
            "id": mqtt_broker.id,
            "connected_publisher": mqtt_broker.connected_publisher,
            "host": mqtt_broker.host,
            "port": mqtt_broker.port,
            "check_host": mqtt_broker.check_host,
            "check_port": mqtt_broker.check_port,
            "is_active": int(mqtt_broker.is_active),
            "created_at": mqtt_broker.created_at.isoformat(),
            "updated_at": mqtt_broker.updated_at.isoformat(),
        },
    )
    return mqtt_broker


async def get_all_mqtt_datas() -> list[MQTTBroker]:
    redis_client = get_redis_client(REDIS_MQTT_DB)
    brokers: list[MQTTBroker] = []
    async for key in redis_client.scan_iter(match="mqtt_broker:*"):
        data = await redis_client.hgetall(key)
        if data:
            brokers.append(_build_broker(data))
    return brokers


async def clear_mqtt_datas() -> None:
    redis_client = get_redis_client(REDIS_MQTT_DB)
    await redis_client.flushdb()
