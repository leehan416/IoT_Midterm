from paho.mqtt.client import MQTT_ERR_SUCCESS

from app.config.mqtt import get_mqtt_client
from app.config.redis import get_redis_client
from app.config.settings import get_settings


def check_health() -> dict[str, str]:
    return {"status": "ok"}


def get_redis_db_value(db: int) -> dict[str, str | int]:
    if db < 0:
        raise ValueError("db must be >= 0")

    client = get_redis_client(db)
    key = "iot:db_number"
    client.set(key, str(db))
    value = client.get(key)

    return {
        "status": "ok",
        "db": db,
        "value": value if value is not None else "",
    }


def publish_mqtt_message(topic: str, message: str) -> dict[str, str]:
    settings = get_settings()
    full_topic = f"{settings.mqtt_topic_prefix}/{topic}".strip("/")
    client = get_mqtt_client(client_id="iot-midterm-server")

    try:
        result = client.publish(full_topic, payload=message, qos=0, retain=False)
        if result.rc != MQTT_ERR_SUCCESS:
            raise RuntimeError(f"mqtt publish failed: rc={result.rc}")
    finally:
        client.disconnect()

    return {"status": "ok", "topic": full_topic, "message": message}
