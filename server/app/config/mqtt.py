import paho.mqtt.client as mqtt

from app.config.settings import get_settings


def get_mqtt_client(client_id: str) -> mqtt.Client:
    settings = get_settings()
    client = mqtt.Client(client_id=client_id)
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)
    return client
