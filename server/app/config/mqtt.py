import paho.mqtt.client as mqtt

from app.config.settings import settings


def get_mqtt_client(
        client_id: str,
        on_connect=None,
        on_disconnect=None,
        on_message=None) -> mqtt.Client:
    client = mqtt.Client(client_id=client_id)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    if on_connect is not None:
        client.on_connect = on_connect
    if on_disconnect is not None:
        client.on_disconnect = on_disconnect
    if on_message is not None:
        client.on_message = on_message
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)
    return client
