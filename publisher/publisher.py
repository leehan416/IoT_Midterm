import json
import logging
import os
import time
import threading
import requests
import paho.mqtt.client as mqtt
import io
import base64
from collections import deque
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SERVER_URL = os.getenv("SERVER_URL", "http://localhost:80")
PUBLISHER_ID = os.getenv("PUBLISHER_ID", "camera-1")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "iot/video/stream")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))
MQTT_HOST_OVERRIDE = os.getenv("MQTT_HOST_OVERRIDE", "")

TOPIC = f"{TOPIC_PREFIX}/{PUBLISHER_ID}"

local_queue: deque[str] = deque()
is_connected = threading.Event()
is_lock = threading.Lock()
current_broker: dict = {}

# =========================
# 📷 Camera Auto Detection
# =========================
CAMERA_MODE = "file"

try:
    from picamera2 import Picamera2
    picam2 = Picamera2()
    picam2.configure(picam2.create_still_configuration(
        main={"size": (640, 480)}
    ))
    picam2.start()
    CAMERA_MODE = "picamera2"
    log.info("Using Picamera2")
except Exception:
    try:
        from picamera import PiCamera
        camera = PiCamera()
        camera.resolution = (640, 480)
        time.sleep(2)
        CAMERA_MODE = "picamera"
        log.info("Using PiCamera")
    except Exception:
        log.warning("No camera available → using test.png")
        CAMERA_MODE = "file"


# =========================
# 🔥 Host Resolver (핵심)
# =========================
def resolve_host(broker_host: str) -> str:
    if MQTT_HOST_OVERRIDE:
        return MQTT_HOST_OVERRIDE

    # mosquitto-1 같은 Docker 내부 이름이면
    if broker_host.startswith("mosquitto"):
        parsed = urlparse(SERVER_URL)
        fallback_host = parsed.hostname
        log.warning(f"Broker host '{broker_host}' not resolvable → using {fallback_host}")
        return fallback_host

    return broker_host


# =========================
# MQTT Callbacks
# =========================
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        log.info(f"Connected to broker: {current_broker.get('host')} (topic: {TOPIC})")
        is_connected.set()
        flush_local_queue(client)
    else:
        log.error(f"Connection failed: rc={rc}")
        is_connected.clear()


def on_disconnect(client, userdata, flags, rc, properties):
    if rc != 0:
        log.warning(f"Unexpected disconnection (rc={rc}) → failover 시작")
        is_connected.clear()
        threading.Thread(target=failover, args=(client,), daemon=True).start()


def on_publish(client, userdata, mid, rc, properties):
    log.debug(f"PUBACK received (mid={mid})")


# =========================
# Server Communication
# =========================
def receive_broker_info() -> dict:
    for attempt in range(1, 6):
        try:
            res = requests.get(f"{SERVER_URL}/api/mqtt", timeout=5)
            res.raise_for_status()
            broker = res.json()
            log.info(f"Broker info received: {broker}")
            return broker
        except Exception as e:
            log.warning(f"Failed to get broker info ({attempt}/5): {e}")
            time.sleep(3)
    raise RuntimeError("Unable to retrieve broker info from server.")


def register_to_server(broker_id: int) -> None:
    for attempt in range(1, 6):
        try:
            res = requests.post(
                f"{SERVER_URL}/api/mqtt",
                json={"id": broker_id, "topic": TOPIC},
                timeout=5,
            )
            res.raise_for_status()
            log.info(f"Registered to server (broker_id={broker_id}, topic={TOPIC})")
            return
        except Exception as e:
            log.warning(f"Failed to register to server ({attempt}/5): {e}")
            time.sleep(3)
    raise RuntimeError("Unable to register to server.")


# =========================
# Failover
# =========================
def failover(client: mqtt.Client):
    global current_broker

    for attempt in range(1, 11):
        try:
            broker = receive_broker_info()
            host = resolve_host(broker["host"])

            client.disconnect()
            client.connect(host, broker["port"], keepalive=60)

            if is_connected.wait(timeout=5.0):
                current_broker = broker
                log.info(f"Failover success → {host}:{broker['port']}")
                return

        except Exception as e:
            log.warning(f"Failover attempt ({attempt}/10) failed: {e}")
            time.sleep(3)

    log.error("Failover exhausted. Messages preserved in local_queue.")


# =========================
# Queue Flush
# =========================
def flush_local_queue(client: mqtt.Client):
    with is_lock:
        count = len(local_queue)
    if count == 0:
        return

    log.info(f"Flushing local_queue: {count} messages")
    while True:
        with is_lock:
            if not local_queue:
                break
            payload = local_queue.popleft()
        result = client.publish(TOPIC, payload, qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            with is_lock:
                local_queue.appendleft(payload)
            log.error("Flush failed, stopping flush.")
            break


# =========================
# Payload Builder
# =========================
def build_payload() -> str:
    buf = io.BytesIO()

    if CAMERA_MODE == "picamera2":
        picam2.capture_file(buf, format="jpeg")

    elif CAMERA_MODE == "picamera":
        camera.capture(buf, format="jpeg")

    else:
        with open("test.png", "rb") as f:
            buf.write(f.read())

    img_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

    data = {
        "publisher_id": PUBLISHER_ID,
        "topic":        TOPIC,
        "timestamp":    time.time(),
        "data": {
            "image":  img_b64,
            "width":  640,
            "height": 480,
            "format": "jpeg",
        },
    }
    return json.dumps(data)


# =========================
# Main
# =========================
def main():
    global current_broker

    broker = receive_broker_info()
    register_to_server(broker["id"])
    current_broker = broker

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=PUBLISHER_ID,
        protocol=mqtt.MQTTv311,
        clean_session=False,
    )
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish    = on_publish
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    mqtt_host = resolve_host(broker["host"])
    log.info(f"Connecting to {mqtt_host}:{broker['port']}")

    client.connect(mqtt_host, broker["port"], keepalive=60)
    client.loop_start()

    log.info(f"Publishing started (interval={PUBLISH_INTERVAL}s)")

    try:
        while True:
            payload = build_payload()

            if not is_connected.is_set():
                with is_lock:
                    local_queue.append(payload)
                log.warning(f"Broker unavailable → queued (queue_size={len(local_queue)})")
            else:
                result = client.publish(TOPIC, payload, qos=1)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    log.info(f"Published (topic={TOPIC})")
                else:
                    with is_lock:
                        local_queue.append(payload)
                    log.error(f"Publish failed (rc={result.rc}) → queued")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        if CAMERA_MODE == "picamera2":
            picam2.stop()
        elif CAMERA_MODE == "picamera":
            camera.close()

        client.loop_stop()
        client.disconnect()
        log.info("Disconnected.")


if __name__ == "__main__":
    main()