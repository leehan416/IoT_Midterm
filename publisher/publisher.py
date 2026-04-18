import json
import logging
import os
import socket
import time
import threading
import requests
import paho.mqtt.client as mqtt
import base64
from collections import deque
from urllib.parse import urlparse
import subprocess
import random
import string

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SERVER_URL = os.getenv("SERVER_URL", "http://172.18.137.135:80")
PUBLISHER_ID = os.getenv("PUBLISHER_ID", "camera-1")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "iot/video/stream")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))
MQTT_HOST_OVERRIDE = os.getenv("MQTT_HOST_OVERRIDE", "")
PUBLISHER_HOST_OVERRIDE = os.getenv("PUBLISHER_HOST", "")
# generate random value
# CAM_ID = random.randint(1, 9999)
CAM_ID = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
TOPIC = f"{TOPIC_PREFIX}/{PUBLISHER_ID}/{CAM_ID}"
LWT_TOPIC = f"{TOPIC}/status"

# runtime status variables
# local_queue: deque[str] = deque()
is_connected = threading.Event()
is_lock = threading.Lock()
current_broker: dict = {}


CAMERA_MODE = "rpi"


# create lwt msg
def build_lwt_payload(status: str):
    return json.dumps({
        "publisher_id": PUBLISHER_ID,
        "status": status,
        "timestamp": time.time()
    })

# callback func when MQTT connection is successful
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        log.info(f"Connected to broker: {current_broker.get('host')} (topic: {TOPIC})")
        is_connected.set()
        
        client.publish(LWT_TOPIC, build_lwt_payload("online"), qos=1, retain=True)
        log.info(f"LWT update → status=online")

        # flush_local_queue(client)
    else:
        log.error(f"Connection failed: rc={rc}")
        is_connected.clear()


# callback func when MQTT connection is lost
def on_disconnect(client, userdata, flags, rc, properties):
    if rc != 0:
        log.warning(f"Unexpected disconnection (rc={rc}) → failover 시작")
        is_connected.clear()
        threading.Thread(target=failover, args=(client,), daemon=True).start()

# callback func for pub acknowledgement
def on_publish(client, userdata, mid, rc, properties):
    log.debug(f"PUBACK received (mid={mid})")


# get a broker info from server
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


# when broker host is unusual, resolve broker host
def resolve_host(broker_host: str) -> str:
    # change a broker
    if MQTT_HOST_OVERRIDE:
        return MQTT_HOST_OVERRIDE

    if broker_host.startswith("mosquitto"):
        parsed = urlparse(SERVER_URL)
        fallback_host = parsed.hostname
        log.warning(f"Broker host '{broker_host}' not resolvable → using {fallback_host}")
        return fallback_host

    return broker_host

# get a publisher's ip
def resolve_publisher_host() -> str:
    if PUBLISHER_HOST_OVERRIDE.strip():
        return PUBLISHER_HOST_OVERRIDE.strip()
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "127.0.0.1"

# register publisher info to server
def register_to_server(broker_id: int) -> None:
    publisher_host = resolve_publisher_host()

    for attempt in range(1, 6):
        try:
            res = requests.post(
                f"{SERVER_URL}/api/publisher",
                json={
                    "broker_id": broker_id,
                    "publisher_host": publisher_host,
                    "topic": TOPIC,
                },
                timeout=5,
            )
            res.raise_for_status()
            log.info(f"Registered to server (broker_id={broker_id}, topic={TOPIC})")
            return
        except requests.exceptions.HTTPError as e:
            detail = ""
            response = e.response
            if response is not None:
                try:
                    payload = response.json()
                    detail = payload.get("detail", payload)
                except ValueError:
                    detail = response.text.strip()
            if detail:
                log.warning(f"Failed to register to server ({attempt}/5): {e} | detail={detail}")
            else:
                log.warning(f"Failed to register to server ({attempt}/5): {e}")
            time.sleep(3)
        except Exception as e:
            log.warning(f"Failed to register to server ({attempt}/5): {e}")
            time.sleep(3)
    raise RuntimeError("Unable to register to server.")


# handle failover when connection is lost
def failover(client: mqtt.Client):
    global current_broker

    for attempt in range(1, 11):
        try:
            CAM_ID = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            TOPIC = f"{TOPIC_PREFIX}/{PUBLISHER_ID}/{CAM_ID}"
            LWT_TOPIC = f"{TOPIC}/status"

            broker = receive_broker_info()
            register_to_server(broker["id"])
            host = resolve_host(broker["host"])

            client.will_set(
                topic=LWT_TOPIC,
                payload=build_lwt_payload("offline"),
                qos=1,
                retain=True
            )

            client.disconnect()
            client.connect(host, broker["port"], keepalive=60)

            if is_connected.wait(timeout=5.0):
                current_broker = broker
                log.info(f"Failover success → {host}:{broker['port']}")
                return

        except Exception as e:
            log.warning(f"Failover attempt ({attempt}/10) failed: {e}")
            time.sleep(3)

    # log.error("Failover exhausted. Messages preserved in local_queue.")

# queue retransmission
# def flush_local_queue(client: mqtt.Client):
#     with is_lock:
#         count = len(local_queue)
#     if count == 0:
#         return

#     log.info(f"Flushing local_queue: {count} messages")
#     while True:
#         with is_lock:
#             if not local_queue:
#                 break
#             payload = local_queue.popleft()
#         result = client.publish(TOPIC, payload, qos=1)
#         if result.rc != mqtt.MQTT_ERR_SUCCESS:
#             with is_lock:
#                 local_queue.appendleft(payload)
#             log.error("Flush failed, stopping flush.")
#             break

# make a data
def build_payload_stream(publish_callback):
    """
    rpicam-vid MJPEG 스트림을 파이프로 읽어서
    JPEG 프레임(SOI~EOI)이 완성될 때마다 publish_callback을 호출
    """
    cmd = [
        "rpicam-vid", "-t", "0", "--inline", "-n",
        "--width", "640", "--height", "480",
        "--codec", "mjpeg", "-o", "-"
    ]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.info("rpicam-vid stream start")

    frame_buf = bytearray()
    CHUNK_SIZE = 4096

    try:
        while True:
            chunk = process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break
            frame_buf.extend(chunk)

            while True:
                # find EOI (0xFF 0xD9)
                eoi = frame_buf.find(b'\xff\xd9')
                if eoi == -1:
                    break
                # find SOI (0xFF 0xD8)
                soi = frame_buf.find(b'\xff\xd8')
                if soi == -1 or soi > eoi:
                    # if there are only EOI, then dump
                    frame_buf = frame_buf[eoi + 2:]
                    continue
                # SOI through EOI is one complete frame
                frame_bytes = bytes(frame_buf[soi:eoi + 2])
                frame_buf = frame_buf[eoi + 2:]

                publish_callback(frame_bytes)

    except Exception as e:
        log.error(f"Stream read error: {e}")
    finally:
        process.terminate()
        process.wait()
        log.info("rpicam-vid stream stopped")


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

    client.will_set(
        topic=LWT_TOPIC,
        payload=build_lwt_payload("offline"),
        qos=1,
        retain=True
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    mqtt_host = resolve_host(broker["host"])
    client.connect(mqtt_host, broker["port"], keepalive=60)
    client.loop_start()

    log.info("MJPEG 스트리밍 모드 시작")

    def on_frame(frame_bytes):
        img_b64 = base64.b64encode(frame_bytes).decode("utf-8")
        payload = json.dumps({
            "publisher_id": PUBLISHER_ID,
            "topic": TOPIC,
            "timestamp": time.time(),
            "data": {
                "image": img_b64,
                "width": 640,
                "height": 480,
                "format": "jpeg",
            },
        })

        if not is_connected.is_set():
            # with is_lock:
                # local_queue.append(payload)
            # log.warning(f"Broker unavailable → queued (queue_size={len(local_queue)})")
            log.warning(f"Broker unavailable")
        else:
            result = client.publish(TOPIC, payload, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log.info(f"Published frame (topic={TOPIC})")
            else:
                # with is_lock:
                    # local_queue.append(payload)
                # log.error(f"Publish failed (rc={result.rc}) → queued")
                log.error(f"Publish failed (rc={result.rc})")

    try:
        build_payload_stream(on_frame)
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        client.publish(LWT_TOPIC, build_lwt_payload("offline"), qos=1, retain=True)
        client.loop_stop()
        client.disconnect()
        log.info("Disconnected.")


if __name__ == "__main__":
    main()
