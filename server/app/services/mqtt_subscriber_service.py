from __future__ import annotations

import asyncio
import base64
import json
import logging
import socket
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt

from app.config.settings import settings
import app.repository.mqtt_repository as mqtt_repository
from app.repository import publisher_repository
from app.services import video_stream_hub

logger = logging.getLogger(__name__)

_clients: list[mqtt.Client] = []
_connected_brokers: set[tuple[str, int]] = set()
_extra_topics: set[str] = set()
_topic_to_publisher_id: dict[str, str] = {}
_loop: asyncio.AbstractEventLoop | None = None


async def start() -> None:
    global _loop
    if _loop is None:
        try:
            _loop = asyncio.get_running_loop()
        except RuntimeError:
            _loop = None

    await refresh_brokers()


def stop() -> None:
    global _clients, _connected_brokers
    if not _clients:
        return
    for client in _clients:
        client.loop_stop()
        client.disconnect()
    client_count = len(_clients)
    _clients = []
    _connected_brokers = set()
    logger.info("MQTT subscriber stopped (clients=%s)", client_count)


async def refresh_brokers() -> None:
    candidates = await _broker_candidates()
    started = 0
    for host, port in candidates:
        if _ensure_client(host, port):
            started += 1
    if not _clients and started == 0:
        logger.error("MQTT subscriber could not connect to any broker candidate")


def _ensure_client(host: str, port: int) -> bool:
    endpoint = (host, port)
    if endpoint in _connected_brokers:
        return False

    client_id = f"iot-server-video-subscriber-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    try:
        client.connect(host, port, keepalive=60)
        client.loop_start()
        _clients.append(client)
        _connected_brokers.add(endpoint)
        logger.info(
            "MQTT subscriber started host=%s port=%s client_id=%s",
            host,
            port,
            client_id,
        )
        return True
    except Exception:
        logger.exception("MQTT subscriber connect failed host=%s port=%s", host, port)
        return False


def register_publisher_topic(publisher_id: int | str, topic: str, qos: int = 1) -> None:
    normalized = topic.strip()
    if not normalized:
        return

    _topic_to_publisher_id[normalized] = str(publisher_id)
    _extra_topics.add(normalized)
    for client in _clients:
        result, _ = client.subscribe(normalized, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error("Failed to subscribe topic=%s result=%s", normalized, result)
        else:
            logger.info(
                "MQTT subscriber subscribed dynamic topic=%s publisher_id=%s",
                normalized,
                publisher_id,
            )


def subscribe_topic(topic: str, qos: int = 1) -> None:
    register_publisher_topic("unknown", topic, qos=qos)


def unregister_publisher_topic(topic: str) -> None:
    normalized = topic.strip()
    if not normalized:
        return
    _extra_topics.discard(normalized)
    _topic_to_publisher_id.pop(normalized, None)
    for client in _clients:
        result, _ = client.unsubscribe(normalized)
        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error("Failed to unsubscribe topic=%s result=%s", normalized, result)


def _on_connect(client: mqtt.Client, _userdata, _flags, rc, _properties) -> None:
    if rc != 0:
        logger.error("MQTT subscriber connect failed rc=%s", rc)
        return
    client.subscribe(settings.mqtt_subscribe_topic, qos=1)
    for topic in _extra_topics:
        client.subscribe(topic, qos=1)
    logger.info("MQTT subscriber connected and subscribed topic=%s", settings.mqtt_subscribe_topic)


def _on_disconnect(_client: mqtt.Client, _userdata, _flags, rc, _properties) -> None:
    if rc == 0:
        logger.info("MQTT subscriber disconnected gracefully")
        return
    logger.warning("MQTT subscriber disconnected unexpectedly rc=%s", rc)


def _on_message(_client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
    raw_payload = msg.payload.decode("utf-8", errors="replace")
    if _is_last_will_payload(raw_payload):
        logger.warning(
            "LWT message received topic=%s payload=%s",
            msg.topic,
            raw_payload[:200],
        )
        _handle_last_will(msg.topic, raw_payload)
        return
    _touch_publisher_activity(msg.topic)

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        logger.exception("Failed to parse MQTT payload as JSON topic=%s", msg.topic)
        return

    publisher_id = _extract_publisher_id(msg.topic, payload)
    image_b64 = payload.get("data", {}).get("image")
    if not isinstance(image_b64, str):
        logger.error("Payload missing data.image base64 topic=%s", msg.topic)
        return

    try:
        frame_bytes = base64.b64decode(image_b64, validate=True)
    except Exception:
        logger.exception("Failed to decode base64 image topic=%s publisher_id=%s", msg.topic, publisher_id)
        return

    _save_latest_file(publisher_id, frame_bytes)
    video_stream_hub.publish_frame(publisher_id, frame_bytes)


def _handle_last_will(topic: str, raw_payload: str) -> None:
    normalized_topic = topic.strip()
    if not normalized_topic:
        return

    payload: dict = {}
    try:
        parsed = json.loads(raw_payload)
        if isinstance(parsed, dict):
            payload = parsed
    except json.JSONDecodeError:
        payload = {}

    publisher_id = _extract_publisher_id(normalized_topic, payload)
    topic_candidates = {normalized_topic}
    if normalized_topic.endswith("/status"):
        topic_candidates.add(normalized_topic[: -len("/status")])
    for candidate in topic_candidates:
        unregister_publisher_topic(candidate)
    logger.warning(
        "Last will received. Removing publisher topic=%s publisher_id=%s",
        normalized_topic,
        publisher_id,
    )
    if _loop is None:
        logger.error("Event loop unavailable. skip publisher removal topic=%s", normalized_topic)
        return

    async def _cleanup() -> int:
        deleted_total = 0
        for candidate in topic_candidates:
            deleted_total += await publisher_repository.delete_publisher_by_topic(candidate)
        if isinstance(publisher_id, str) and publisher_id and publisher_id != "unknown":
            deleted_total += await publisher_repository.delete_publisher_by_publisher_id(publisher_id)
        return deleted_total

    future = asyncio.run_coroutine_threadsafe(_cleanup(), _loop)
    try:
        deleted = future.result(timeout=3)
        logger.info(
            "LWT cleanup completed topic=%s candidates=%s publisher_id=%s removed_records=%s",
            normalized_topic,
            sorted(topic_candidates),
            publisher_id,
            deleted,
        )
    except Exception:
        logger.exception("Failed to remove publisher topic=%s", normalized_topic)


def _touch_publisher_activity(topic: str) -> None:
    normalized_topic = topic.strip()
    if not normalized_topic or _loop is None:
        return
    future = asyncio.run_coroutine_threadsafe(
        publisher_repository.touch_publisher_by_topic(normalized_topic),
        _loop,
    )
    try:
        touched = future.result(timeout=2)
        if touched > 0:
            logger.debug("Publisher heartbeat updated topic=%s touched=%s", normalized_topic, touched)
    except Exception:
        logger.exception("Failed to update publisher heartbeat topic=%s", normalized_topic)



def _is_last_will_payload(raw_payload: str) -> bool:
    stripped = raw_payload.strip().lower()
    if stripped in {"offline", "lastwill", "last_will", "disconnect", "disconnected"}:
        return True

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False

    candidates = (
        payload.get("event"),
        payload.get("type"),
        payload.get("status"),
        payload.get("message"),
    )
    for value in candidates:
        if isinstance(value, str) and value.strip().lower() in {
            "offline",
            "lastwill",
            "last_will",
            "disconnect",
            "disconnected",
        }:
            return True
    return False



def _extract_publisher_id(topic: str, payload: dict) -> str:
    normalized_topic = topic.strip()
    mapped_publisher_id = _topic_to_publisher_id.get(normalized_topic)
    if mapped_publisher_id:
        return mapped_publisher_id

    # In multi-server deployments, the registration request may hit another API instance.
    # Resolve by topic from shared Redis and cache the mapping locally.
    if normalized_topic and _loop is not None:
        try:
            future = asyncio.run_coroutine_threadsafe(
                publisher_repository.get_publisher_by_topic(normalized_topic),
                _loop,
            )
            publisher = future.result(timeout=2)
            if publisher is not None:
                resolved_id = str(publisher.id)
                _topic_to_publisher_id[normalized_topic] = resolved_id
                _extra_topics.add(normalized_topic)
                return resolved_id
        except Exception:
            logger.exception("Failed to resolve publisher id by topic topic=%s", normalized_topic)

    publisher_id = payload.get("publisher_id")
    if isinstance(publisher_id, str) and publisher_id:
        return publisher_id
    topic_parts = [part for part in topic.split("/") if part]
    if topic_parts:
        topic_camera_id = topic_parts[-1]
        if topic_camera_id not in ("#", "+"):
            return topic_camera_id
    return "unknown"



def _save_latest_file(camera_id: str, frame_bytes: bytes) -> None:
    target_dir = Path(settings.mqtt_upload_dir) / camera_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "latest.jpg"
    target_file.write_bytes(frame_bytes)


async def _broker_candidates() -> list[tuple[str, int]]:
    repository_candidates: list[tuple[str, int]] = []
    try:
        brokers = await mqtt_repository.get_all_mqtt_datas()
        for broker in brokers:
            if not broker.is_active:
                continue
            repository_candidates.append((broker.check_host, broker.check_port))
    except Exception:
        logger.exception("Failed to load MQTT broker candidates from repository")

    if repository_candidates:
        # Keep deterministic ordering and unique endpoints.
        unique = list(dict.fromkeys(repository_candidates))
        return unique

    if settings.mqtt_brokers.strip():
        candidates: list[tuple[str, int]] = []
        for item in settings.mqtt_brokers.split(","):
            host, port = item.strip().split(":")
            candidates.append((host.strip(), int(port.strip())))
        return candidates

    if settings.mqtt_broker_count > 1:
        return [
            (f"{settings.mqtt_broker_name_prefix}-{idx}", settings.mqtt_port)
            for idx in range(1, settings.mqtt_broker_count + 1)
        ]

    return [(settings.mqtt_host, settings.mqtt_port)]
