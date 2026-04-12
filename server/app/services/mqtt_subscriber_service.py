from __future__ import annotations

import base64
import json
import logging
import socket
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt

from app.config.settings import settings
from app.services.video_stream_hub import video_stream_hub

logger = logging.getLogger(__name__)


class MQTTSubscriberService:
    def __init__(self) -> None:
        self._client: mqtt.Client | None = None

    def start(self) -> None:
        if self._client is not None:
            return

        client_id = f"iot-server-video-subscriber-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        connected = False
        for host, port in self._broker_candidates():
            try:
                client.connect(host, port, keepalive=60)
                connected = True
                logger.info(
                    "MQTT subscriber initial connect target host=%s port=%s client_id=%s",
                    host,
                    port,
                    client_id,
                )
                break
            except Exception:
                logger.exception("MQTT subscriber connect failed host=%s port=%s", host, port)

        client.loop_start()
        self._client = client
        if not connected:
            logger.error(
                "MQTT subscriber could not connect to any broker candidate at startup; "
                "service stays alive and will rely on manual restart/config fix"
            )

    def stop(self) -> None:
        if self._client is None:
            return
        self._client.loop_stop()
        self._client.disconnect()
        self._client = None
        logger.info("MQTT subscriber stopped")

    def _on_connect(self, client: mqtt.Client, _userdata, _flags, rc, _properties) -> None:
        if rc != 0:
            logger.error("MQTT subscriber connect failed rc=%s", rc)
            return
        client.subscribe(settings.mqtt_subscribe_topic, qos=1)
        logger.info("MQTT subscriber connected and subscribed topic=%s", settings.mqtt_subscribe_topic)

    def _on_disconnect(self, _client: mqtt.Client, _userdata, _flags, rc, _properties) -> None:
        if rc == 0:
            logger.info("MQTT subscriber disconnected gracefully")
            return
        logger.warning("MQTT subscriber disconnected unexpectedly rc=%s", rc)

    def _on_message(self, _client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
        raw_payload = msg.payload.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            logger.exception("Failed to parse MQTT payload as JSON topic=%s", msg.topic)
            return

        camera_id = self._extract_camera_id(msg.topic, payload)
        image_b64 = payload.get("data", {}).get("image")
        if not isinstance(image_b64, str):
            logger.error("Payload missing data.image base64 topic=%s", msg.topic)
            return

        try:
            frame_bytes = base64.b64decode(image_b64, validate=True)
        except Exception:
            logger.exception("Failed to decode base64 image topic=%s camera_id=%s", msg.topic, camera_id)
            return

        self._save_latest_file(camera_id, frame_bytes)
        video_stream_hub.publish_frame(camera_id, frame_bytes)

    @staticmethod
    def _extract_camera_id(topic: str, payload: dict) -> str:
        topic_parts = [part for part in topic.split("/") if part]
        if topic_parts:
            topic_camera_id = topic_parts[-1]
            if topic_camera_id not in ("#", "+"):
                return topic_camera_id
        publisher_id = payload.get("publisher_id")
        if isinstance(publisher_id, str) and publisher_id:
            return publisher_id
        return "unknown"

    @staticmethod
    def _save_latest_file(camera_id: str, frame_bytes: bytes) -> None:
        target_dir = Path(settings.mqtt_upload_dir) / camera_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "latest.jpg"
        target_file.write_bytes(frame_bytes)

    @staticmethod
    def _broker_candidates() -> list[tuple[str, int]]:
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


mqtt_subscriber_service = MQTTSubscriberService()
