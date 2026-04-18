from __future__ import annotations

import asyncio
import base64
import json
import logging
import socket
import uuid

import paho.mqtt.client as mqtt

import app.repository.publisher_repository as publisher_repository
import app.services.video_stream_service as video_stream_service
import app.services.mqtt_service as mqtt_service
import app.repository.mqtt_repository as mqtt_repository

logger = logging.getLogger(__name__)

_clients: list[mqtt.Client] = []
_connected_brokers: set[tuple[str, int]] = set()
_extra_topics: set[str] = set()
_topic_to_publisher_id: dict[str, str] = {}
_loop: asyncio.AbstractEventLoop | None = None


def _status_topic_of(topic: str) -> str:
    normalized = topic.strip()
    if not normalized:
        return ""
    if normalized.endswith("/status"):
        return normalized
    return f"{normalized}/status"


async def start() -> None:
    """MQTT 구독 서비스를 시작하는 함수.
    현재 이벤트 루프를 저장하고 브로커 연결 갱신을 수행한다.
    """
    global _loop
    if _loop is None:
        try:
            _loop = asyncio.get_running_loop()
        except RuntimeError:
            _loop = None

    await refresh_brokers()


def stop() -> None:
    """MQTT 구독 서비스를 중지하는 함수.
    활성 클라이언트를 모두 종료하고 내부 연결 상태를 초기화한다.
    """
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
    """활성 브로커 후보를 기준으로 MQTT 클라이언트를 갱신하는 함수.
    아직 연결되지 않은 브로커에 대해 신규 구독 클라이언트를 생성한다.
    """
    await mqtt_service.sync_mqtt_broker_statuses()
    repository_candidates: list[tuple[str, int]] = []
    try:
        brokers = await mqtt_repository.get_all_mqtt_datas()
        for broker in brokers:
            if not broker.is_active:
                continue
            repository_candidates.append((broker.host, broker.port))
    except Exception:
        logger.exception("Failed to load MQTT broker candidates from repository")

    candidates = list(dict.fromkeys(repository_candidates))
    if not candidates:
        logger.info("MQTT subscriber skipped (no broker candidates configured)")
        return
    started = 0
    for host, port in candidates:
        if _ensure_client(host, port):
            started += 1
    if not _clients and started == 0:
        logger.error("MQTT subscriber could not connect to any broker candidate")


def _ensure_client(host: str, port: int) -> bool:
    """지정한 브로커에 MQTT 구독 클라이언트 연결을 보장하는 함수.
    연결 생성에 성공하면 `True`, 실패 또는 이미 연결된 경우 `False`를 반환한다.
    """
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


def register_publisher_topic(publisher_id: str, topic: str, qos: int = 1) -> None:
    """퍼블리셔 토픽을 동적 구독 목록에 등록하는 함수.
    현재 연결된 모든 클라이언트에 해당 토픽 구독을 즉시 적용한다.
    """
    normalized = topic.strip()
    if not normalized:
        return

    topics = {normalized}
    status_topic = _status_topic_of(normalized)
    if status_topic:
        topics.add(status_topic)

    for subscribe_topic in topics:
        _topic_to_publisher_id[subscribe_topic] = publisher_id
        _extra_topics.add(subscribe_topic)
        for client in _clients:
            result, _ = client.subscribe(subscribe_topic, qos=qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                logger.error("Failed to subscribe topic=%s result=%s", subscribe_topic, result)
            else:
                logger.info(
                    "MQTT subscriber subscribed dynamic topic=%s publisher_id=%s",
                    subscribe_topic,
                    publisher_id,
                )


def unregister_publisher_topic(topic: str) -> None:
    """퍼블리셔 토픽을 동적 구독 목록에서 제거하는 함수.
    내부 매핑을 정리하고 연결된 클라이언트의 구독을 해제한다.
    """
    normalized = topic.strip()
    if not normalized:
        return
    topics = {normalized}
    status_topic = _status_topic_of(normalized)
    if status_topic:
        topics.add(status_topic)
    if normalized.endswith("/status"):
        base_topic = normalized[: -len("/status")]
        if base_topic:
            topics.add(base_topic)

    for unsubscribe_topic in topics:
        _extra_topics.discard(unsubscribe_topic)
        _topic_to_publisher_id.pop(unsubscribe_topic, None)
        for client in _clients:
            result, _ = client.unsubscribe(unsubscribe_topic)
            if result != mqtt.MQTT_ERR_SUCCESS:
                logger.error("Failed to unsubscribe topic=%s result=%s", unsubscribe_topic, result)


def _on_connect(client: mqtt.Client, _userdata, _flags, rc, _properties) -> None:
    """MQTT 연결 성공 시 기본/동적 토픽 구독을 수행하는 콜백 함수.
    연결 실패 코드가 전달되면 구독을 수행하지 않고 오류 로그를 남긴다.
    """
    if rc != 0:
        logger.error("MQTT subscriber connect failed rc=%s", rc)
        return
    for topic in _extra_topics:
        client.subscribe(topic, qos=1)
    logger.info("MQTT subscriber connected and restored dynamic topics count=%s", len(_extra_topics))


def _on_disconnect(_client: mqtt.Client, _userdata, _flags, rc, _properties) -> None:
    """MQTT 연결 종료 이벤트를 처리하는 콜백 함수.
    정상 종료와 비정상 종료를 구분해 로그를 기록한다.
    """
    if rc == 0:
        logger.info("MQTT subscriber disconnected gracefully")
        return
    logger.warning("MQTT subscriber disconnected unexpectedly rc=%s", rc)


def _on_message(_client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
    """MQTT 수신 메시지를 처리해 최신 프레임을 반영하는 콜백 함수.
    Last-Will 여부를 판별하고 일반 메시지는 이미지 디코딩 후 허브로 전달한다.
    """
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

    normalized_topic = msg.topic.strip()
    if normalized_topic.endswith("/status"):
        status_value = payload.get("status") if isinstance(payload, dict) else None
        logger.info(
            "Publisher status update topic=%s status=%s",
            normalized_topic,
            status_value if isinstance(status_value, str) else "unknown",
        )
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

    video_stream_service.publish_frame(publisher_id, frame_bytes)


def _handle_last_will(topic: str, raw_payload: str) -> None:
    """Last-Will 메시지를 처리해 퍼블리셔 정리를 수행하는 함수.
    토픽 구독을 해제하고 저장소에서 관련 퍼블리셔 레코드를 삭제한다.
    """
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
    """퍼블리셔 토픽의 최근 활동 시간을 갱신하는 함수.
    이벤트 루프가 가능할 때 저장소의 heartbeat 타임스탬프를 업데이트한다.
    """
    normalized_topic = topic.strip()
    if not normalized_topic or _loop is None:
        return

    topics_to_touch = {normalized_topic}
    if normalized_topic.endswith("/status"):
        base_topic = normalized_topic[: -len("/status")]
        if base_topic:
            topics_to_touch.add(base_topic)

    async def _touch_all() -> int:
        touched = 0
        for candidate in topics_to_touch:
            touched += await publisher_repository.touch_publisher_by_topic(candidate)
        return touched

    future = asyncio.run_coroutine_threadsafe(_touch_all(), _loop)
    try:
        touched = future.result(timeout=2)
        if touched > 0:
            logger.debug("Publisher heartbeat updated topic=%s touched=%s", normalized_topic, touched)
    except Exception:
        logger.exception("Failed to update publisher heartbeat topic=%s", normalized_topic)


def _is_last_will_payload(raw_payload: str) -> bool:
    """수신 페이로드가 Last-Will 메시지인지 판별하는 함수.
    문자열/JSON 내 이벤트 필드를 검사해 오프라인 신호 여부를 반환한다.
    """
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
    """메시지 토픽과 페이로드에서 퍼블리셔 ID를 추출하는 함수.
    로컬 매핑, 저장소 조회, 페이로드, 토픽 순서로 후보를 확인한다.
    """
    normalized_topic = topic.strip()
    mapped_publisher_id = _topic_to_publisher_id.get(normalized_topic)
    if mapped_publisher_id:
        return mapped_publisher_id
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
