import logging
from datetime import UTC, datetime

from fastapi import HTTPException

from app.config.settings import settings
from app.models.publisher import Publisher
from app.repository import publisher_repository
import app.repository.mqtt_repository as mqtt_repository
import app.services.mqtt_subscriber_service as mqtt_subscriber_service
from app.schemas.mqtt_schemas import *

logger = logging.getLogger(__name__)


async def set_mqtt_broker_data(request_data: MQTTConnectedDataRequest) -> MQTTStatusResponse:
    """퍼블리셔를 브로커에 등록하고 연결 수를 갱신하는 함수.
    유효한 토픽이면 퍼블리셔를 저장하고 브로커 상태를 업데이트한다.
    """
    broker = await mqtt_repository.get_mqtt_data_by_id(request_data.broker_id)
    if broker is None:
        raise HTTPException(status_code=404, detail="No MQTT broker data found")
    if not broker.is_active:
        raise HTTPException(status_code=404, detail="MQTT broker is inactive")

    normalized_topic = request_data.topic.strip()
    if not normalized_topic:
        raise HTTPException(status_code=400, detail="topic is required")

    publishers: list[Publisher] = await publisher_repository.get_all_publisher_data()
    for pub in publishers:
        if pub.topic == normalized_topic:
            raise HTTPException(status_code=409, detail="topic already using")

    publisher = Publisher(
        broker_id=broker.id,
        host=request_data.publisher_host or "unknown",
        topic=normalized_topic,
    )
    await publisher_repository.save_publisher_data(publisher)
    mqtt_subscriber_service.register_publisher_topic(str(publisher.id), normalized_topic)

    broker.connected_publisher += 1
    saved = await mqtt_repository.save_mqtt_data(broker)
    return MQTTStatusResponse.model_validate(saved)


async def get_all_publishers() -> list[PublisherResponse]:
    """등록된 전체 퍼블리셔 목록을 브로커 정보와 함께 반환하는 함수.
    퍼블리셔별 브로커 주소/포트/활성 상태를 포함해 응답한다.
    """
    publishers = await publisher_repository.get_all_publisher_data()
    brokers = await mqtt_repository.get_all_mqtt_datas()
    broker_map = {broker.id: broker for broker in brokers}
    return [
        PublisherResponse(
            id=pub.id,
            broker_id=pub.broker_id,
            host=pub.host,
            topic=pub.topic,
            broker_host=broker_map[pub.broker_id].host if pub.broker_id in broker_map else "unknown",
            broker_port=broker_map[pub.broker_id].port if pub.broker_id in broker_map else -1,
            broker_is_active=broker_map[pub.broker_id].is_active if pub.broker_id in broker_map else False,
        )
        for pub in publishers
    ]


async def restore_publisher_subscriptions() -> None:
    """저장된 퍼블리셔 토픽을 구독기로 재등록하는 함수.
    서버 재시작 이후 기존 토픽 구독 상태를 복구할 때 사용한다.
    """
    publishers = await publisher_repository.get_all_publisher_data()
    for publisher in publishers:
        mqtt_subscriber_service.register_publisher_topic(str(publisher.id), publisher.topic)


async def sync_connected_publisher_counts() -> None:
    """브로커별 실제 퍼블리셔 수를 재계산해 저장하는 함수.
    저장된 연결 수와 실제 수가 다를 때만 값을 갱신한다.
    """
    brokers = await mqtt_repository.get_all_mqtt_datas()
    publishers = await publisher_repository.get_all_publisher_data()

    broker_counts: dict[int, int] = {}
    for pub in publishers:
        broker_counts[pub.broker_id] = broker_counts.get(pub.broker_id, 0) + 1

    for broker in brokers:
        actual_count = broker_counts.get(broker.id, 0)
        if broker.connected_publisher != actual_count:
            broker.connected_publisher = actual_count
            await mqtt_repository.save_mqtt_data(broker)


async def _cleanup_stale_publishers() -> None:
    """TTL을 초과한 비활성 퍼블리셔를 정리하는 함수.
    만료된 퍼블리셔 레코드를 삭제하고 관련 토픽 구독을 해제한다.
    """
    ttl_seconds = settings.mqtt_publisher_ttl_seconds
    if ttl_seconds <= 0:
        return

    now = datetime.now(UTC)
    publishers = await publisher_repository.get_all_publisher_data()
    for publisher in publishers:
        inactive_for = (now - publisher.updated_at).total_seconds()
        if inactive_for <= ttl_seconds:
            continue
        deleted = await publisher_repository.delete_publisher_by_topic(publisher.topic)
        mqtt_subscriber_service.unregister_publisher_topic(publisher.topic)
        logger.warning(
            "Publisher expired by TTL id=%s topic=%s inactive_for=%.1fs ttl=%ss deleted=%s",
            publisher.id,
            publisher.topic,
            inactive_for,
            ttl_seconds,
            deleted,
        )


async def sync_publisher_subscription_state() -> None:
    """퍼블리셔 구독 상태를 1회 동기화하는 함수.
    브로커 갱신, 구독 복구, 만료 정리, 연결 수 동기화를 순서대로 수행한다.
    """
    await mqtt_subscriber_service.refresh_brokers()
    await restore_publisher_subscriptions()
    await _cleanup_stale_publishers()
    await sync_connected_publisher_counts()
