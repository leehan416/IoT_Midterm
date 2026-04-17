from app.config.redis import get_redis_client
from app.models.publisher import Publisher
from datetime import datetime, UTC


REDIS_PUBLISHER_DB = 2
PUBLISHER_SEQ_KEY = "publisher:seq"


def _publisher_key(publisher_id: int) -> str:
    return f"publisher:{publisher_id}"


def _build_publisher(data: dict[str, str]) -> Publisher:
    created_at_raw = data.get("created_at")
    updated_at_raw = data.get("updated_at")
    created_at = (
        datetime.fromisoformat(created_at_raw).astimezone(UTC)
        if created_at_raw
        else datetime.now(UTC)
    )
    updated_at = (
        datetime.fromisoformat(updated_at_raw).astimezone(UTC)
        if updated_at_raw
        else datetime.now(UTC)
    )
    return Publisher(
        id=int(data.get("id", 0)),
        host=data.get("host", "localhost"),
        broker_id=int(data.get("broker_id", -1)),
        topic=data.get("topic", ""),
        created_at=created_at,
        updated_at=updated_at,
    )


async def save_publisher_data(publisher: Publisher) -> Publisher:
    redis_client = get_redis_client(REDIS_PUBLISHER_DB)
    if publisher.id > 9_007_199_254_740_991 or publisher.id <= 0:
        publisher.id = int(await redis_client.incr(PUBLISHER_SEQ_KEY))
    else:
        current_seq = await redis_client.get(PUBLISHER_SEQ_KEY)
        current_seq_int = int(current_seq) if current_seq is not None else 0
        if publisher.id > current_seq_int:
            await redis_client.set(PUBLISHER_SEQ_KEY, publisher.id)
    publisher.touch()
    await redis_client.hset(
        _publisher_key(publisher.id),
        mapping={
            "id": publisher.id,
            "host": publisher.host,
            "broker_id": publisher.broker_id,
            "topic": publisher.topic,
            "created_at": publisher.created_at.isoformat(),
            "updated_at": publisher.updated_at.isoformat(),
        },
    )
    return publisher


async def get_all_publisher_data() -> list[Publisher]:
    redis_client = get_redis_client(REDIS_PUBLISHER_DB)
    publishers: list[Publisher] = []
    async for key in redis_client.scan_iter(match="publisher:*"):
        if key == PUBLISHER_SEQ_KEY:
            continue
        key_type = await redis_client.type(key)
        if key_type != "hash":
            continue
        data = await redis_client.hgetall(key)
        if data:
            publishers.append(_build_publisher(data))
    return publishers


async def clear_publisher_datas() -> None:
    redis_client = get_redis_client(REDIS_PUBLISHER_DB)
    await redis_client.flushdb()


async def delete_publisher_by_topic(topic: str) -> int:
    redis_client = get_redis_client(REDIS_PUBLISHER_DB)
    normalized_topic = topic.strip()
    if not normalized_topic:
        return 0

    deleted = 0
    async for key in redis_client.scan_iter(match="publisher:*"):
        if key == PUBLISHER_SEQ_KEY:
            continue
        key_type = await redis_client.type(key)
        if key_type != "hash":
            continue
        stored_topic = await redis_client.hget(key, "topic")
        if stored_topic != normalized_topic:
            continue
        deleted += await redis_client.delete(key)
    return deleted


async def touch_publisher_by_topic(topic: str) -> int:
    redis_client = get_redis_client(REDIS_PUBLISHER_DB)
    normalized_topic = topic.strip()
    if not normalized_topic:
        return 0

    matched = 0
    now = datetime.now(UTC).isoformat()
    async for key in redis_client.scan_iter(match="publisher:*"):
        if key == PUBLISHER_SEQ_KEY:
            continue
        key_type = await redis_client.type(key)
        if key_type != "hash":
            continue
        stored_topic = await redis_client.hget(key, "topic")
        if stored_topic != normalized_topic:
            continue
        matched += 1
        await redis_client.hset(key, mapping={"updated_at": now})
    return matched


async def delete_publisher_by_publisher_id(publisher_id: str) -> int:
    redis_client = get_redis_client(REDIS_PUBLISHER_DB)
    normalized_publisher_id = publisher_id.strip()
    if not normalized_publisher_id:
        return 0

    deleted = 0
    suffix = f"/{normalized_publisher_id}"
    async for key in redis_client.scan_iter(match="publisher:*"):
        if key == PUBLISHER_SEQ_KEY:
            continue
        key_type = await redis_client.type(key)
        if key_type != "hash":
            continue
        stored_topic = await redis_client.hget(key, "topic")
        if not isinstance(stored_topic, str):
            continue
        if not stored_topic.strip().endswith(suffix):
            continue
        deleted += await redis_client.delete(key)
    return deleted
