from fastapi import APIRouter, HTTPException
from redis.exceptions import RedisError

from app.services.comon_service import (
    check_health,
    get_redis_db_value,
    publish_mqtt_message,
)


router = APIRouter(prefix="/api", tags=["api"])


@router.get("/health")
def healthcheck() -> dict[str, str]:
    return check_health()


@router.get("/redis/{db}")
def redis_by_db(db: int) -> dict[str, str | int]:
    try:
        return get_redis_db_value(db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis error: {exc}") from exc


@router.post("/mqtt/publish")
def mqtt_publish(topic: str, message: str) -> dict[str, str]:
    try:
        return publish_mqtt_message(topic=topic, message=message)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
