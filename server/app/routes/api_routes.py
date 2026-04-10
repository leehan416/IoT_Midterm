from fastapi import APIRouter, HTTPException
from redis.exceptions import RedisError

from app.services.comon_service import (
    check_health,
    get_redis_db_value,
    publish_mqtt_message,
)


router = APIRouter(prefix="/api", tags=["api"])


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return await check_health()

