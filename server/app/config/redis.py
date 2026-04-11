from redis.asyncio import Redis
from app.config.settings import settings

def get_redis_client(db: int) -> Redis | None:
    if db < 0:
        return None
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=db,
        decode_responses=True,
    )

def get_redis_binary_client() -> Redis:
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=0,
        decode_responses=False,
    )
