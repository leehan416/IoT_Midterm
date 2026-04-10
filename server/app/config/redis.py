from redis import Redis
from app.config.settings import get_settings

def get_redis_client(db: int) -> Redis | None:
    if db < 0:
        return None
    settings = get_settings()
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=db,
        decode_responses=True,
    )
