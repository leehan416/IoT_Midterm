import asyncio
import logging

from app.services.publisher_service import sync_publisher_subscription_state

logger = logging.getLogger(__name__)


async def publisher_status_checker(interval: int = 3) -> None:
    """퍼블리셔 구독/정리 상태를 주기적으로 동기화하는 스케줄러 함수.
    서비스의 1회 동기화 함수를 반복 실행해 상태를 최신으로 유지한다.
    """
    logger.info("Publisher checker task started. Interval: %s seconds.", interval)
    while True:
        try:
            await sync_publisher_subscription_state()
        except Exception as e:
            logger.error("Error during publisher subscription sync: %s", e)
        await asyncio.sleep(interval)
