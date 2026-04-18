import asyncio
from contextlib import suppress

from app.schemas.comon_schemas import HealthCheckResponse
import app.services.publisher_service as publisher_service
import app.services.mqtt_subscriber_service as mqtt_subscriber_service
import app.services.video_stream_service as video_stream_service
import app.scheduler.mqtt_checker as mqtt_scheduler
import app.scheduler.publisher_checker as publisher_scheduler


async def check_health() -> HealthCheckResponse:
    """서비스 헬스체크 상태를 반환하는 함수.
    현재 서버가 정상 동작 중이면 `ok` 상태를 응답한다.
    """
    return HealthCheckResponse(status="ok")


async def run_startup() -> tuple[asyncio.Task, asyncio.Task]:
    """애플리케이션 시작 시 필요한 초기화 작업을 수행하는 함수.
    이벤트 루프 설정, 구독 복구, 스케줄러 시작, MQTT subscriber 시작을 처리한다.
    """
    video_stream_service.set_event_loop(asyncio.get_running_loop())
    await publisher_service.restore_publisher_subscriptions()
    checker_task = asyncio.create_task(mqtt_scheduler.mqtt_status_checker())
    publisher_sync_task = asyncio.create_task(publisher_scheduler.publisher_status_checker())
    await mqtt_subscriber_service.start()
    return checker_task, publisher_sync_task


async def run_shutdown(checker_task: asyncio.Task, publisher_sync_task: asyncio.Task) -> None:
    """애플리케이션 종료 시 백그라운드 태스크와 MQTT subscriber를 정리하는 함수.
    실행 중인 스케줄러 태스크를 취소하고 정상 종료를 기다린다.
    """
    mqtt_subscriber_service.stop()
    checker_task.cancel()
    publisher_sync_task.cancel()
    with suppress(asyncio.CancelledError):
        await checker_task
    with suppress(asyncio.CancelledError):
        await publisher_sync_task
