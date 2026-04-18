from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import asyncio

from app.routes.api_routes import router as api_router
from app.routes.comon_routes import router as comon_router
import app.services.mqtt_service as mqtt_service
import app.services.mqtt_subscriber_service as mqtt_subscriber_service
import app.services.video_stream_hub as video_stream_hub
from app.config.settings import ensure_runtime_dirs
from app.scheduler.mqtt_checker import mqtt_status_checker


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_runtime_dirs()
    video_stream_hub.set_event_loop(asyncio.get_running_loop())

    await mqtt_service.restore_publisher_subscriptions()
    checker_task = asyncio.create_task(mqtt_status_checker())
    publisher_sync_task = asyncio.create_task(mqtt_service.publisher_subscription_sync_worker())
    await mqtt_subscriber_service.start()
    try:
        yield
    finally:
        mqtt_subscriber_service.stop()
        checker_task.cancel()
        publisher_sync_task.cancel()
        with suppress(asyncio.CancelledError):
            await checker_task
        with suppress(asyncio.CancelledError):
            await publisher_sync_task


app = FastAPI(title="iot-server", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(comon_router)
app.include_router(api_router)
