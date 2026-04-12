from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import asyncio

from app.routes.api_routes import router as api_router
from app.routes.comon_routes import router as comon_router
import app.services.mqtt_service as mqtt_service
from app.scheduler.mqtt_checker import mqtt_status_checker

app = FastAPI(title="iot-server")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(comon_router)
app.include_router(api_router)


@app.on_event("startup")
async def startup_event() -> None:
    await mqtt_service.register_mqtt_brokers()
    asyncio.create_task(mqtt_status_checker())
