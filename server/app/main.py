from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes.api_routes import router as api_router
from app.routes.comon_routes import router as comon_router
import app.services.comon_service as comon_service


@asynccontextmanager
async def lifespan(_app: FastAPI):
    checker_task, publisher_sync_task = await comon_service.run_startup()
    try:
        yield
    finally:
        await comon_service.run_shutdown(checker_task, publisher_sync_task)


app = FastAPI(title="iot-server", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(comon_router)
app.include_router(api_router)
