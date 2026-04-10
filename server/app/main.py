from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes.api_routes import router as api_router
from app.routes.comon_routes import router as comon_router
from app.routes.mqtt_routes import router as mqtt_router


app = FastAPI(title="iot-server")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(comon_router)
app.include_router(api_router)
app.include_router(mqtt_router)
