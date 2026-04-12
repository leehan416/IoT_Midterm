from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    redis_host: str = "redis"
    redis_port: int = 6379
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_brokers: str = ""
    mqtt_broker_count: int = 1
    mqtt_broker_name_prefix: str = "mosquitto"
    mqtt_topic_prefix: str = "iot"
    mqtt_subscribe_topic: str = "iot/#"
    mqtt_upload_dir: str = "app/static/uploads"
    ws_heartbeat_seconds: int = 20

settings = Settings()


def ensure_runtime_dirs() -> None:
    Path(settings.mqtt_upload_dir).mkdir(parents=True, exist_ok=True)
