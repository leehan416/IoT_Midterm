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
    mqtt_host: str = ""
    mqtt_port: int = 1883
    mqtt_brokers: str = ""
    mqtt_broker_count: int = 0
    mqtt_broker_name_prefix: str = ""
    mqtt_topic_prefix: str = "iot"
    mqtt_default_host: str = ""
    mqtt_advertised_host: str = "localhost"
    mqtt_advertised_port_start: int = 1883
    mqtt_subscribe_topic: str = "iot/#"
    mqtt_publisher_ttl_seconds: int = 5
    ws_heartbeat_seconds: int = 5

settings = Settings()

