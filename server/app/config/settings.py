from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


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
    mqtt_topic_prefix: str = "iot"


@lru_cache
def get_settings() -> Settings:
    return Settings()
