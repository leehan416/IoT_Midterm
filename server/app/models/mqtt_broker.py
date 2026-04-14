from app.models.base_entity import BaseEntity
from pydantic import Field


class MQTTBroker(BaseEntity):
    connected_publisher: int = Field(default=0, ge=0)
    # 실제 host
    host: str = Field(default="localhost", min_length=1)
    port: int = Field(default=1883, ge=1, le=65535)

    # 헬스체킹용 host
    check_host: str = Field(default="localhost", min_length=1)
    check_port: int = Field(default=1883, ge=1, le=65535)
    is_active: bool = Field(default=True)
