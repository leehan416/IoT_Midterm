from app.models.base_entity import BaseEntity
from pydantic import Field


class Publisher(BaseEntity):
    host: str = Field(default="localhost", min_length=1)
    broker_id: int = Field(default=-1)
    topic: str = Field(default="", min_length=1)
