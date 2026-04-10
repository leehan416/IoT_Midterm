from datetime import UTC, datetime
from time import time_ns

from pydantic import BaseModel, Field


class BaseEntity(BaseModel):
    id: int = Field(default_factory=lambda: time_ns())
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # since without ORM System, have to execute this function when data modified
    def touch(self):
        self.updated_at = datetime.now(UTC)
        return self