from pydantic import BaseModel, ConfigDict


class MQTTStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    connected_publisher: int
    id: int
    host: str
    port: int
    is_active: bool


class MQTTDataResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    host: str
    port: int

class MQTTConnectedDataRequest(BaseModel):
    id: int
    topic: str


class MQTTActiveRequest(BaseModel):
    id: int
    is_active: bool
