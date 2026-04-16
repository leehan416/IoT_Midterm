from pydantic import BaseModel, ConfigDict, Field, model_validator


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

class MQTTAddRequest(BaseModel):
    mqtt_host: str = ""
    mqtt_port: int


class MQTTConnectedDataRequest(BaseModel):
    broker_id: int
    publisher_host: str = ""
    topic: str

    @model_validator(mode="before")
    @classmethod
    def normalize_broker_id(cls, data):
        if isinstance(data, dict) and "broker_id" not in data and "id" in data:
            data = dict(data)
            data["broker_id"] = data["id"]
        return data


class MQTTActiveRequest(BaseModel):
    id: int
    is_active: bool


class PublisherResponse(BaseModel):
    id: int
    broker_id: int
    host: str
    topic: str
    broker_host: str
    broker_port: int
    broker_is_active: bool
