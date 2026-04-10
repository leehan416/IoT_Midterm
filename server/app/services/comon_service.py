from app.schemas.comon_schemas import HealthCheckResponse


async def check_health() -> HealthCheckResponse:
    return HealthCheckResponse(status="ok")
