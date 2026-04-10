from fastapi import APIRouter

from app.services.comon_service import check_health

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return await check_health()
