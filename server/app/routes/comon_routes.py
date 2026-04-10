from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/")
async def home(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"title": "IoT Midterm"},
    )


@router.get("/status")
async def status(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="status.html",
        context={"title": "Broker Status"},
    )
