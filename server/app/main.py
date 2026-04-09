from fastapi import FastAPI

app = FastAPI(title="iot-midterm-server")


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
