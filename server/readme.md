# Server Directory

## Structure

```text
server/
├── .env
├── .env.example
├── Dockerfile
├── app/
│   ├── config/
│   ├── main.py
│   ├── models/
│   ├── repository/
│   ├── routes/
│   ├── schemas/
│   ├── services/
│   ├── static/
│   └── templates/
├── pyproject.toml
├── readme.md
└── uv.lock
```

## Notes

- `main.py` mounts static files and registers routers.
- `routes/` contains page and API endpoints.
- `services/` contains business logic.
- `config/` contains Redis, MQTT, and settings loaders.
- `schemas/` contains request/response data schemas.
- `models/` contains domain/base model definitions.
- `repository/` contains data access layer code.

## Video Streaming (MQTT -> WebSocket)

1. Server starts an internal MQTT subscriber on app lifespan startup.
2. Incoming publisher JSON payload is parsed from MQTT:
   - expected image path: `data.image` (base64 jpeg)
   - camera id source: MQTT topic tail (`.../{camera_id}`), fallback to `publisher_id`
3. Latest frame is:
   - published to in-memory hub for WebSocket clients
   - saved as `app/static/uploads/{camera_id}/latest.jpg`
4. Frontend consumes `/api/ws/video/{camera_id}` and renders frames as `data:image/jpeg;base64,...`.

## Quick Validation

1. Run server:
   ```bash
   cd /Users/whal3/Documents/IoT_Midterm
   docker compose up -d --build
   ```
2. Run publisher (on device/environment with camera):
   ```bash
   cd /Users/whal3/Documents/IoT_Midterm/publisher
   python publisher.py
   ```
3. Open dashboard: `http://localhost/`
4. Verify:
   - browser devtools Network shows active `ws://.../api/ws/video/{camera_id}`
   - image cards update continuously
   - file updates at `server/app/static/uploads/{camera_id}/latest.jpg`
