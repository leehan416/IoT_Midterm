# IoT Midterm

## Project Structure

```text
.
├── data/
│   ├── mosquitto/
│   │   ├── config/
│   │   ├── data/
│   │   └── log/
│   ├── nginx/
│   └── redis/
├── docker-compose.yml
├── publisher/
│   └── readme.md
├── readme.md
└── server/
    ├── .env
    ├── .env.example
    ├── Dockerfile
    ├── app/
    ├── pyproject.toml
    ├── readme.md
    └── uv.lock
```

## Run Server

From the project root:

```bash
docker compose up -d --build
```

Access:

- `http://localhost/` (Nginx, port 80)
- `http://localhost:443/` (Nginx, port 443 over plain HTTP in current setup)

Stop:

```bash
docker compose down
```

## Streaming Architecture Change

- Removed: C subscriber + Redis Pub/Sub video pipeline.
- Added: FastAPI internal MQTT subscriber -> in-memory hub -> WebSocket push (`/api/ws/video/{camera_id}`).
- MQTT broker management APIs (`/api/mqtt`, `/api/mqtt/status`) are unchanged.
- Redis is still used for broker status/state storage only (temporary).

## Manual Test

1. Start stack:
```bash
docker compose up -d --build
```
2. Start publisher:
```bash
cd publisher
python publisher.py
```
3. Open [http://localhost/](http://localhost/) and check camera cards update.
4. In browser devtools, confirm WebSocket frames are received from `/api/ws/video/{camera_id}`.
