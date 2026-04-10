# Server Directory

## Structure

```text
server/
├── .env
├── .env.example
├── Dockerfile
├── app/
│   ├── config/
│   │   ├── mqtt.py
│   │   ├── redis.py
│   │   └── settings.py
│   ├── main.py
│   ├── routes/
│   │   ├── api_routes.py
│   │   ├── comon_routes.py
│   │   └── mqtt_routes.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── comon_service.py
│   │   └── mqtt_service.py
│   ├── static/
│   │   └── css/
│   │       └── app.css
│   └── templates/
│       └── index.html
├── pyproject.toml
├── readme.md
└── uv.lock
```

## Notes

- `main.py` mounts static files and registers routers.
- `routes/` contains page and API endpoints.
- `services/` contains business logic.
- `config/` contains Redis, MQTT, and settings loaders.
