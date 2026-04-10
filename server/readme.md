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
