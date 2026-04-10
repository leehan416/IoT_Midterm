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
