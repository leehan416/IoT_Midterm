# IoT Midterm

## Project Structure

```text
.
├── docker-compose.yml
├── publisher/
│   ├── http_requester.py
│   ├── publisher.py
│   ├── readme.md
│   └── requirements.txt
├── readme.md
└── server/
    ├── Dockerfile
    ├── app/
    │   ├── config/
    │   ├── models/
    │   ├── repository/
    │   ├── routes/
    │   ├── scheduler/
    │   ├── schemas/
    │   ├── services/
    │   ├── static/
    │   └── templates/
    ├── pyproject.toml
    ├── readme.md
    └── uv.lock
```

## Run Server

From the project root:

```bash
# server
docker compose up -d --build

# broker register
cd publisher 
python3 http_requester.py {server_host} {server_port}

# publisher
cd publisher 
python3 -m venv .venv
source .venv/bin/activate

pip install paho-mqtt requests

SERVER_URL=http://localhost:80 PUBLISHER_ID=camera-1 python3 publisher.py
#SERVER_URL={server_host} PUBLISHER_ID={TOPIC} python3 publisher.py
```

Access:
- `http://localhost/` (Nginx, port 80)

Stop:

```bash
docker compose down
```

## Compose env

`.env.sample`:

```env
REDIS_HOST=redis
REDIS_PORT=6379

MQTT_HOST=mosquitto
MQTT_PORT=1883
```

`generate env` :
``` bash
cat > .env <<'EOF'
REDIS_HOST=redis
REDIS_PORT=6379

MQTT_HOST=mosquitto
MQTT_PORT=1883
EOF
```
