FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential \
    libpaho-mqtt-dev \
    libhiredis-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 코드 복사
COPY main.c ./

# 빌드
RUN gcc -Wall -O2 -o subscriber main.c -lpaho-mqtt3c -lhiredis

# 실행 (백그라운드 통신 프로세스)
CMD ["./subscriber"]
