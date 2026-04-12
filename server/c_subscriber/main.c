#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <MQTTClient.h>
#include <hiredis/hiredis.h>

/**
 * 전역 설정 및 환경 변수 정의
 * ADDRESS: Docker 네트워크 내의 mosquitto-1 브로커 주소
 * TOPIC: 와일드카드(#)를 사용하여 모든 카메라 스트림을 수신
 */
#define ADDRESS     "tcp://mosquitto-1:1883"
#define CLIENTID    "C_Video_Subscriber"
#define TOPIC       "video/stream/#"
#define QOS         0
#define TIMEOUT     10000L

#define REDIS_HOST  "redis"
#define REDIS_PORT  6379

// Redis 공유 컨텍스트
redisContext *redis_conn = NULL;

/**
 Redis 연결 초기화 함수
 성공 시 연결 객체를 생성하고, 실패 시 에러 로그와 함께 프로그램을 종료함.
 */
void init_redis() {
    redis_conn = redisConnect(REDIS_HOST, REDIS_PORT);
    if (redis_conn != NULL && redis_conn->err) {
        printf("Error: %s\n", redis_conn->errstr);
        exit(1);
    }
    printf("Connected to Redis successfully.\n");
}

/**
 MQTT 메시지 수신 시 실행되는 콜백 함수 (Core Logic)
 1. Topic Parsing: "video/stream/{camera_id}" 구조에서 camera_id를 추출한다.
 2. Binary Handling: 수신된 바이너리 프레임 데이터를 보존한다.
 3. Redis Publish: Redis의 PUBLISH 명령을 사용하여 특정 채널로 데이터를 쏜다.
 */
int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message) {
    if (redis_conn == NULL) return 1;

    // 토픽 문자열 복사 (파싱 시 원본 보존을 고려할 수 있음)
    char *token = strtok(topicName, "/");
    char *camera_id = NULL;
    int depth = 0;
    
    // "video/stream/cam1" 형태에서 세 번째 인자인 cam1 추출
    while (token != NULL) {
        if (depth == 2) {
            camera_id = token;
            break;
        }
        token = strtok(NULL, "/");
        depth++;
    }

    if (camera_id != NULL) {
        // Redis 채널명 생성: "video:stream:cam1"
        char redis_channel[128];
        snprintf(redis_channel, sizeof(redis_channel), "video:stream:%s", camera_id);

        // hiredis의 %b 포맷을 사용하여 이미지 바이너리 전송 (데이터 크기 명시 필수)
        redisReply *reply = redisCommand(redis_conn, "PUBLISH %s %b", redis_channel, message->payload, (size_t)message->payloadlen);
        if (reply != NULL) {
            freeReplyObject(reply);
        } else {
            printf("Failed to publish to redis\n");
        }
    }

    // MQTT 라이브러리에서 할당한 메모리 명시적 해제
    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);
    return 1;
}

/**
 연결 유실 시 호출되는 콜백 함수
 네트워크 불안정으로 인해 브로커와 단절될 경우 원인을 출력함.
 */
void connlost(void *context, char *cause) {
    printf("\nConnection lost\n     cause: %s\n", cause);
}

/**
 메인 실행 루프
 1. Redis 초기화 -> MQTT 클라이언트 생성 순으로 진행한다.
 2. 콜백 함수(메시지 수신, 연결 유실)를 등록한다.
 3. 브로커에 접속한 후 지정된 토픽을 구독(Subscribe)한다.
 */
int main(int argc, char* argv[]) {
    init_redis();

    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    int rc;

    // 클라이언트 인스턴스 생성
    if ((rc = MQTTClient_create(&client, ADDRESS, CLIENTID, MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS) {
        printf("Failed to create client, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    // 콜백 함수 등록
    if ((rc = MQTTClient_setCallbacks(client, NULL, connlost, msgarrvd, NULL)) != MQTTCLIENT_SUCCESS) {
        printf("Failed to set callbacks, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;

    // MQTT 브로커 연결 시도
    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS) {
        printf("Failed to connect to MQTT, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }
    
    printf("Connected to MQTT broker successfully.\n");

    // 영상 스트림 토픽 구독
    if ((rc = MQTTClient_subscribe(client, TOPIC, QOS)) != MQTTCLIENT_SUCCESS) {
        printf("Failed to subscribe, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }
    printf("Subscribing to topic: %s\n", TOPIC);

    printf("Listening for video frames... Press Ctrl-C to quit.\n");
    
    // 메인 루프 유지: 1초마다 sleep하며 콜백이 백그라운드에서 동작하도록 함
    while(1) {
        sleep(1);
    }

    // 자원 해제 및 종료
    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);
    redisFree(redis_conn);
    return rc;
}