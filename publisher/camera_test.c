#include <mosquitto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TARGET_QOS 0          
#define TOTAL_MESSAGES 100    

#define CAMERA_CMD "rpicam-vid -t 0 --inline -n --width 640 --height 480 --codec mjpeg -o -"

void on_connect(struct mosquitto *mosq, void *obj, int reason_code) {
    printf("on_connect: %s\n", mosquitto_connack_string(reason_code));
    if(reason_code != 0) exit(1);
}

int main(int argc, char *argv[]) {
    struct mosquitto *mosq;
    int rc;

    mosquitto_lib_init();
    mosq = mosquitto_new("cctv_publisher", true, NULL); // 클라이언트 ID 명시
    
    mosquitto_connect_callback_set(mosq, on_connect);

    // 라즈베리 파이 로컬 브로커에 연결
    rc = mosquitto_connect(mosq, "localhost", 1883, 60);
    if(rc != MOSQ_ERR_SUCCESS) return 1;

    mosquitto_loop_start(mosq);

    FILE *camera_pipe = popen(CAMERA_CMD, "r");
    if (camera_pipe == NULL) {
        perror("카메라 실행 실패");
        return 1;
    }

    unsigned char buffer[65535]; // 프레임 데이터를 담을 버퍼
    printf("실시간 스트리밍 시작\n");

    unsigned char temp_buf[4096]; // 조금씩 읽어오기 위한 작은 버퍼
    unsigned char frame_buf[200000]; // 한 장의 사진을 모으는 큰 버퍼
    int frame_ptr = 0;

    while (1) {
        size_t n = fread(temp_buf, 1, sizeof(temp_buf), camera_pipe);
        for (size_t i = 0; i < n; i++) {
            frame_buf[frame_ptr++] = temp_buf[i];

            // JPEG EOI (0xFF 0xD9) 확인
            if (frame_ptr >= 2 && frame_buf[frame_ptr-2] == 0xFF && frame_buf[frame_ptr-1] == 0xD9) {
                // 사진의 끝이 오면 publish
                mosquitto_publish(mosq, NULL, "cctv/stream", frame_ptr, frame_buf, 0, false);
                frame_ptr = 0; // 다음 사진을 위해 포인터 초기화
            }
        }
    }

    pclose(camera_pipe);

    printf("CCTV Streaming Finished.\n");
    sleep(1);

    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return 0;
}