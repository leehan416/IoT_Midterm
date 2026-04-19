import sys
import socket
import requests

def get_my_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

def main():
    if len(sys.argv) < 2:
        print("usage: python3 http_requester.py <api_host> [api_port]")
        print("example: python3 http_requester.py 192.168.0.16")
        return

    api_host = sys.argv[1]
    api_port = int(sys.argv[2]) if len(sys.argv) >= 3 else 8000

    endpoint = f"http://{api_host}:{api_port}/api/mqtt"

    my_ip = get_my_ip()
    payload = {
        "mqtt_host": my_ip,   
        "mqtt_port": 1883
    }

    print("endpoint:", endpoint)
    print("payload :", payload)

    try:
        resp = requests.post(endpoint, json=payload, timeout=5)
        print("status:", resp.status_code)
        try:
            print("body:", resp.json())
        except ValueError:
            print("body(text):", resp.text)
    except requests.RequestException as e:
        print("request failed:", e)

if __name__ == "__main__":
    main()
