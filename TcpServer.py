import json
import socket
import logging
import threading
import os
import time
from queue import Queue

SETTINGS_FILE = "Settings.json"
ATTLOG_FILE = "attlog.json"
MAX_BUFFER_SIZE = 2097152  # 2MB buffer size

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {"devices": []}
    with open(SETTINGS_FILE, "r") as file:
        return json.load(file)

class TcpServer:
    def __init__(self, settings_file):
        self.settings_file = settings_file
        self.load_settings()
        self.queue = Queue()
        self.running = True

    def load_settings(self):
        with open(self.settings_file, "r") as file:
            self.settings = json.load(file)
        self.devices = self.settings.get("devices", [])

    def send_http_response(self, conn):
        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Connection: Keep-Alive\r\n"
            "\r\n"
            "OK"
        )
        conn.sendall(response.encode('utf-8'))

    def handle_client(self, conn, addr, port):
        logger.info(f"🚀 Connection established from {addr} on port {port}")

        while self.running:
            try:
                data = conn.recv(MAX_BUFFER_SIZE)
                if not data:
                    logger.info(f"❌ Connection lost from {addr}. Reconnecting...")
                    break

                decoded_data = data.decode('utf-8', errors='ignore')
                logger.info(f"📥 Received Data from {addr}: {decoded_data}")
                self.queue.put(decoded_data)

                self.send_http_response(conn)

            except socket.error as e:
                logger.error(f"⚠ Connection error with {addr}: {e}")
                break

        conn.close()
        time.sleep(2)

    def start_listener(self, port):
        while self.running:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_socket.bind(("0.0.0.0", port))
                    server_socket.listen(5)
                    logger.info(f"✅ Listening for connections on 0.0.0.0:{port}")

                    while self.running:
                        conn, addr = server_socket.accept()
                        client_thread = threading.Thread(target=self.handle_client, args=(conn, addr, port))
                        client_thread.start()

            except Exception as e:
                logger.error(f"❌ Error on port {port}: {e}")
                time.sleep(5)

    def start_server(self):
        threads = []
        for device in self.devices:
            port = device.get("port")
            if port:
                thread = threading.Thread(target=self.start_listener, args=(port,))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

if __name__ == "__main__":
    tcp_server = TcpServer(SETTINGS_FILE)
    tcp_server.start_server()
