import socket
import threading
import json
import os
import logging
from datetime import datetime, timezone, timedelta
import time

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S:%f')

logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[
    logging.FileHandler("server.log"),
    logging.StreamHandler()
])
logger = logging.getLogger()
formatter = CustomFormatter()
for handler in logger.handlers:
    handler.setFormatter(formatter)

class Cmd:
    def __init__(self, settings_file, devices_file):
        self.settings_file = settings_file
        self.devices_file = devices_file
        self.load_settings()
        self.load_devices()
        self.cmd_count_file = "cmd_count.json"
        self.cmd_count = self.load_cmd_count()

    def load_settings(self):
        with open(self.settings_file, "r") as file:
            data = json.load(file)
            self.target_time = data["target_time"]
            self.cmd_template = data["cmd"]

    def load_devices(self):
        with open(self.devices_file, "r") as file:
            self.devices = json.load(file)

    def load_cmd_count(self):
        if os.path.exists(self.cmd_count_file):
            with open(self.cmd_count_file, "r") as file:
                data = json.load(file)
                return data.get("cmd_count", 0)
        else:
            with open(self.cmd_count_file, "w") as file:
                json.dump({"cmd_count": 0}, file)
            return 0

    def save_cmd_count(self, count):
        with open(self.cmd_count_file, "w") as file:
            json.dump({"cmd_count": count}, file)

    def handle_client(self, client_socket):
        try:
            start_time = datetime.now().strftime('%Y/%m/%d')
            end_time = datetime.now().strftime('%Y/%m/%d')
            cmd = self.cmd_template.replace("startTime", start_time).replace("endTime", end_time)
            message = f"C:{self.cmd_count}:{cmd}"
            client_socket.sendall(message.encode('utf-8'))
            logger.info("Server Send Data: {} - {}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'),
                message))

            command_sent = False

            while True:
                try:
                    response = client_socket.recv(1024).decode('utf-8')
                    if response:
                        logger.info("Client Response: {} - {}".format(
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'),
                            response))
                        client_socket.sendall("OK".encode('utf-8'))

                        if not command_sent and f"ID={self.cmd_count}&Return=0&CMD=" in response:
                            self.cmd_count += 1
                            self.save_cmd_count(self.cmd_count)
                            date_now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
                            http_response = (
                                "HTTP/1.1 200 OK\n"
                                "Content-Type: text/plain\n"
                                "Accept-Ranges: bytes\n"
                                f"Date: {date_now}\n"
                                "Content-Length: 4\n"
                            )
                            client_socket.sendall(http_response.encode('utf-8'))
                            command_sent = True
                        else:
                            logger.info("Only receiving data")
                except socket.error as e:
                    logger.error(f"Socket error: {e.strerror} (Error code: {e.errno})")
                except Exception as e:
                    logger.error(f"Error receiving data from client: {e}")

                time.sleep(1)
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            if client_socket:
                client_socket.close()
            logger.info("Client connection closed")

    def start_server(self):
        server_address = ("0.0.0.0", self.port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind(server_address)
            server_socket.listen(5)
            logger.info(f"Server listening on 0.0.0.0:{self.port}")
            while True:
                client_socket, addr = server_socket.accept()
                logger.info(f"Accepted connection from {addr}")
                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                client_handler.start()

    def wait_until_specified_time(self):
        now = datetime.now()
        target_hour, target_minute = map(int, self.target_time.split(":"))
        target_datetime = datetime.combine(now.date(), datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute)
        if target_datetime <= now:
            target_datetime += timedelta(days=1)
        time_until_target = (target_datetime - now).total_seconds()
        time.sleep(time_until_target)
        logger.info(f"It's {self.target_time}! Starting the server...")
        self.start_server()

