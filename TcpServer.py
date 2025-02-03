import json
import socket
import logging
import threading
from datetime import datetime
import os
from queue import Queue
import time
import select

SETTINGS_FILE = "Settings.json"
ATTLOG_FILE = "attlog.json"
MAX_BUFFER_SIZE = 2097152  # 2 MB

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {"System para": [{"Rec Count": "0"}]}
    
    with open(SETTINGS_FILE, "r") as file:
        return json.load(file)

def save_settings(settings):
    with open(SETTINGS_FILE, "w") as file:
        json.dump(settings, file, indent=4)

def update_record_count():
    settings = load_settings()
    settings["System para"][0]["Rec Count"] = str(int(settings["System para"][0]["Rec Count"]) + 1)
    save_settings(settings)

class TcpServer:
    def __init__(self, settings_file):
        self.settings_file = settings_file
        self.load_settings()
        self.queue = Queue()

    def load_settings(self):
        with open(self.settings_file, "r") as file:
            self.settings = json.load(file)
        self.devices = self.settings["devices"]

    def handle_device(self, host, port, queue):
        logger.info(f"🔍 Starting TCP handler for {host}:{port}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((host, port))
            server_socket.listen()
            logger.info(f"✅ Listening for connections on {host}:{port}")

            while True:
                conn, addr = server_socket.accept()
                logger.info(f"🚀 New Connection from {addr} on port {port}")

                with conn:
                    while True:
                        data = conn.recv(MAX_BUFFER_SIZE)
                        if not data:
                            logger.info(f"❌ Connection closed by {addr}")
                            break
                        decoded_data = data.decode('utf-8', errors='ignore')
                        logger.info(f"📥 Received Data from {addr}: {decoded_data}")
                        queue.put(decoded_data)
                        update_record_count()

    def write_to_attlog(self):
        while True:
            data = self.queue.get()
            if not data:
                continue

            if not os.path.exists(ATTLOG_FILE):
                with open(ATTLOG_FILE, 'w') as f:
                    json.dump([], f)

            with open(ATTLOG_FILE, 'r+') as f:
                content = json.load(f)
                content.append({"Timestamp": datetime.now().strftime("%Y/%m/%d %H:%M:%S"), "Data": data})
                f.seek(0)
                json.dump(content, f, indent=4)
            self.queue.task_done()

    def start_server(self):
        writer_thread = threading.Thread(target=self.write_to_attlog)
        writer_thread.start()

        threads = []
        for device in self.devices:
            thread = threading.Thread(target=self.handle_device, args=(device["ip"], device["port"], self.queue))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        
        writer_thread.join()

if __name__ == "__main__":
    tcp_server = TcpServer(SETTINGS_FILE)
    tcp_server.start_server()
