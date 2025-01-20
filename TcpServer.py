import socket
from datetime import datetime
import logging
import threading
import json
import os
from queue import Queue
import time
import re
from attlog_parser import AttLogParser

# Configure logging with ANSI escape codes for colors
class CustomFormatter(logging.Formatter):
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RED = "\033[91m"
    PINK = "\033[95m"
    RESET = "\033[0m"
    FORMATS = {
        logging.INFO: "%(asctime)s - %(levelname)s - %(message)s",
        'DEFAULT': "%(asctime)s - %(levelname)s - %(message)s"
    }

    def format(self, record):
        if "Connected by" in record.msg or "Server listening" in record.msg:
            log_fmt = self.GREEN + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Received from client" in record.msg:
            log_fmt = self.YELLOW + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Writing to file" in record.msg or "Parsed JSON packet" in record.msg:
            log_fmt = self.BLUE + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Closing connection" in record.msg:
            log_fmt = self.RED + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Writing new entries to attlog.json" in record.msg:
            log_fmt = self.PINK + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        else:
            log_fmt = self.FORMATS.get(record.levelno, self.FORMATS['DEFAULT'])
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logging.basicConfig(level=logging.DEBUG, handlers=[handler])

class TcpServer:
    def __init__(self, devices_file, attlog_filename):
        self.devices_file = devices_file
        self.load_devices()
        self.attlog_filename = attlog_filename
        self.local_ip = self.get_local_ip()

    def get_local_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Connect to an external IP address (Google DNS)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception as e:
            logging.error(f"Failed to get local IP address: {e}")
            ip = "0.0.0.0"
        finally:
            s.close()
        return ip

    def load_devices(self):
        with open(self.devices_file, "r") as file:
            self.devices = json.load(file)

    def extract_attlog(self, data):
        content_length_index = data.find("Content-Length:")
        if content_length_index == -1:
            return None

        content_length_start = content_length_index + len("Content-Length:")
        content_length_end = data.find("\n", content_length_start)
        content_length = int(data[content_length_start:content_length_end].strip())

        data_start = data.find("\n", content_length_end) + 1
        attlog_data = data[data_start:data_start + content_length].strip()
        
        return attlog_data

    def extract_sn(self, data_str):
        sn_match = re.search(r'SN=([^&]+)', data_str)
        if sn_match:
            return sn_match.group(1)
        else:
            return None

    def write_to_file(self, queue, filename):
        while True:
            json_packet = queue.get()
            if json_packet is None:
                break
            logging.debug(f"Writing to file: {json_packet}")
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    json.dump([], f)
            with open(filename, 'r+') as f:
                data = json.load(f)
                data.append(json_packet)
                f.seek(0)
                json.dump(data, f, indent=2)
            queue.task_done()

    def handle_device(self, host, port, device_ip, queue):
        retry_count = 0
        max_retries = 5
        retry_delay = 5

        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind((host, port))
                    s.listen()
                    s.settimeout(60)  # Increase socket timeout
                    logging.info(f"Server listening on {host}:{port}")
                    while True:
                        try:
                            conn, addr = s.accept()
                            with conn:
                                logging.info(f"Connected by {addr} (expected {device_ip})")
                                conn.settimeout(60)
                                while True:
                                    try:
                                        data = conn.recv(4096)
                                        if not data:
                                            break
                                        data_str = data.decode('utf-8')
                                        logging.debug(f"Received data from client: {data_str}")
                                        
                                        if "=ATTLOG&Stamp=9999" in data_str:
                                            attlog_data = self.extract_attlog(data_str)
                                            sn_value = self.extract_sn(data_str)
                                            logging.debug(f"Extracted attlog_data: {attlog_data}")
                                            logging.debug(f"Extracted sn_value: {sn_value}")
                                            
                                            if attlog_data and sn_value:
                                                # Split the attlog_data into individual log entries
                                                log_entries = attlog_data.split("\n")
                                                for entry in log_entries:
                                                    if entry.strip():  # Check if the entry is not empty
                                                        combined_value = f"{entry}\t{addr}\t{sn_value}"
                                                        log_entry = AttLogParser.parse_attlog(combined_value)
                                                        logging.info(f"Parsed log entry: {log_entry}")
                                                        
                                                        logging.debug(f"Adding log entry to queue: {log_entry}")
                                                        queue.put(log_entry)

                                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]
                                        response = f"Server Send Data: {timestamp}\nOK"
                                        conn.sendall(response.encode())
                                    except socket.timeout:
                                        logging.info(f"Connection timeout with {addr}")
                                        break
                        except socket.timeout:
                            logging.info("Listening socket timeout")
            except OSError as e:
                if e.errno == 98:
                    retry_count += 1
                    if retry_count > max_retries:
                        logging.error(f"Port {port} is still in use after {max_retries} retries. Exiting.")
                        break
                    logging.error(f"Port {port} is already in use. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise

    def start_server(self):
        queue = Queue()
        
        writer_thread = threading.Thread(target=self.write_to_file, args=(queue, self.attlog_filename))
        writer_thread.start()
        logging.debug("Writer thread started")
        
        threads = []
        
        for device in self.devices:
            thread = threading.Thread(target=self.handle_device, args=(self.local_ip, device['port'], device['ip'], queue))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        queue.put(None)
        writer_thread.join()

    def run(self):
        if not os.path.exists(self.attlog_filename):
            with open(self.attlog_filename, 'w') as f:
                json.dump([], f)

        parser_thread = threading.Thread(target=AttLogParser().run)
        parser_thread.start()

        self.start_server()

if __name__ == "__main__":
    devices_file = "devices.json"
    attlog_filename = "attlog.json"

    tcp_server = TcpServer(devices_file, attlog_filename)
    tcp_server.run()
