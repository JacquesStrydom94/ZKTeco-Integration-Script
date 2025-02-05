import socket
import threading
import json
import os
import logging
from datetime import datetime

# Set the maximum buffer size per connection attempt (2 MB)
MAX_BUFFER_SIZE = 2097152  # 2 MB
ATTLOG_FILE = "attlog.json"
SETTINGS_FILE = "Settings.json"

# Logger Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


class TcpServer:
    def __init__(self, settings_file):
        self.settings_file = settings_file
        self.load_settings()

    def load_settings(self):
        """Loads device configurations from Settings.json."""
        with open(self.settings_file, "r") as file:
            self.settings = json.load(file)
        self.devices = self.settings.get("devices", [])  # Load all devices

    def extract_attlog(self, data_str):
        """Extracts ATTLOG data from incoming TCP packets."""
        try:
            if "=ATTLOG&Stamp=9999" not in data_str:
                return None  # Ignore non-ATTLOG data

            lines = data_str.strip().split("\n")
            attlog_entries = [line.strip() for line in lines if line.strip() and not line.startswith("GET")]
            return "\n".join(attlog_entries)  # Join valid lines into a single string
        except Exception as e:
            logger.error(f"❌ Error extracting ATTLOG data: {e}")
            return None

    def write_to_attlog(self, log_data):
        """Safely writes ATTLOG data to attlog.json, skipping malformed entries."""
        if not os.path.exists(ATTLOG_FILE):
            with open(ATTLOG_FILE, 'w') as f:
                json.dump([], f)

        try:
            with open(ATTLOG_FILE, 'r+') as f:
                try:
                    content = json.load(f)  # Load existing data
                except json.JSONDecodeError:
                    content = []  # If JSON is corrupted, reset to an empty list

                log_entries = log_data.strip().split("\n")

                for entry in log_entries:
                    parts = entry.split()

                    # ✅ Ensure data has at least 5 fields before processing
                    if len(parts) < 5:
                        logger.warning(f"⚠ Skipping malformed entry (not enough fields): {entry}")
                        continue  # Skip invalid data

                    try:
                        log_entry = {
                            "ZKID": parts[0],
                            "Timestamp": f"{parts[1]} {parts[2]}" if len(parts) > 2 else "Unknown",
                            "InorOut": int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0,  # Default to 0
                            "attype": int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0,  # Default to 0
                            "Device": "Unknown",
                            "SN": "Unknown",
                            "Devrec": "N/A"
                        }
                        content.append(log_entry)

                    except ValueError as e:
                        logger.error(f"❌ Invalid data format in entry: {entry} | Error: {e}")
                        continue  # Skip this entry instead of crashing

                f.seek(0)
                json.dump(content, f, indent=4)

            logger.info(f"✅ Successfully wrote {len(log_entries)} entries to attlog.json")

        except json.JSONDecodeError as e:
            logger.error(f"❌ Error reading/writing attlog.json: {e}")

    def handle_client(self, client_socket, addr, port):
        """Handles incoming TCP client connections securely."""
        try:
            logger.info(f"🚀 Connection established from {addr} on port {port}")

            while True:
                data = client_socket.recv(MAX_BUFFER_SIZE)
                if not data:
                    logger.info(f"❌ Connection lost from {addr}. Reconnecting...")
                    break  # Exit loop if no data is received

                data_str = data.decode('utf-8', errors='ignore')
                logger.info(f"📥 Received Data from {addr}: {data_str}")

                # ✅ Ensure only ATTLOG data is processed
                if "=ATTLOG&Stamp=9999" not in data_str:
                    logger.warning(f"⚠ Skipping non-ATTLOG data from {addr}: {data_str[:100]}...")  # Log first 100 chars
                    continue  # Skip processing this data

                # Extract ATTLOG data after the header
                attlog_data = self.extract_attlog(data_str)
                if attlog_data:
                    self.write_to_attlog(attlog_data)

        except Exception as e:
            logger.error(f"❌ Error in handle_client: {e}")
        finally:
            client_socket.close()
            logger.info(f"🔌 Connection closed: {addr}")

    def start_server(self):
        """Starts the TCP server for each configured device."""
        threads = []
        for device in self.devices:
            ip, port = device["ip"], device["port"]
            thread = threading.Thread(target=self.listen_for_connections, args=(ip, port))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def listen_for_connections(self, host, port):
        """Listens for incoming TCP connections on the specified host and port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen(5)
            logger.info(f"✅ Listening for connections on {host}:{port}")

            while True:
                client_socket, addr = server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, addr, port))
                client_thread.start()


if __name__ == "__main__":
    tcp_server = TcpServer(SETTINGS_FILE)
    tcp_server.start_server()
