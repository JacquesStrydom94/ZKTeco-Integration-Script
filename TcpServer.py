import socket
import threading
import logging
import json
import os
from datetime import datetime

# Maximum buffer size per connection attempt (2MB)
MAX_BUFFER_SIZE = 2097152

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

class TcpServer:
    def __init__(self, settings_file):
        self.settings_file = settings_file
        self.load_settings()
        self.attlog_filename = "attlog.json"

    def load_settings(self):
        """Load settings from JSON file."""
        with open(self.settings_file, "r") as file:
            self.settings = json.load(file)
        self.devices = self.settings.get("devices", [])

    def start_server(self):
        """Start TCP server on multiple ports as defined in settings.json."""
        for device in self.devices:
            thread = threading.Thread(target=self.listen_for_connections, args=(device["port"],))
            thread.start()

    def listen_for_connections(self, port):
        """Listens for incoming TCP connections on the specified port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("0.0.0.0", port))  # ✅ Accept connections from any IP
            server_socket.listen(5)
            logger.info(f"✅ Listening for connections on 0.0.0.0:{port}")

            while True:
                client_socket, addr = server_socket.accept()
                logger.info(f"🚀 New connection from {addr} on port {port}")

                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, addr, port))
                client_thread.start()

    def handle_client(self, client_socket, addr, port):
        """Handles incoming TCP client connections securely."""
        try:
            logger.info(f"🔌 Connection established from {addr} on port {port}")

            while True:
                data = client_socket.recv(MAX_BUFFER_SIZE)
                if not data:
                    logger.info(f"❌ Connection lost from {addr}. Reconnecting...")
                    break  # Exit loop if no data is received

                # Decode data
                data_str = data.decode('utf-8', errors='ignore')
                logger.info(f"📥 FULL TCP DATA from {addr}:\n{data_str}\n{'-'*60}")

                # ✅ Log all TCP data, even if it's not ATTLOG
                if "=ATTLOG&Stamp=9999" not in data_str:
                    logger.warning(f"⚠ Non-ATTLOG Data Received (IGNORED): {data_str[:200]}...")  # Log first 200 chars
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

    def extract_attlog(self, data_str):
        """Extracts ATTLOG data from TCP payload."""
        try:
            lines = data_str.split("\n")
            attlog_entries = [line.strip() for line in lines if line.strip() and not line.startswith("POST")]
            return attlog_entries
        except Exception as e:
            logger.error(f"❌ Error extracting ATTLOG: {e}")
            return None

    def write_to_attlog(self, attlog_entries):
        """Writes extracted ATTLOG data to attlog.json."""
        try:
            if not os.path.exists(self.attlog_filename):
                with open(self.attlog_filename, 'w') as f:
                    json.dump([], f)

            with open(self.attlog_filename, 'r+') as f:
                content = json.load(f)
                for entry in attlog_entries:
                    parts = entry.split()
                    if len(parts) < 5:
                        logger.warning(f"⚠ Skipping malformed ATTLOG entry: {entry}")
                        continue

                    record = {
                        "ZKID": parts[0],
                        "Timestamp": parts[1] + " " + parts[2],
                        "InorOut": parts[3],
                        "attype": parts[4]
                    }

                    content.append(record)
                    logger.info(f"✅ Added new ATTLOG record: {record}")

                # Write updated data
                f.seek(0)
                json.dump(content, f, indent=4)

        except Exception as e:
            logger.error(f"❌ Error writing to attlog.json: {e}")
