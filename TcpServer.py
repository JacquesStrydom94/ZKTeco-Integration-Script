import socket
import threading
import logging
import os
import json
import time
import psutil  # ✅ Used to check & free ports
from datetime import datetime

# ✅ Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

MAX_RETRIES = 5  # ✅ Retry binding if port is in use
MAX_BUFFER_SIZE = 2097152  # ✅ 2MB max buffer per connection
ATTLOG_FILENAME = "attlog.json"


class TcpServer:
    def __init__(self, settings_file):
        self.settings_file = settings_file
        self.load_settings()
        self.attlog_lock = threading.Lock()  # ✅ Lock to prevent race conditions

    def load_settings(self):
        """Load settings from Settings.json."""
        if not os.path.exists(self.settings_file):
            logger.error(f"⚠️ Settings file '{self.settings_file}' not found. Exiting...")
            exit(1)

        with open(self.settings_file, "r") as file:
            self.settings = json.load(file)

        self.ports = [entry["port"] for entry in self.settings.get("devices", []) if "port" in entry]

    def free_port(self, port):
        """Check and free a port if it's already in use."""
        for conn in psutil.net_connections(kind="inet"):
            if conn.laddr.port == port:
                try:
                    proc = psutil.Process(conn.pid)
                    logger.warning(f"⚠ Killing process {proc.pid} ({proc.name()}) using port {port}")
                    proc.terminate()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    logger.warning(f"⚠ Port {port} is in use but cannot be freed. Trying anyway...")

    def listen_for_connections(self, port):
        """Listen for incoming TCP connections."""
        retries = 0
        while retries < MAX_RETRIES:
            try:
                self.free_port(port)  # ✅ Ensure the port is free before binding

                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # ✅ Allow port reuse
                server_socket.bind(("0.0.0.0", port))
                server_socket.listen(5)

                logger.info(f"✅ Listening for connections on 0.0.0.0:{port}")

                while True:
                    client_socket, addr = server_socket.accept()
                    logger.info(f"🚀 Connection established from {addr} on port {port}")
                    threading.Thread(target=self.handle_client, args=(client_socket, addr, port)).start()

            except OSError as e:
                if e.errno == 98:  # Address already in use
                    retries += 1
                    logger.error(f"❌ Port {port} is already in use. Retrying in 5 seconds... (Attempt {retries}/{MAX_RETRIES})")
                    time.sleep(5)  # Wait before retrying
                else:
                    logger.error(f"❌ Unexpected error binding to port {port}: {e}")
                    break

            finally:
                try:
                    server_socket.close()
                except NameError:
                    pass  # Ignore if server_socket wasn't created

        logger.critical(f"🔥 Failed to bind to port {port} after {MAX_RETRIES} attempts. Exiting...")

    def handle_client(self, client_socket, addr, port):
        """Handle a single client connection."""
        try:
            while True:
                data = client_socket.recv(MAX_BUFFER_SIZE)
                if not data:
                    logger.info(f"❌ Connection lost from {addr}. Reconnecting...")
                    break

                data_str = data.decode('utf-8', errors='ignore')
                logger.info(f"📥 FULL TCP DATA from {addr}:\n{data_str}\n{'-'*60}")

                # ✅ Log all TCP data, but only process ATTLOG records
                if "=ATTLOG&Stamp=9999" not in data_str:
                    logger.warning(f"⚠ Non-ATTLOG Data Received (IGNORED): {data_str[:200]}...")
                    continue

                # ✅ Extract ATTLOG data and write to attlog.json
                attlog_data = self.extract_attlog(data_str)
                if attlog_data:
                    self.write_to_attlog(attlog_data)

        except IOError as e:
            logger.error(f"❌ I/O Error in handle_client: {e}")

        except Exception as e:
            logger.error(f"❌ Unexpected Error in handle_client: {e}")

        finally:
            client_socket.close()
            logger.info(f"🔌 Connection closed: {addr}")

    def extract_attlog(self, data_str):
        """Extract ATTLOG data from received TCP data."""
        try:
            attlog_start = data_str.find("Content-Length:")
            if attlog_start == -1:
                return None

            content_length_start = attlog_start + len("Content-Length:")
            content_length_end = data_str.find("\n", content_length_start)
            content_length = int(data_str[content_length_start:content_length_end].strip())

            data_start = data_str.find("\n", content_length_end) + 1
            attlog_data = data_str[data_start:data_start + content_length].strip()

            return attlog_data.split("\n")  # ✅ Split into multiple entries

        except Exception as e:
            logger.error(f"❌ Error extracting ATTLOG data: {e}")
            return None

    def write_to_attlog(self, attlog_entries):
        """Writes extracted ATTLOG data to attlog.json safely."""
        try:
            with self.attlog_lock:  # ✅ Prevent multiple threads from writing simultaneously
                if not os.path.exists(ATTLOG_FILENAME):
                    with open(ATTLOG_FILENAME, 'w') as f:
                        json.dump([], f)

                with open(ATTLOG_FILENAME, 'r+') as f:
                    try:
                        content = json.load(f)
                    except json.JSONDecodeError:
                        content = []

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

                    f.seek(0)
                    json.dump(content, f, indent=4)
                    f.truncate()  # ✅ Ensure old data is removed

        except IOError as e:
            logger.error(f"❌ I/O Error writing to attlog.json: {e}")

        except Exception as e:
            logger.error(f"❌ Unexpected Error writing to attlog.json: {e}")

    def start_server(self):
        """Start the TCP server and listen for connections on all configured ports."""
        if not self.ports:
            logger.error("❌ No ports configured in Settings.json! Exiting...")
            return

        threads = []
        for port in self.ports:
            thread = threading.Thread(target=self.listen_for_connections, args=(port,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    SETTINGS_FILE = "Settings.json"
    tcp_server = TcpServer(SETTINGS_FILE)
    tcp_server.start_server()
