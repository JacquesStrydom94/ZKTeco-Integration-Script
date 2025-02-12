import socket
import threading
import logging
import os
import json
import time
import psutil
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

DB_NAME = "PUSH.db"
TABLE_NAME = "attendance"

MAX_RETRIES = 5
MAX_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB
# The device might send "ATTLOG" data in a custom format. Adjust parsing to match your data.


class TcpServer:
    def __init__(self, settings_file):
        self.settings_file = settings_file
        self.load_settings()

    def load_settings(self):
        """Load ports from Settings.json."""
        if not os.path.exists(self.settings_file):
            logger.error(f"⚠️ Settings file '{self.settings_file}' not found. Exiting...")
            exit(1)

        with open(self.settings_file, "r") as file:
            self.settings = json.load(file)

        # Grab all 'port' entries from "devices"
        self.ports = [entry["port"] for entry in self.settings.get("devices", []) if "port" in entry]

    def free_port(self, port):
        """Check and free a port if it's in use (kill the process)."""
        for conn in psutil.net_connections(kind="inet"):
            if conn.laddr.port == port:
                try:
                    proc = psutil.Process(conn.pid)
                    logger.warning(f"⚠ Killing process {proc.pid} ({proc.name()}) using port {port}")
                    proc.terminate()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    logger.warning(f"⚠ Port {port} in use but cannot be freed. Retrying...")

    def listen_for_connections(self, port):
        """Bind & listen on a TCP port, accept connections, spawn threads for each client."""
        retries = 0
        while retries < MAX_RETRIES:
            try:
                self.free_port(port)

                server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind(("0.0.0.0", port))
                server_socket.listen(5)

                logger.info(f"✅ Listening for connections on 0.0.0.0:{port}")

                while True:
                    client_socket, addr = server_socket.accept()
                    logger.info(f"🚀 Connection from {addr} on port {port}")
                    threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, addr, port),
                        daemon=True
                    ).start()

            except OSError as e:
                if e.errno == 98:  # Address in use
                    retries += 1
                    logger.error(f"❌ Port {port} in use. Retry {retries}/{MAX_RETRIES} in 5s...")
                    time.sleep(5)
                else:
                    logger.error(f"❌ Unexpected error binding {port}: {e}")
                    break
            finally:
                try:
                    server_socket.close()
                except NameError:
                    pass

        logger.critical(f"🔥 Failed to bind port {port} after {MAX_RETRIES} attempts.")

    def handle_client(self, client_socket, addr, port):
        """Handle data from a single client connection."""
        try:
            while True:
                data = client_socket.recv(MAX_BUFFER_SIZE)
                if not data:
                    logger.info(f"❌ Connection lost from {addr}")
                    break

                data_str = data.decode('utf-8', errors='ignore')
                logger.info(f"📥 FULL TCP DATA from {addr}:\n{data_str}\n{'-'*60}")

                # 1) Look for an 'ATTLOG' indicator or a known pattern
                #    For example, your device might send lines that start with "ATTLOG" ...
                #    We'll do a very simple example parse:
                if "ATTLOG" not in data_str:
                    logger.warning("⚠ Non-ATTLOG data (ignored).")
                    continue

                # 2) Split lines by newline
                lines = data_str.split('\n')
                for line in lines:
                    line = line.strip()
                    if line.startswith("ATTLOG"):
                        # Example: "ATTLOG ZKID=12345 Timestamp=2023/02/11 08:00:00 InorOut=1 attype=0 Device=DeviceX SN=Serial999"
                        # Parse key=value
                        self.parse_and_insert_attlog(line)

        except Exception as e:
            logger.error(f"❌ Error in handle_client: {e}")
        finally:
            client_socket.close()
            logger.info(f"🔌 Connection closed: {addr}")

    def parse_and_insert_attlog(self, line):
        """
        Example line:
          ATTLOG  ZKID=123  Timestamp=2023-02-11 08:00:00  InorOut=1  attype=0 ...
        Adjust to match your actual format from the device.
        """
        # Remove the initial "ATTLOG"
        parts = line[len("ATTLOG"):].strip().split()
        # parts might be: ["ZKID=123", "Timestamp=2023-02-11", "08:00:00", "InorOut=1", ...]

        data_map = {}
        for p in parts:
            if '=' in p:
                k, v = p.split('=', 1)
                # If the timestamp is split across multiple array elements, you might need a custom parser
                data_map[k.strip()] = v.strip()
            else:
                # Potentially a leftover piece of the timestamp
                if "Timestamp" in data_map and ":" in p:
                    # Append time to existing date
                    data_map["Timestamp"] = data_map["Timestamp"] + " " + p

        # Insert into DB
        self.insert_record_into_db(data_map)

    def insert_record_into_db(self, data_map):
        """
        Insert record into 'attendance' table. 
        Ensure your columns match what's in Dbcon.py
        """
        # Convert timestamp to a uniform format if needed
        timestamp_raw = data_map.get("Timestamp", "")
        timestamp_fmt = self.normalize_timestamp(timestamp_raw)

        # Build the row
        row = {
            "ZKID": data_map.get("ZKID", ""),
            "Timestamp": timestamp_fmt,
            "InorOut": data_map.get("InorOut", None),
            "attype": data_map.get("attype", None),
            "Device": data_map.get("Device", ""),
            "SN": data_map.get("SN", ""),
            "Devrec": data_map.get("Devrec", "")
        }

        try:
            with sqlite3.connect(DB_NAME) as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["ZKID"],
                    row["Timestamp"],
                    row["InorOut"],
                    row["attype"],
                    row["Device"],
                    row["SN"],
                    row["Devrec"]
                ))
                conn.commit()

            logger.info(f"✅ Inserted ATTLOG record: {row}")

        except sqlite3.IntegrityError:
            logger.warning(f"⚠ Duplicate record ignored: {row}")
        except Exception as e:
            logger.error(f"❌ DB Error: {e}")

    def normalize_timestamp(self, ts_str):
        """
        Convert timestamp to 'YYYY-MM-DD HH:MM:SS' if possible.
        Accepts 'YYYY/MM/DD HH:MM:SS' or 'YYYY-MM-DD HH:MM:SS'.
        """
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts_str, fmt)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        return ts_str  # fallback

    def start_server(self):
        """Listen on all configured ports. This is called within a while True in Main.py."""
        if not self.ports:
            logger.error("❌ No ports configured in Settings.json!")
            return

        threads = []
        for port in self.ports:
            t = threading.Thread(target=self.listen_for_connections, args=(port,), daemon=True)
            t.start()
            threads.append(t)

        # Keep them alive
        for t in threads:
            t.join()
