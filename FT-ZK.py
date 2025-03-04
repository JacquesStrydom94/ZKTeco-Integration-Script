#!/usr/bin/env python3
import os
import sys
import time
import threading
import socket
import datetime
import json
import re
import logging
from queue import Queue
from urllib.parse import urlparse, parse_qs
import requests
import sqlite3

# --- Load Settings ---
SETTINGS_FILE = "settings.json"
if not os.path.exists(SETTINGS_FILE):
    print(f"{SETTINGS_FILE} not found. Exiting.")
    sys.exit(1)
with open(SETTINGS_FILE, 'r') as f:
    settings = json.load(f)

DBID = settings.get("DBID")
Token = settings.get("Token")
devices = settings.get("devices", [])
if not DBID or not Token or not devices:
    print("DBID, Token and devices must be provided in settings.json. Exiting.")
    sys.exit(1)

# --- Global Configuration ---
ATTLOG_FILE = "attlog.json"
DB_FILE = "PUSH.db"
POST_API_URL = f'https://appnostic.dbflex.net/secure/api/v2/{DBID}/{Token}/ZK_stage/create.json'
# How long each command server cycle lasts (in seconds)
RUN_INTERVAL = 43200

# --- Logging Setup (with ANSI colors) ---
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
        elif "Received from" in record.msg:
            log_fmt = self.YELLOW + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Writing to file" in record.msg or "Parsed JSON packet" in record.msg:
            log_fmt = self.BLUE + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Closing connection" in record.msg:
            log_fmt = self.RED + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        elif "Writing new entries to" in record.msg:
            log_fmt = self.PINK + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET
        else:
            log_fmt = self.FORMATS.get(record.levelno, self.FORMATS['DEFAULT'])
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logging.basicConfig(level=logging.DEBUG, handlers=[handler])

# --- Global Variables & Locks for Command Server ---
global_counter = 1000
counter_lock = threading.Lock()
port_query_lock = threading.Lock()
port_query_sent = {}  # key: port, value: bool
shutdown_event = threading.Event()

# --- Helper Functions (Command Server Part) ---
def get_timestamp():
    now = datetime.datetime.now()
    ms = int(now.microsecond / 1000)
    return now.strftime(f"%Y-%m-%d %H:%M:%S:{ms:03d}")

def get_date_header():
    return datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

def extract_attlog(data):
    cl_index = data.find("Content-Length:")
    if cl_index == -1:
        return None
    cl_start = cl_index + len("Content-Length:")
    cl_end = data.find("\n", cl_start)
    try:
        content_length = int(data[cl_start:cl_end].strip())
    except ValueError:
        return None
    data_start = data.find("\n", cl_end) + 1
    attlog_data = data[data_start:data_start+content_length].strip()
    return attlog_data

def extract_sn(data_str):
    m = re.search(r'SN=([^&\s]+)', data_str)
    return m.group(1) if m else None

def split_attlog_records(record_str):
    return [line.strip() for line in record_str.strip().splitlines() if line.strip()]

def parse_log_entry(entry):
    tokens = entry.split()
    if len(tokens) < 5:
        return None
    record = {
        "ZKID": tokens[0],
        "timestamp": tokens[1] + " " + tokens[2],
        "inorout": tokens[3],
        "attype": tokens[4]
    }
    for i, token in enumerate(tokens[5:], start=1):
        record[f"col{i}"] = token
    return record

def write_to_file(queue, filename):
    while True:
        json_packet = queue.get()
        if json_packet is None:
            break
        raw_attlog = json_packet.get("attlog", "")
        record_list = split_attlog_records(raw_attlog)
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                try:
                    data = json.load(f)
                    if not isinstance(data, list):
                        data = []
                except json.JSONDecodeError:
                    data = []
        else:
            data = []
        sn_value = json_packet.get("sn", "")
        for entry in record_list:
            record_dict = parse_log_entry(entry)
            if record_dict is None:
                continue
            record_dict["SN"] = sn_value
            record_dict["log_timestamp"] = get_timestamp()
            if record_dict not in data:
                data.append(record_dict)
                logging.debug(f"New record added: {record_dict}")
            else:
                logging.debug(f"Duplicate record skipped: {record_dict}")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        queue.task_done()

def handle_client(client_socket, client_address, server_port, queue):
    global global_counter
    try:
        data = client_socket.recv(10240).decode(errors='ignore')
        if not data:
            client_socket.close()
            return
        logging.info(f"Received from {client_address} on port {server_port}:\n{data}")
        request_line = data.splitlines()[0]
        parts = request_line.split()
        if len(parts) < 2:
            client_socket.close()
            return
        method, url = parts[0], parts[1]
        parsed_url = urlparse(url)
        path = parsed_url.path
        qs = parse_qs(parsed_url.query)
        logging.debug(f"DEBUG (port {server_port}): Query parameters from {client_address}: {qs}")
        # GET /iclock/getrequest – respond with dt1 as yesterday and dt2 as today in GMT+2
        if method.upper() == "GET" and path == "/iclock/getrequest":
            from datetime import timedelta, timezone
            tz = timezone(timedelta(hours=2))
            now = datetime.datetime.now(tz)
            dt2 = now.strftime("%Y-%m-%d")
            dt1 = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            if "INFO" in qs:
                body = "OK"
            else:
                with port_query_lock:
                    sent = port_query_sent.get(server_port, False)
                    if not sent:
                        port_query_sent[server_port] = True
                        with counter_lock:
                            current_value = global_counter
                            global_counter += 1
                        body = f"C:{current_value}:DATA QUERY ATTLOG StartTime={dt1}\tEndTime={dt2}"
                    else:
                        body = "OK"
            body_bytes = body.encode()
            header = (f"HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/plain\r\n"
                      "Accept-Ranges: bytes\r\n"
                      f"Date: {get_date_header()}\r\n"
                      f"Content-Length: {len(body_bytes)}\r\n"
                      "\r\n")
            client_socket.sendall(header.encode())
            client_socket.sendall(body_bytes)
            client_socket.close()
            return
        # POST /iclock/cdata?table=ATTLOG – add attlog data to queue
        if method.upper() == "POST" and path == "/iclock/cdata" and qs.get("table", [""])[0].upper() == "ATTLOG":
            body = "OK"
            attlog_data = extract_attlog(data)
            sn_value = extract_sn(data)
            if attlog_data and sn_value:
                json_packet = {"attlog": attlog_data, "client": client_address, "sn": sn_value}
                logging.info(f"Parsed JSON packet: {json.dumps(json_packet, indent=2)}")
                logging.debug(f"Adding packet to queue: {json_packet}")
                queue.put(json_packet)
            body_bytes = body.encode()
            header = (f"HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/plain\r\n"
                      "Accept-Ranges: bytes\r\n"
                      f"Date: {get_date_header()}\r\n"
                      f"Content-Length: {len(body_bytes)}\r\n"
                      "\r\n")
            client_socket.sendall(header.encode())
            client_socket.sendall(body_bytes)
            client_socket.close()
            return
        # GET /iclock/cdata?options=all – return a fixed command string
        if method.upper() == "GET" and path == "/iclock/cdata" and qs.get("options", [""])[0] == "all":
            SN = qs.get("SN", [""])[0]
            command_body = (
                f"GET OPTION FROM:{SN}\n"
                "Stamp=9999\n"
                "OpStamp=9999\n"
                "PhotoStamp=0\n"
                "TransFlag=TransData AttLog\tOpLog\tAttPhoto\tEnrollUser\tChgUser\tEnrollFP\tChgFP\tFPImag\tFACE\tUserPic\tWORKCODE\tBioPhoto\n"
                "ErrorDelay=120\n"
                "Delay=10\n"
                "TimeZone=120\n"
                "TransTimes=\n"
                "TransInterval=30\n"
                "SyncTime=0\n"
                "Realtime=1\n"
                "ServerVer=2.2.14 2025/02/19\n"
                "PushProtVer=2.4.1\n"
                "PushOptionsFlag=1\n"
                "ATTLOGStamp=9999\n"
                "OPERLOGStamp=9999\n"
                "ATTPHOTOStamp=0\n"
                "ServerName=Logtime Server\n"
                "MultiBioDataSupport=0:1:0:0:0:0:0:0:0:"
            )
            body_bytes = command_body.encode()
            header = (f"HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/plain\r\n"
                      "Accept-Ranges: bytes\r\n"
                      f"Date: {get_date_header()}\r\n"
                      f"Content-Length: {len(body_bytes)}\r\n"
                      "\r\n")
            client_socket.sendall(header.encode())
            client_socket.sendall(body_bytes)
            client_socket.close()
            return
        # Default response:
        body = "OK"
        body_bytes = body.encode()
        header = (f"HTTP/1.1 200 OK\r\n"
                  "Content-Type: text/plain\r\n"
                  "Accept-Ranges: bytes\r\n"
                  f"Date: {get_date_header()}\r\n"
                  f"Content-Length: {len(body_bytes)}\r\n"
                  "\r\n")
        client_socket.sendall(header.encode())
        client_socket.sendall(body_bytes)
        client_socket.close()
    except Exception as e:
        logging.error(f"Error handling client {client_address} on port {server_port}: {e}")
        client_socket.close()

def start_server_on_port(host, port, queue):
    with port_query_lock:
        port_query_sent[port] = False
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    server.settimeout(0.5)
    logging.info(f"Server listening on {host}:{port}")
    while not shutdown_event.is_set():
        try:
            client_socket, client_address = server.accept()
            threading.Thread(target=handle_client, args=(client_socket, client_address, port, queue), daemon=True).start()
        except socket.timeout:
            continue
    server.close()
    logging.info(f"Server on port {port} shutting down.")

def run_command_server(host, devices, queue, run_interval):
    threads = []
    for device in devices:
        port = device.get('port')
        with port_query_lock:
            port_query_sent[port] = False
        t = threading.Thread(target=start_server_on_port, args=(host, port, queue), daemon=True)
        t.start()
        threads.append(t)
        logging.info(f"Started server on {host}:{port}")
    writer_thread = threading.Thread(target=write_to_file, args=(queue, ATTLOG_FILE), daemon=True)
    writer_thread.start()
    logging.info("File writer thread started.")
    logging.info(f"Command server running for {run_interval} seconds...")
    time.sleep(run_interval)
    shutdown_event.set()
    logging.info("Shutdown event set. Waiting for server threads to finish...")
    for t in threads:
        t.join(timeout=5)
    writer_thread.join(timeout=5)
    logging.info("Command server cycle stopped.")

# --- Sync Process Functions ---
def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # WARNING: This drops the existing table – adjust as needed.
    cursor.execute('DROP TABLE IF EXISTS attendance')
    cursor.execute('''
        CREATE TABLE attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ZKID TEXT,
            Timestamp TEXT,
            InorOut TEXT,
            attype TEXT,
            col1 TEXT,
            col2 TEXT,
            col3 TEXT,
            col4 TEXT,
            col5 TEXT,
            col6 TEXT,
            col7 TEXT,
            SN TEXT,
            log_timestamp TEXT,
            FTID TEXT,
            KEY TEXT,
            RESPONSE TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized.")

def record_exists(cursor, zkid, timestamp_val):
    cursor.execute('SELECT 1 FROM attendance WHERE ZKID = ? AND Timestamp = ?', (zkid, timestamp_val))
    return cursor.fetchone() is not None

def process_attlog_file():
    try:
        with open(ATTLOG_FILE, 'r') as file:
            content = json.load(file)
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    for record in content:
        zkid = record.get("ZKID")
        timestamp_val = record.get("timestamp")
        if zkid is None or timestamp_val is None:
            print("Skipping record due to missing ZKID or timestamp:", record)
            continue
        if not record_exists(cursor, zkid, timestamp_val):
            cursor.execute('''
                INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, col1, col2, col3, col4, col5, col6, col7, SN, log_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record.get("ZKID"),
                record.get("timestamp"),
                record.get("inorout"),
                record.get("attype"),
                record.get("col1", ""),
                record.get("col2", ""),
                record.get("col3", ""),
                record.get("col4", ""),
                record.get("col5", ""),
                record.get("col6", ""),
                record.get("col7", ""),
                record.get("SN", ""),
                record.get("log_timestamp", "")
            ))
        else:
            print("Duplicate record skipped (by file):", record)
    conn.commit()
    conn.close()
    print("Finished processing attlog file.")

def log_posting_json_sql(record_id, zk_id, in_or_out, attype, sn, timestamp_val, response_status, response_text):
    log_entry = {
        "Posting JSON SQL ID": {
            "ZKID": zk_id,
            "Timestamp": timestamp_val,
            "InorOut": in_or_out,
            "attype": attype,
            "SN": sn
        },
        "HTTP Status Code": response_status,
        "Response Text": response_text,
        "Message": f"Successfully updated record with id {record_id}",
        "Logged At": datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    }
    print(json.dumps(log_entry, indent=2))

def post_records():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM attendance")
    records = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    conn.close()
    for record in records:
        response_val = record[column_names.index('RESPONSE')]
        if response_val not in (None, ""):
            print(f"Skipping record with id {record[column_names.index('id')]}: RESPONSE is set to {response_val}")
            continue
        exclude = {"id", "FTID", "RESPONSE", "KEY", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "log_timestamp"}
        record_dict = {column: value for column, value in zip(column_names, record) if column not in exclude}
        for key, value in record_dict.items():
            if isinstance(value, str) and '-' in value and ':' in value:
                try:
                    dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    record_dict[key] = dt.strftime("%Y/%m/%d %H:%M:%S")
                except ValueError:
                    pass
        record_json = json.dumps(record_dict)
        record_id = record[column_names.index('id')]
        print(f"Posting JSON SQL ID {record_id}: {record_json}")
        try:
            response = requests.post(
                POST_API_URL,
                data=record_json,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {Token}'
                }
            )
            print(f"HTTP Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            if response.status_code == 200:
                response_data = response.json()[0]
                if 'key' in response_data:
                    try:
                        conn = sqlite3.connect(DB_FILE)
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE attendance 
                            SET RESPONSE = ?, KEY = ?, FTID = ? 
                            WHERE id = ?
                        """, (response_data['status'], response_data['key'], response_data['id'], record_id))
                        conn.commit()
                        conn.close()
                        print(f"Successfully updated record with id {record_id}: RESPONSE={response_data['status']}, KEY={response_data['key']}, FTID={response_data['id']}")
                        log_posting_json_sql(
                            record_id,
                            record_dict.get("ZKID", ""),
                            record_dict.get("InorOut", ""),
                            record_dict.get("attype", ""),
                            record_dict.get("SN", ""),
                            record_dict.get("Timestamp", ""),
                            response.status_code,
                            response.text
                        )
                    except sqlite3.Error as e:
                        print(f"Failed to update record with id {record_id}: {e}")
                    finally:
                        conn.close()
                else:
                    print("API returned error data:", response_data)
            else:
                print(f"Non-200 HTTP response: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print("Request exception:", e)

def sync_loop():
    while True:
        print("Processing attlog file...")
        process_attlog_file()
        print("Posting records from the database...")
        post_records()
        time.sleep(10)

# --- Main Entry Point ---
def main():
    host = "0.0.0.0"
    # Ensure attlog.json exists
    if not os.path.exists(ATTLOG_FILE):
        with open(ATTLOG_FILE, 'w') as f:
            json.dump([], f)
    q = Queue()
    # Start the command server in its own thread
    command_thread = threading.Thread(target=lambda: run_command_server(host, devices, q, RUN_INTERVAL), daemon=True)
    command_thread.start()
    logging.info("Command server thread started.")
    # Initialize the database (drops and recreates the attendance table)
    initialize_database()
    # Start the sync process in its own thread
    sync_thread = threading.Thread(target=sync_loop, daemon=True)
    sync_thread.start()
    logging.info("Sync thread started.")
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Script stopped by user.")

if __name__ == "__main__":
    main()
