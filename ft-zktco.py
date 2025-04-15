#!/usr/bin/env python3
import os, sys, time, json, re, socket, sqlite3, threading, requests, datetime
from queue import Queue
from urllib.parse import urlparse, parse_qs
from dateutil.relativedelta import relativedelta
import logging

##########################################
# Load settings from settings.json
##########################################
SETTINGS_FILE = "settings.json"
if not os.path.exists(SETTINGS_FILE):
    raise FileNotFoundError(f"Settings file '{SETTINGS_FILE}' not found.")
with open(SETTINGS_FILE, 'r') as f:
    settings = json.load(f)
DBID    = settings.get("DBID")
Token   = settings.get("Token")
devices = settings.get("devices", [])
if not (DBID and Token and devices):
    raise ValueError("DBID, Token, and devices must be provided in settings.json")
# API endpoints
DEVICE_URL  = f'https://appnostic.dbflex.net/secure/api/v2/{DBID}/{Token}/ZK%20Device/select.json'
STAFF_URL   = f'https://appnostic.dbflex.net/secure/api/v2/{DBID}/{Token}/Staff/ZK_DATA/select.json'
POST_API_URL = f'https://appnostic.dbflex.net/secure/api/v2/{DBID}/{Token}/ZK_stage/create.json'
# Files & DB
DB_FILE = "PUSH.db"
ATTLOG_FILE = "attlog.json"
# Run interval in seconds
RUN_INTERVAL = 43200

##########################################
# Globals for TCP server
##########################################
global_counter = 1000
counter_lock   = threading.Lock()
port_query_lock = threading.Lock()
port_query_sent = {}  # {port: bool}
shutdown_event  = threading.Event()

##########################################
# Logging Setup (ANSI colors)
##########################################
class CustomFormatter(logging.Formatter):
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    RED    = "\033[91m"
    PINK   = "\033[95m"
    RESET  = "\033[0m"
    FORMATS = { logging.INFO: "%(asctime)s - %(levelname)s - %(message)s",
                'DEFAULT': "%(asctime)s - %(levelname)s - %(message)s" }
    def format(self, record):
        msg = record.msg
        log_fmt = (self.GREEN if "Connected by" in msg or "Server listening" in msg else
                   self.YELLOW if "Received from" in msg else
                   self.BLUE if "Writing to file" in msg or "Parsed JSON packet" in msg else
                   self.RED if "Closing connection" in msg else
                   self.PINK if "New record added" in msg else
                   self.FORMATS.get(record.levelno, self.FORMATS['DEFAULT']))
        return logging.Formatter(log_fmt + "%(asctime)s - %(levelname)s - %(message)s" + self.RESET).format(record)

handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logging.basicConfig(level=logging.DEBUG, handlers=[handler])

##########################################
# INITIALIZATION FUNCTIONS
##########################################
def ensure_attlog_file():
    os.path.exists(ATTLOG_FILE) or (open(ATTLOG_FILE, 'w').write(json.dumps([])) and print(f"Created empty {ATTLOG_FILE}"))
    print(f"{ATTLOG_FILE} exists.") if os.path.exists(ATTLOG_FILE) else None

def create_attendance_table():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance (
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
    print("Attendance table ensured.")

def refresh_devices_table():
    logging.info("Refreshing DEVICES table...")
    try:
        data = requests.get(DEVICE_URL).json()
    except Exception as e:
        logging.error("Error fetching devices: " + str(e))
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS DEVICES (id INTEGER PRIMARY KEY, remote_id INTEGER)')
    cursor.execute('DELETE FROM DEVICES')
    sanitize = lambda name: re.sub(r'\W|^(?=\d)', '_', name)
    for key in data[0].keys():
        if key != 'Id':
            try:
                cursor.execute(f'ALTER TABLE DEVICES ADD COLUMN {sanitize(key)} TEXT')
            except sqlite3.OperationalError:
                pass
    for item in data:
        sanitized_item = {sanitize(k): v for k, v in item.items() if k != 'Id'}
        sanitized_item['remote_id'] = item.get('Id')
        cols = ', '.join(sanitized_item.keys())
        placeholders = ', '.join('?' for _ in sanitized_item)
        cursor.execute(f'INSERT INTO DEVICES ({cols}) VALUES ({placeholders})', list(sanitized_item.values()))
    conn.commit()
    conn.close()
    logging.info("DEVICES table refreshed.")

def refresh_staff_table():
    logging.info("Refreshing STAFF table...")
    try:
        data = requests.get(STAFF_URL).json()
    except Exception as e:
        logging.error("Error fetching staff: " + str(e))
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Create STAFF table with only needed columns.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS STAFF (
            id INTEGER PRIMARY KEY,
            remote_id INTEGER,
            Employee_Name TEXT,
            Staff_Id TEXT,
            Access_Control TEXT
        )
    ''')
    cursor.execute('DELETE FROM STAFF')
    for item in data:
        remote_id      = item.get("Id")
        emp_name       = item.get("Employee_Name", "")
        staff_id       = item.get("Staff_Id", "")
        access_control = item.get("Access_Control", "")
        cursor.execute('''
            INSERT INTO STAFF (remote_id, Employee_Name, Staff_Id, Access_Control)
            VALUES (?, ?, ?, ?)
        ''', (remote_id, emp_name, staff_id, access_control))
    conn.commit()
    conn.close()
    logging.info("STAFF table refreshed.")

def initialize_db_and_files():
    ensure_attlog_file()
    create_attendance_table()
    refresh_devices_table()
    refresh_staff_table()
    logging.info("Database initialization (refresh) complete.")

##########################################
# CLEANING FUNCTION: Remove attlog.json records older than one month ago
##########################################
def clean_attlog_file():
    try:
        if not os.path.exists(ATTLOG_FILE):
            return
        with open(ATTLOG_FILE, 'r') as f:
            try:
                records = json.load(f)
                records = records if isinstance(records, list) else []
            except json.JSONDecodeError:
                records = []
        now = datetime.datetime.now()
        # Use relativedelta to get same day last month
        threshold = now - relativedelta(months=1)
        new_records = []
        for rec in records:
            ts = rec.get("log_timestamp")
            dt = (datetime.datetime.strptime(ts.rsplit(":", 1)[0], "%Y-%m-%d %H:%M:%S").replace(microsecond=int(ts.rsplit(":", 1)[1])*1000)
                  if ts and len(ts.rsplit(":", 1))==2 else None)
            new_records.append(rec) if dt and dt >= threshold else logging.debug(f"Removed record with log_timestamp {ts}")
        with open(ATTLOG_FILE, 'w') as f:
            json.dump(new_records, f, indent=2)
        logging.info(f"Cleaned attlog.json; kept {len(new_records)} records.")
    except Exception as e:
        logging.error("Error cleaning attlog.json: " + str(e))

##########################################
# TCP SERVER FUNCTIONS
##########################################
def get_timestamp():
    now = datetime.datetime.now()
    ms = int(now.microsecond / 1000)
    return now.strftime(f"%Y-%m-%d %H:%M:%S:{ms:03d}")

def get_date_header():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    return now_utc.strftime("%a, %d %b %Y %H:%M:%S GMT")

def extract_attlog(data):
    cl_index = data.find("Content-Length:")
    if cl_index == -1: return None
    cl_start = cl_index + len("Content-Length:")
    cl_end = data.find("\n", cl_start)
    try:
        content_length = int(data[cl_start:cl_end].strip())
    except ValueError:
        return None
    data_start = data.find("\n", cl_end) + 1
    return data[data_start:data_start+content_length].strip()

def extract_sn(data_str):
    m = re.search(r'SN=([^&\s]+)', data_str)
    return m.group(1) if m else None

def split_attlog_records(record_str):
    return [line.strip() for line in record_str.strip().splitlines() if line.strip()]

def parse_log_entry(entry):
    tokens = entry.split()
    return None if len(tokens) < 5 else {
        "ZKID": tokens[0],
        "timestamp": tokens[1] + " " + tokens[2],
        "inorout": tokens[3],
        "attype": tokens[4],
        **({f"col{i}": token for i, token in enumerate(tokens[5:], start=1)})
    }

def write_to_file(q, filename):
    while True:
        json_packet = q.get()
        if json_packet is None:
            break
        raw_attlog = json_packet.get("attlog", "")
        record_list = split_attlog_records(raw_attlog)
        data = []
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                try:
                    data = json.load(f)
                    data = data if isinstance(data, list) else []
                except json.JSONDecodeError:
                    data = []
        sn_value = json_packet.get("sn", "")
        for entry in record_list:
            record_dict = parse_log_entry(entry)
            if record_dict is None: continue
            record_dict["SN"] = sn_value
            record_dict["log_timestamp"] = get_timestamp()
            data.append(record_dict) if record_dict not in data else logging.debug(f"Duplicate record skipped: {record_dict}")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        q.task_done()

def handle_client(client_socket, client_address, server_port, q):
    global global_counter
    try:
        data = client_socket.recv(10240).decode(errors='ignore')
        if not data:
            client_socket.close()
            return
        logging.info(f"Received from {client_address} on port {server_port}:\n{data}")
        parts = data.splitlines()[0].split()
        if len(parts) < 2:
            client_socket.close()
            return
        method, url = parts[0], parts[1]
        parsed_url = urlparse(url)
        path = parsed_url.path
        qs = parse_qs(parsed_url.query)
        logging.debug(f"DEBUG (port {server_port}): Query parameters from {client_address}: {qs}")
        # GET /iclock/getrequest
        if method.upper() == "GET" and path == "/iclock/getrequest":
            tz = datetime.timezone(datetime.timedelta(hours=2))
            now = datetime.datetime.now(tz)
            dt2 = now.strftime("%Y-%m-%d")  # Today’s date
            dt1 = (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")  # Yesterday’s date
            body = "OK" if "INFO" in qs else None
            if body is None:
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
            header = (f"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nAccept-Ranges: bytes\r\n"
                      f"Date: {get_date_header()}\r\nContent-Length: {len(body_bytes)}\r\n\r\n")
            client_socket.sendall(header.encode())
            client_socket.sendall(body_bytes)
            client_socket.close()
            return
        # POST /iclock/cdata?table=ATTLOG
        if method.upper() == "POST" and path == "/iclock/cdata" and qs.get("table", [""])[0].upper() == "ATTLOG":
            body = "OK"
            attlog_data = extract_attlog(data)
            sn_value = extract_sn(data)
            if attlog_data and sn_value:
                json_packet = {"attlog": attlog_data, "client": client_address, "sn": sn_value}
                logging.info(f"Parsed JSON packet: {json.dumps(json_packet, indent=2)}")
                logging.debug(f"Adding packet to queue: {json_packet}")
                q.put(json_packet)
            body_bytes = body.encode()
            header = (f"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nAccept-Ranges: bytes\r\n"
                      f"Date: {get_date_header()}\r\nContent-Length: {len(body_bytes)}\r\n\r\n")
            client_socket.sendall(header.encode())
            client_socket.sendall(body_bytes)
            client_socket.close()
            return
        # GET /iclock/cdata?options=all
        if method.upper() == "GET" and path == "/iclock/cdata" and qs.get("options", [""])[0] == "all":
            SN = qs.get("SN", [""])[0]
            command_body = (
                f"GET OPTION FROM:{SN}\nStamp=9999\nOpStamp=9999\nPhotoStamp=0\n"
                "TransFlag=TransData AttLog\tOpLog\tAttPhoto\tEnrollUser\tChgUser\tEnrollFP\tChgFP\tFPImag\tFACE\tUserPic\tWORKCODE\tBioPhoto\n"
                "ErrorDelay=120\nDelay=10\nTimeZone=120\nTransTimes=\nTransInterval=30\nSyncTime=0\nRealtime=1\n"
                "ServerVer=2.2.14 2025/02/19\nPushProtVer=2.4.1\nPushOptionsFlag=1\nATTLOGStamp=9999\n"
                "OPERLOGStamp=9999\nATTPHOTOStamp=0\nServerName=Logtime Server\nMultiBioDataSupport=0:1:0:0:0:0:0:0:0:"
            )
            body_bytes = command_body.encode()
            header = (f"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nAccept-Ranges: bytes\r\n"
                      f"Date: {get_date_header()}\r\nContent-Length: {len(body_bytes)}\r\n\r\n")
            client_socket.sendall(header.encode())
            client_socket.sendall(body_bytes)
            client_socket.close()
            return
        # Default response:
        body = "OK"
        body_bytes = body.encode()
        header = (f"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nAccept-Ranges: bytes\r\n"
                  f"Date: {get_date_header()}\r\nContent-Length: {len(body_bytes)}\r\n\r\n")
        client_socket.sendall(header.encode())
        client_socket.sendall(body_bytes)
        client_socket.close()
    except Exception as e:
        logging.error(f"Error handling client {client_address} on port {server_port}: {e}")
        client_socket.close()

def start_server_on_port(host, port, q):
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
            threading.Thread(target=handle_client, args=(client_socket, client_address, port, q), daemon=True).start()
        except socket.timeout:
            continue
    server.close()
    logging.info(f"Server on port {port} shutting down.")

def run_server(host, devices, q, run_interval):
    threads = []
    for device in devices:
        port = device['port']
        with port_query_lock:
            port_query_sent[port] = False
        t = threading.Thread(target=start_server_on_port, args=(host, port, q), daemon=True)
        t.start()
        threads.append(t)
        logging.info(f"Started server on {host}:{port}")
    writer_thread = threading.Thread(target=write_to_file, args=(q, ATTLOG_FILE), daemon=True)
    writer_thread.start()
    logging.info("File writer thread started.")
    logging.info(f"Server running for {run_interval} seconds...")
    time.sleep(run_interval)
    shutdown_event.set()
    logging.info("Shutdown event set. Waiting for server threads to finish...")
    for t in threads:
        t.join(timeout=5)
    writer_thread.join(timeout=5)
    logging.info("Server stopped for this cycle.")

##########################################
# SYNC FUNCTIONS: Process attlog.json and post records to API
##########################################
def record_exists(cursor, zkid, timestamp_val):
    cursor.execute('SELECT 1 FROM attendance WHERE ZKID = ? AND Timestamp = ?', (zkid, timestamp_val))
    return True if cursor.fetchone() is not None else False

def process_attlog_file():
    try:
        content = json.load(open(ATTLOG_FILE, 'r'))
    except json.JSONDecodeError as e:
        print("Error decoding JSON from attlog file:", e)
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    for record in content:
        zkid = record.get("ZKID")
        timestamp_val = record.get("timestamp")
        print("Skipping record (missing ZKID or timestamp):", record) if (zkid is None or timestamp_val is None) else None
        if zkid is None or timestamp_val is None:
            continue
        (cursor.execute('''
                INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, col1, col2, col3, col4, col5, col6, col7, SN, log_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record.get("ZKID"),
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
                 )) if not record_exists(cursor, zkid, timestamp_val)
         else print("Duplicate record skipped (from file):", record))
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
            print(f"Skipping record id {record[column_names.index('id')]}: RESPONSE is set")
            continue
        exclude = {"id", "FTID", "RESPONSE", "KEY", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "log_timestamp"}
        record_dict = {col: val for col, val in zip(column_names, record) if col not in exclude}
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
                        print(f"Successfully updated record id {record_id}")
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
                        print(f"Failed to update record id {record_id}: {e}")
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
        clean_attlog_file()
        print("Processing attlog file...")
        process_attlog_file()
        print("Posting records from the database...")
        post_records()
        time.sleep(10)

##########################################
# MAIN INITIALIZATION & LOOP
##########################################
def main():
    host = "0.0.0.0"
    # INITIALIZATION: refresh attlog file, create attendance table, and update DEVICES and STAFF.
    initialize_db_and_files()
    # Create a Queue for incoming attlog packets.
    q = Queue()
    # Start the TCP server cycle in a separate thread.
    def server_cycle():
        while True:
            refresh_devices_table()
            refresh_staff_table()
            shutdown_event.clear()
            with port_query_lock:
                [port_query_sent.update({device['port']: False}) for device in devices]
            run_server(host, devices, q, run_interval=RUN_INTERVAL)
            logging.info("Restarting the server cycle...")
    threading.Thread(target=server_cycle, daemon=True).start()
    # Start the sync loop in another thread.
    threading.Thread(target=sync_loop, daemon=True).start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Script stopped by user.")
        q.put(None)

if __name__ == "__main__":
    main()
