import socket
import threading
import json
import os
import logging
from datetime import datetime, timezone

# -------------------------------
# LOGGING SETUP
# -------------------------------
class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S:%f')

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.FileHandler("server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()
formatter = CustomFormatter()
for handler in logger.handlers:
    handler.setFormatter(formatter)

# -------------------------------
# GLOBALS
# -------------------------------
cmd_count_file = "cmd_count.json"
settings_file = "Settings.json"

# We'll store the command template in memory
cmd_template = "DATA QUERY ATTLOG StartTime=startTime EndTime=endTime"

# -------------------------------
# LOAD / SAVE CMD_COUNT
# -------------------------------
def load_cmd_count():
    """Loads the cmd_count from cmd_count.json or initializes it to 0."""
    if os.path.exists(cmd_count_file):
        with open(cmd_count_file, "r") as file:
            data = json.load(file)
            return data.get("cmd_count", 0)
    else:
        with open(cmd_count_file, "w") as file:
            json.dump({"cmd_count": 0}, file)
        return 0

def save_cmd_count(count):
    """Saves the cmd_count to cmd_count.json."""
    with open(cmd_count_file, "w") as file:
        json.dump({"cmd_count": count}, file)

cmd_count = load_cmd_count()

# -------------------------------
# READ SETTINGS.JSON
# -------------------------------
def load_settings():
    """
    Expects something like:
    {
      "settings": {
        "cmd": "DATA QUERY ATTLOG StartTime=startTime EndTime=endTime"
      },
      "devices": [
        { "ip": "127.0.0.1", "port": 5001 },
        ...
      ],
      "logs": [...],
      "System para": [...]
    }
    """
    global cmd_template

    if not os.path.exists(settings_file):
        logger.error(f"⚠️ Settings file '{settings_file}' not found. Exiting...")
        os._exit(1)

    try:
        with open(settings_file, "r") as f:
            data = json.load(f)

            # 1) Extract the command template if present
            config_settings = data.get("settings", {})
            if "cmd" in config_settings:
                cmd_template = config_settings["cmd"]
                logger.info(f"Using CMD template from settings: {cmd_template}")

            # 2) Gather all (ip, port) pairs
            devices = []
            for dev in data.get("devices", []):
                ip = dev.get("ip")
                port = dev.get("port")
                if ip and port:
                    devices.append((ip, port))

            return devices
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"❌ Error reading {settings_file}: {e}")
        os._exit(1)

# -------------------------------
# HTTP RESPONSE UTILITY
# -------------------------------
def send_http_response(client_socket, response_text, content_length):
    """
    Sends a minimal HTTP/1.1 200 response with the specified content length,
    plus the actual message (response_text).
    """
    date_now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    http_response = (
        f"HTTP/1.1 200 OK\n"
        f"Content-Type: text/plain\n"
        f"Accept-Ranges: bytes\n"
        f"Date: {date_now}\n"
        f"Content-Length: {content_length}\n\n"
    )
    client_socket.sendall(http_response.encode('utf-8'))
    logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\n{http_response}")

# -------------------------------
# HANDLE CLIENT CONNECTION
# -------------------------------
def handle_client(client_socket):
    global cmd_count, cmd_template
    try:
        response = client_socket.recv(4096).decode('utf-8', errors='ignore')
        if response:
            # Filter out ATTPHOTO to reduce log clutter
            log_response = response.replace("ATTPHOTO", "[FILTERED]")
            logger.info(f"Sever Receive Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\n{log_response}")

            # ---------------------------------------------
            # 1) GET /iclock/cdata?SN=... &options=all...
            # ---------------------------------------------
            if (
                "GET /iclock/cdata?SN=" in response and 
                "&options=all&language=69&pushver=2.4.1&DeviceType=att&PushOptionsFlag=1" in response
            ):
                sn = response.split("SN=")[1].split("&")[0]
                # a) Send HTTP 200 "OK"
                send_http_response(client_socket, "OK", 450)
                # b) Then send device options
                option_response = (
                    f"GET OPTION FROM:{sn}\n"
                    "Stamp=9999\n"
                    "OpStamp=9999\n"
                    "PhotoStamp=0\n"
                    "TransFlag=TransData AttLog\tOpLog\t[ATTPHOTO REMOVED]\tEnrollUser\tChgUser\tEnrollFP\tChgFP\tFPImag\tFACE\tUserPic\tWORKCODE\tBioPhoto\n"
                    "ErrorDelay=120\n"
                    "Delay=10\n"
                    "TimeZone=120\n"
                    "TransTimes=\n"
                    "TransInterval=30\n"
                    "SyncTime=0\n"
                    "Realtime=1\n"
                    "ServerVer=2.2.14 2025/02/12\n"
                    "PushProtVer=2.4.1\n"
                    "PushOptionsFlag=1\n"
                    "ATTLOGStamp=9999\n"
                    "OPERLOGStamp=9999\n"
                    "ServerName=Logtime Server\n"
                    "MultiBioDataSupport=0:1:0:0:0:0:0:0:0:0\n"
                )
                client_socket.sendall(option_response.encode('utf-8'))
                logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\n[GET OPTION RESPONSE SENT]")

            # ---------------------------------------------
            # 2) GET /iclock/getrequest?SN=...
            # ---------------------------------------------
            elif "GET /iclock/getrequest?SN=" in response:
                # a) 200 "OK"
                send_http_response(client_socket, "OK", 4)
                client_socket.sendall("OK".encode('utf-8'))
                logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\nOK\n")

                # b) Also send the command from your JSON
                now_str = datetime.now().strftime('%Y/%m/%d')
                # 2b-i) Replace placeholders "startTime" and "endTime" in cmd_template
                cmd_string = cmd_template.replace("startTime", now_str).replace("endTime", now_str)
                # e.g. "DATA QUERY ATTLOG StartTime=2023/02/14 EndTime=2023/02/14"

                cmd_text = f"C:{cmd_count}:{cmd_string}"
                send_http_response(client_socket, cmd_text, len(cmd_text))
                client_socket.sendall(cmd_text.encode('utf-8'))
                logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\n{cmd_text}")

            # ---------------------------------------------
            # 3) POST /iclock/cdata?SN=... &table=OPERLOG...
            # ---------------------------------------------
            elif "POST /iclock/cdata?SN=" in response and "&table=OPERLOG&Stamp=" in response:
                send_http_response(client_socket, "OK", 4)
                client_socket.sendall("OK".encode('utf-8'))
                logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\nOK\n")

            # ---------------------------------------------
            # 4) POST /iclock/devicecmd?SN=...
            # ---------------------------------------------
            elif "POST /iclock/devicecmd?SN=" in response:
                send_http_response(client_socket, "OK", 4)
                client_socket.sendall("OK".encode('utf-8'))
                logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\nOK\n")

            # ---------------------------------------------
            # 5) If device responds with "ID=xx &Return=0&CMD=DATA"
            #    => increment cmd_count
            # ---------------------------------------------
            if "ID=" in response and "&Return=0&CMD=DATA" in response:
                cmd_count += 1
                save_cmd_count(cmd_count)
                send_http_response(client_socket, "OK", 4)
                client_socket.sendall("OK".encode('utf-8'))
                logger.info(f"Sever Send Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}\nOK\n")

    except Exception as e:
        logger.error(f"Error handling client: {e}")
    finally:
        client_socket.close()
        logger.info(f"Sever Client Disconnected: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')}")

# -------------------------------
# START SERVER
# -------------------------------
def start_server(ip, port):
    """
    Bind to the specified IP/Port from Settings.json and handle connections.
    """
    server_address = (ip, port)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(server_address)
            server_socket.listen(5)
            logger.info(f"Sever Start:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')} - Listening on {ip}:{port}")

            while True:
                client_socket, addr = server_socket.accept()
                logger.info(f"Sever Accepted Connection: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')} - {addr}")
                client_handler = threading.Thread(target=handle_client, args=(client_socket,))
                client_handler.start()
    except OSError as e:
        logger.error(f"❌ Could not bind to {ip}:{port} -> {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error in start_server({ip}:{port}): {e}")

def main():
    # Load device IP/ports from Settings.json and the cmd template
    devices = load_settings()
    if not devices:
        logger.error(f"No valid (ip, port) pairs found in {settings_file}. Exiting...")
        return

    # Start a server thread for each (ip, port)
    logger.info(f"Starting servers for devices: {devices}")
    for (ip, port) in devices:
        threading.Thread(target=start_server, args=(ip, port), daemon=True).start()

    # Keep main thread alive
    while True:
        try:
            # Sleep or do some other monitoring
            pass
        except KeyboardInterrupt:
            logger.info("Shutting down servers via KeyboardInterrupt.")
            break

if __name__ == "__main__":
    main()
