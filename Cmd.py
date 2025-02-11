import asyncio
import socket
import threading
import json
import os
import logging
import ntplib
from datetime import datetime, timezone, timedelta

class Cmd:
    def __init__(self, settings_file, pause_event):
        self.settings_file = settings_file
        self.pause_event = pause_event  # Controls when services pause/resume
        self.cmd_count_file = "cmd_count.json"
        self.response_received = asyncio.Event()  # Signal when correct response is received

        self.load_settings()  # Load parameters from Settings.json
        self.cmd_count = self.load_cmd_count()

    def load_settings(self):
        """Load settings from a JSON file."""
        if not os.path.exists(self.settings_file):
            logging.error(f"⚠️ Settings file '{self.settings_file}' not found.")
            exit(1)
        
        with open(self.settings_file, "r") as file:
            data = json.load(file)
            self.target_time = data["settings"]["target_time"]
            self.cmd_template = data["settings"]["cmd"]
            self.device_ip = next((entry["ip"] for entry in data["devices"] if "ip" in entry), "127.0.0.1")
            self.ports = [entry["port"] for entry in data["devices"] if "port" in entry]

    def load_cmd_count(self):
        """Load command count from a JSON file."""
        if os.path.exists(self.cmd_count_file):
            with open(self.cmd_count_file, "r") as file:
                data = json.load(file)
                return data.get("cmd_count", 0)
        return 0

    def save_cmd_count(self, count):
        """Save the current command count to a file."""
        with open(self.cmd_count_file, "w") as file:
            json.dump({"cmd_count": count}, file)

    def get_ntp_time(self):
        """Fetch the accurate time from an NTP server and adjust to GMT+2."""
        ntp_client = ntplib.NTPClient()
        try:
            response = ntp_client.request("pool.ntp.org", version=3)
            utc_time = datetime.utcfromtimestamp(response.tx_time).replace(tzinfo=timezone.utc)
            gmt_plus_2_time = utc_time + timedelta(hours=2)  # Convert to GMT+2
            return gmt_plus_2_time
        except Exception as e:
            logging.error(f"❌ Failed to get time from NTP server: {e}")
            return datetime.now(timezone.utc) + timedelta(hours=2)  # Fallback to system time

    async def wait_until_specified_time(self):
        """Pause all services at the exact target time in GMT+2."""
        now = self.get_ntp_time()
        target_hour, target_minute = map(int, self.target_time.split(":"))
        target_datetime = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

        if target_datetime <= now:
            target_datetime += timedelta(days=1)  # If time has passed, schedule for the next day

        time_until_target = (target_datetime - now).total_seconds()
        logging.info(f"⏳ Waiting {time_until_target / 60:.2f} minutes until {self.target_time} GMT+2.")
        await asyncio.sleep(time_until_target)  # Non-blocking wait

        logging.info(f"⏰ Reached {self.target_time} GMT+2, pausing services...")
        self.pause_event.clear()  # PAUSE all other services

        # Start sending commands to all ports using the public IP
        for port in self.ports:
            threading.Thread(target=self.send_command_to_device, args=(self.device_ip, port)).start()

        # Start listening for responses on all specified ports
        for port in self.ports:
            threading.Thread(target=self.start_server, args=(port,)).start()

        # Wait until a valid response is received before resuming services
        logging.info("⏳ Waiting for a valid TCP response before resuming services...")
        await self.response_received.wait()

        logging.info("✅ Valid TCP response received. Resuming all services.")
        self.pause_event.set()  # RESUME all services

    def send_command_to_device(self, device_ip, port):
        """Send a command to the given device IP and port."""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(5)
            client_socket.connect((device_ip, port))

            # Generate command with the current date
            start_time = datetime.now().strftime('%Y/%m/%d')
            end_time = datetime.now().strftime('%Y/%m/%d')
            cmd = self.cmd_template.replace("startTime", start_time).replace("endTime", end_time)
            message = f"C:{self.cmd_count}:{cmd}"

            client_socket.sendall(message.encode('utf-8'))
            logging.info(f"📤 Sent data to {device_ip}:{port} - {message}")

            client_socket.close()

        except socket.error as e:
            logging.error(f"❌ Failed to send command to {device_ip}:{port} - {e}")

    def start_server(self, port):
        """Start a TCP server that listens for responses."""
        server_address = ("0.0.0.0", port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind(server_address)
            server_socket.listen(5)
            logging.info(f"🟢 Server listening for responses on 0.0.0.0:{port}")

            while True:
                client_socket, addr = server_socket.accept()
                logging.info(f"✅ Accepted connection from {addr}")

                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                client_handler.start()

    def handle_client(self, client_socket):
        """Handle incoming responses to confirm conditions are met."""
        try:
            while True:
                response = client_socket.recv(1024).decode('utf-8')
                if not response:
                    break

                logging.info(f"📥 Received response: {response}")
                client_socket.sendall("OK".encode('utf-8'))  # Acknowledge response

                # Check if the correct condition is met before resuming services
                if f"ID={self.cmd_count}&Return=0&CMD=" in response:
                    self.cmd_count += 1
                    self.save_cmd_count(self.cmd_count)

                    # Notify that the correct response has been received
                    self.response_received.set()

                    # Send HTTP response
                    date_now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
                    http_response = (
                        "HTTP/1.1 200 OK\n"
                        "Content-Type: text/plain\n"
                        "Accept-Ranges: bytes\n"
                        f"Date: {date_now}\n"
                        "Content-Length: 4\n"
                    )
                    client_socket.sendall(http_response.encode('utf-8'))

        except Exception as e:
            logging.error(f"❌ Error handling response: {e}")
        finally:
            client_socket.close()
            logging.info("🔴 Response connection closed")
