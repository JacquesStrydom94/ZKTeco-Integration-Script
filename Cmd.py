import asyncio
import socket
import threading
import json
import os
import logging
from datetime import datetime, timezone, timedelta

class Cmd:
    def __init__(self, settings_file, pause_event):
        self.settings_file = settings_file
        self.pause_event = pause_event  # Event for pausing and resuming
        self.load_settings()
        self.cmd_count_file = "cmd_count.json"
        self.cmd_count = self.load_cmd_count()

    def load_settings(self):
        """Load settings from a JSON file."""
        if not os.path.exists(self.settings_file):
            logging.error(f"Settings file '{self.settings_file}' not found.")
            exit(1)
        
        with open(self.settings_file, "r") as file:
            data = json.load(file)
            self.target_time = data["settings"]["target_time"]
            self.cmd_template = data["settings"]["cmd"]
            self.devices = data.get("devices", [])

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

    async def wait_until_specified_time(self):
        """Pause all services at the target time, check conditions, then resume execution."""
        now = datetime.now()
        target_hour, target_minute = map(int, self.target_time.split(":"))
        target_datetime = datetime.combine(now.date(), datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute)

        if target_datetime <= now:
            target_datetime += timedelta(days=1)

        time_until_target = (target_datetime - now).total_seconds()
        logging.info(f"Waiting {time_until_target / 60:.2f} minutes until {self.target_time}.")
        await asyncio.sleep(time_until_target)  # Non-blocking wait

        logging.info(f"Reached {self.target_time}, pausing services...")

        self.pause_event.clear()  # PAUSE all other tasks

        # Handle client connections (this was part of the original script)
        for device in self.devices:
            threading.Thread(target=self.start_server, args=(device["port"],)).start()

        # Perform necessary checks before resuming
        while True:
            condition_met = os.path.exists("ready_signal.txt")  # Example condition

            if condition_met:
                break  # Exit loop when condition is met
            
            logging.info("Condition not met. Waiting...")
            await asyncio.sleep(5)  # Recheck every 5 seconds

        logging.info("Condition met. Resuming all services.")
        self.pause_event.set()  # RESUME all services

    def handle_client(self, client_socket):
        """Handle incoming client connections."""
        try:
            while True:
                start_time = datetime.now().strftime('%Y/%m/%d')
                end_time = datetime.now().strftime('%Y/%m/%d')
                cmd = self.cmd_template.replace("startTime", start_time).replace("endTime", end_time)
                message = f"C:{self.cmd_count}:{cmd}"
                
                client_socket.sendall(message.encode('utf-8'))
                logging.info(f"Server Sent Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')} - {message}")

                command_sent = False

                while True:
                    try:
                        response = client_socket.recv(1024).decode('utf-8')
                        if not response:
                            break

                        logging.info(f"Client Response: {datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')} - {response}")
                        client_socket.sendall("OK".encode('utf-8'))  # Acknowledge response

                        if not command_sent and f"ID={self.cmd_count}&Return=0&CMD=" in response:
                            self.cmd_count += 1
                            self.save_cmd_count(self.cmd_count)

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
                            command_sent = True

                    except socket.error as e:
                        logging.error(f"Socket error: {e}")
                        break
                    except Exception as e:
                        logging.error(f"Error receiving data: {e}")
                        break

                asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Error handling client: {e}")
        finally:
            client_socket.close()
            logging.info("Client connection closed")

    def start_server(self, port):
        """Start a server that listens on the given port."""
        server_address = ("0.0.0.0", port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind(server_address)
            server_socket.listen(5)
            logging.info(f"Server listening on 0.0.0.0:{port}")

            while True:
                client_socket, addr = server_socket.accept()
                logging.info(f"Accepted connection from {addr}")

                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                client_handler.start()
