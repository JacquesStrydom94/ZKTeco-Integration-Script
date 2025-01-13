import socket
import threading
import json
import os
from datetime import datetime, timezone, timedelta
import time
import logging

logger = logging.getLogger()

class CmdScript:
    def __init__(self, port, target_time, cmd_count_file="cmd_count.json"):
        self.port = port
        self.target_time = target_time
        self.cmd_count_file = cmd_count_file
        self.cmd_count = self.load_cmd_count()

    def load_cmd_count(self):
        if os.path.exists(self.cmd_count_file):
            with open(self.cmd_count_file, "r") as file:
                data = json.load(file)
                return data.get("cmd_count", 0)
        else:
            with open(self.cmd_count_file, "w") as file:
                json.dump({"cmd_count": 0}, file)
            return 0

    def save_cmd_count(self, count):
        with open(self.cmd_count_file, "w") as file:
            json.dump({"cmd_count": count}, file)

    def handle_client(self, client_socket):
        try:
            # Generate cmd with the current date before sending to the client
            cmd = f"DATA QUERY ATTLOG StartTime={datetime.now().strftime('%Y/%m/%d')} EndTime={datetime.now().strftime('%Y/%m/%d')}"
            message = f"C:{self.cmd_count}:{cmd}"
            client_socket.sendall(message.encode('utf-8'))
            logger.info("Server Send Data: {} - {}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'),
                message))

            command_sent = False

            while True:
                try:
                    response = client_socket.recv(1024).decode('utf-8')
                    if response:
                        logger.info("Client Response: {} - {}".format(
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f'),
                            response))
                        # Send "OK" immediately after receiving any content
                        client_socket.sendall("OK".encode('utf-8'))

                        if not command_sent and f"ID={self.cmd_count}&Return=0&CMD=" in response:
                            self.cmd_count += 1
                            self.save_cmd_count(self.cmd_count)

                            # Send HTTP response back to the client
                            date_now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
                            http_response = (
                                "HTTP/1.1 200 OK\n"
                                "Content-Type: text/plain\n"
                                "Accept-Ranges: bytes\n"
                                f"Date: {date_now}\n"
                                "Content-Length: 4\n"
                            )
                            client_socket.sendall(http_response.encode('utf-8'))
                            
                            command_sent = True  # Set flag to indicate the command was sent
                        else:
                            # After the command is sent, only receive data
                            logger.info("Only receiving data")
                        
                except socket.error as e:
                    logger.error(f"Socket error: {e.strerror} (Error code: {e.errno})")
                except Exception as e:
                    logger.error(f"Error receiving data from client: {e}")

                time.sleep(1)

        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            if client_socket:
                client_socket.close()
            logger.info("Client connection closed")

    def run(self):
        while True:
            current_time = time.strftime("%H:%M")
            if current_time == self.target_time:
                self.logger.info(f"CmdScript is running at scheduled time {self.target_time}.")
                # Add any additional functionality you need here
                # Example: Call some function or perform an action
            time.sleep(60)  # Check every minute
