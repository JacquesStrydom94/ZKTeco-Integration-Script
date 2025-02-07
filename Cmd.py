import asyncio
import socket
import threading
import json
import os
import logging
from datetime import datetime, timezone, timedelta

class Cmd:
    def __init__(self, settings_file, event):
        self.settings_file = settings_file
        self.event = event  # This event will control when other tasks can resume
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

    async def wait_until_specified_time(self):
        """Wait until the specified time, execute conditions, then resume all processes."""
        now = datetime.now()
        target_hour, target_minute = map(int, self.target_time.split(":"))
        target_datetime = datetime.combine(now.date(), datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute)

        if target_datetime <= now:
            target_datetime += timedelta(days=1)

        time_until_target = (target_datetime - now).total_seconds()
        logging.info(f"Waiting {time_until_target / 60:.2f} minutes until {self.target_time}.")
        await asyncio.sleep(time_until_target)  # Non-blocking wait until the target time

        logging.info(f"It's {self.target_time}! Running required conditions before resuming other tasks...")

        # Perform necessary checks before resuming other scripts
        while True:
            # Replace with actual condition check (Example: waiting for a file to be created)
            condition_met = os.path.exists("ready_signal.txt")  # Example condition

            if condition_met:
                break  # Exit the loop when condition is met
            
            logging.info("Condition not met. Waiting...")
            await asyncio.sleep(5)  # Recheck every 5 seconds

        logging.info("Condition met. Resuming all other processes.")
        self.event.set()  # Allow other tasks to proceed
