import json
import threading
import time
import os
import logging
from datetime import datetime
import tempfile
import queue

SETTINGS_FILE = "Settings.json"
ATTLOG_FILE = "attlog.json"

def load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {"System para": [{"Rec Count": "0"}]}
    with open(SETTINGS_FILE, "r") as file:
        return json.load(file)

def save_settings(settings):
    with open(SETTINGS_FILE, "w") as file:
        json.dump(settings, file, indent=4)

def get_record_count():
    settings = load_settings()
    return int(settings["System para"][0]["Rec Count"])

def update_record_count(count):
    settings = load_settings()
    settings["System para"][0]["Rec Count"] = str(count)
    save_settings(settings)

class AttLogParser(threading.Thread):
    def __init__(self, attlog_file=ATTLOG_FILE, check_interval=5):
        threading.Thread.__init__(self)
        self.attlog_file = attlog_file
        self.check_interval = check_interval
        self.log_queue = queue.Queue()
        self.lock = threading.Lock()

    def run(self):
        logging.info("Starting AttLogParser thread")
        while True:
            self.check_for_new_content()
            time.sleep(self.check_interval)

    def check_for_new_content(self):
        if not os.path.exists(self.attlog_file):
            return
        
        with open(self.attlog_file, 'r') as file:
            try:
                content = json.load(file)
            except json.JSONDecodeError:
                logging.error("Failed to load JSON content. File might be empty or malformed.")
                return
        
        self.parse_and_write(content)

    def parse_and_write(self, content):
        record_count = get_record_count()
        new_entries = content[record_count:]

        if not new_entries:
            return

        with self.lock:
            try:
                with open(self.attlog_file, 'r') as file:
                    existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []

            if not isinstance(existing_data, list):
                existing_data = []

            for entry in new_entries:
                self.log_queue.put(entry)
                existing_data.append(entry)
            
            with tempfile.NamedTemporaryFile('w', delete=False, dir=os.path.dirname(self.attlog_file)) as tmp_file:
                json.dump(existing_data, tmp_file, indent=4)
                tempname = tmp_file.name
            
            os.replace(tempname, self.attlog_file)
            update_record_count(record_count + len(new_entries))
        
        logging.info(f"Successfully wrote {len(new_entries)} new entries to {self.attlog_file}")

if __name__ == "__main__":
    parser = AttLogParser()
    parser.start()
