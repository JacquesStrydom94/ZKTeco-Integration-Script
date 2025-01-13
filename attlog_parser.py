
import json
import threading
import time
import os
import logging

class AttLogParser(threading.Thread):
    def __init__(self, attlog_file='attlog.json', output_file='sanatise.json', check_interval=5):
        threading.Thread.__init__(self)
        self.attlog_file = attlog_file
        self.output_file = output_file
        self.check_interval = check_interval
        self.last_position = 0

        # Load existing data from sanatise.json to avoid duplicates
        self.existing_data = self.load_existing_data()

    def load_existing_data(self):
        if not os.path.exists(self.output_file):
            return set()
        
        with open(self.output_file, 'r') as file:
            data = json.load(file)
            return { (entry['zkid'], entry['timestamp'], entry['attype']) for entry in data }

    def run(self):
        logging.info("Starting AttLogParser thread")
        # Process existing data in attlog.json at the start
        self.process_existing_attlog()

        while True:
            self.check_for_new_content()
            time.sleep(self.check_interval)

    def process_existing_attlog(self):
        if not os.path.exists(self.attlog_file):
            logging.info(f"{self.attlog_file} does not exist.")
            return

        logging.info(f"Processing existing data in {self.attlog_file}")
        with open(self.attlog_file, 'r') as file:
            content = file.read()
        
        self.parse_and_write(content)

    def check_for_new_content(self):
        if not os.path.exists(self.attlog_file):
            return

        with open(self.attlog_file, 'r') as file:
            file.seek(self.last_position)
            new_content = file.read()
            self.last_position = file.tell()

        if new_content:
            logging.info("New content found in attlog.json")
            self.parse_and_write(new_content)

    def parse_and_write(self, content):
        lines = content.splitlines()
        sanitized_data = []

        for line in lines:
            if "attlog" in line:
                parts = line.split("\t")
                if len(parts) < 2:
                    continue  # Skip lines that don't have enough parts
                zkid = parts[0].split()[0]
                timestamp = parts[1]
                attype = parts[2].split()[0] if len(parts) > 2 else None

                unique_key = (zkid, timestamp, attype)
                if unique_key not in self.existing_data:
                    sanitized_data.append({
                        "zkid": zkid,
                        "timestamp": timestamp,
                        "attype": attype
                    })
                    self.existing_data.add(unique_key)

        if sanitized_data:
            logging.info(f"\033[95mWriting {len(sanitized_data)} new entries to {self.output_file}\033[0m")
            self.write_to_output(sanitized_data)

    def write_to_output(self, data):
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w') as file:
                json.dump([], file)

        with open(self.output_file, 'r+') as file:
            existing_data = json.load(file)
            existing_data.extend(data)
            file.seek(0)
            json.dump(existing_data, file, indent=4)
        logging.info(f"\033[95mSuccessfully wrote {len(data)} entries to {self.output_file}\033[0m")