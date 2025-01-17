import json
import threading
import time
import os
import logging
from datetime import datetime
import tempfile
import queue

class AttLogParser(threading.Thread):
    def __init__(self, attlog_file='attlog.json', check_interval=5):
        threading.Thread.__init__(self)
        self.attlog_file = attlog_file
        self.check_interval = check_interval
        self.last_position = 0

        # Load existing data from attlog.json to avoid duplicates
        self.existing_data = self.load_existing_data()
        self.lock = threading.Lock()
        self.log_queue = queue.Queue()

    def load_existing_data(self):
        if not os.path.exists(self.attlog_file):
            return set()
        
        with open(self.attlog_file, 'r') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                return set()
            return { (entry['ZKID'], entry['Timestamp'], entry['attype']) for entry in data }

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
            try:
                content = json.load(file)
            except json.JSONDecodeError:
                logging.error("Failed to load JSON content. File might be empty or malformed.")
                return
        
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
        sanitized_data = []
        
        if isinstance(content, str):
            lines = content.splitlines()
        elif isinstance(content, list):
            lines = content
        else:
            lines = []
        
        for line in lines:
            if "attlog" in line:
                parts = line.split("\t")
                if len(parts) < 12:
                    continue  # Skip lines that don't have enough parts
                zkid = parts[0].strip()
                timestamp = datetime.strptime(parts[1].strip(), "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
                inorout = int(parts[2].strip())
                attype = int(parts[3].strip())
                device = parts[10].strip()
                sn = parts[11].strip()
                devrec = parts[4].strip()

                unique_key = (zkid, timestamp, attype)
                if unique_key not in self.existing_data:
                    sanitized_data.append({
                        "ZKID": zkid,
                        "Timestamp": timestamp,
                        "InorOut": inorout,
                        "attype": attype,
                        "Device": device,
                        "SN": sn,
                        "Devrec": devrec
                    })
                    self.existing_data.add(unique_key)

        if sanitized_data:
            logging.info(f"\033[95mWriting {len(sanitized_data)} new entries to {self.attlog_file}\033[0m")
            for entry in sanitized_data:
                self.log_queue.put(entry)
            self.write_to_output()

    def write_to_output(self):
        with self.lock:
            try:
                with open(self.attlog_file, 'r') as file:
                    existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []

            if not isinstance(existing_data, list):
                existing_data = []

            # Append the new data from the queue
            while not self.log_queue.empty():
                entry = self.log_queue.get()
                existing_data.append(entry)
            
            # Write the updated data back to the file
            with tempfile.NamedTemporaryFile('w', delete=False, dir=os.path.dirname(self.attlog_file)) as tmp_file:
                json.dump(existing_data, tmp_file, indent=4)
                tempname = tmp_file.name
            
            os.replace(tempname, self.attlog_file)  # Atomic replace

        logging.info(f"\033[95mSuccessfully wrote entries to {self.attlog_file}\033[0m")

    def write_log_entry(self, log_entry):
        log_dict = self.parse_attlog(log_entry)
        self.log_queue.put(log_dict)
        self.write_to_output()

    @staticmethod
    def parse_attlog(log_entry):
        fields = log_entry.split('\t')
        timestamp_formatted = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
        log_dict = {
            "ZKID": fields[0].strip(),
            "Timestamp": timestamp_formatted,
            "InorOut": int(fields[2].strip()),
            "attype": int(fields[3].strip()),
            "Device": fields[10].strip(),
            "SN": fields[11].strip(),
            "Devrec": fields[4].strip()
        }
        return log_dict

# Example usage
if __name__ == "__main__":
    log_entry = "200\t2025-01-16 06:00:46\t0\t15\t0\t0\t0\t255\t0\t0\t('105.235.242.242', 54580)\tCP9M221860043"
    parser = AttLogParser()
    parser.write_log_entry(log_entry)
