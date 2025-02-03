import sqlite3
import json
import os
import logging
from datetime import datetime
import time

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

SETTINGS_FILE = "Settings.json"
ATTLOG_FILE = "attlog.json"
DB_NAME = "PUSH.db"

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

def parse_timestamp(timestamp):
    """
    Convert timestamp to the correct format, handling both 'YYYY/MM/DD HH:MM:SS' and 'YYYY-MM-DD HH:MM:SS'.
    """
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(timestamp, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    logger.error(f"⚠ Invalid Timestamp format: {timestamp}")
    return None

class Dbcon:
    def __init__(self, attlog_file=ATTLOG_FILE, db_name=DB_NAME):
        self.attlog_file = attlog_file
        self.db_name = db_name

    def process_attlog_file(self):
        if os.path.getsize(self.attlog_file) == 0:
            logger.info("⚠ attlog.json file is empty. Skipping processing.")
            return

        try:
            with open(self.attlog_file, 'r') as file:
                content = json.load(file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"❌ Error reading attlog.json: {e}")
            return

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ZKID TEXT,
            Timestamp TEXT,
            InorOut INTEGER,
            attype INTEGER,
            Device TEXT,
            SN TEXT,
            Devrec TEXT,
            RESPONSE TEXT,
            KEY TEXT,
            FTID TEXT
        )''')

        record_count = get_record_count()
        new_count = record_count

        for entry in content[record_count:]:
            formatted_timestamp = parse_timestamp(entry['Timestamp'])
            if formatted_timestamp is None:
                continue  # Skip invalid timestamps

            # Check if record already exists
            cursor.execute("SELECT COUNT(*) FROM attendance WHERE ZKID = ? AND Timestamp = ?", (entry['ZKID'], formatted_timestamp))
            exists = cursor.fetchone()[0] > 0

            if not exists:
                logger.info(f"📌 Inserting new record: {entry}")  # Log each new record before inserting
                cursor.execute('''
                    INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                    (entry['ZKID'], formatted_timestamp, entry['InorOut'], entry['attype'], entry['Device'], entry['SN'], entry.get('Devrec', '')))
                
                new_count += 1
            else:
                logger.info(f"⚠ Record already exists, skipping: {entry}")

        conn.commit()
        conn.close()

        update_record_count(new_count)
        logger.info(f"✅ Successfully inserted {new_count - record_count} new records into PUSH.db")

    def run(self):
        try:
            while True:
                self.process_attlog_file()
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Script stopped by user.")

if __name__ == "__main__":
    dbcon_script = Dbcon()
    dbcon_script.run()
