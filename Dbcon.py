import json
import sqlite3
import os
import logging
from datetime import datetime
import time

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

SETTINGS_FILE = "Settings.json"

def load_settings():
    """ Load settings.json and get Rec Count """
    if not os.path.exists(SETTINGS_FILE):
        return {"System para": [{"Rec Count": "0"}]}
    
    with open(SETTINGS_FILE, "r") as file:
        data = json.load(file)
    return data

def save_settings(settings):
    """ Save updated settings.json """
    with open(SETTINGS_FILE, "w") as file:
        json.dump(settings, file, indent=4)

class Dbcon:
    def __init__(self, attlog_file='attlog.json', db_name='PUSH.db'):
        self.attlog_file = attlog_file
        self.db_name = db_name
        self.settings = load_settings()
        self.rec_count = int(self.settings["System para"][0]["Rec Count"])  # Get last processed count

    def process_attlog_file(self):
        if os.path.getsize(self.attlog_file) == 0:
            logger.info("attlog.json file is empty. Skipping processing.")
            return

        try:
            with open(self.attlog_file, 'r') as file:
                content = json.load(file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error reading attlog.json: {e}")
            return

        if self.rec_count >= len(content):
            logger.info("No new records to process.")
            return

        new_entries = content[self.rec_count:]
        logger.info(f"Processing {len(new_entries)} new entries from attlog.json")
        
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return

        try:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS attendance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ZKID TEXT,
                Timestamp TEXT,
                InorOut INTEGER,
                attype INTEGER,
                Device TEXT,
                SN TEXT,
                Devrec TEXT
            )
            ''')
        except sqlite3.Error as e:
            logger.error(f"Error creating attendance table: {e}")
            conn.close()
            return

        for entry in new_entries:
            logger.debug(f"Processing entry: {entry}")
            if all(key in entry for key in ('ZKID', 'Timestamp', 'InorOut', 'attype', 'Device', 'SN')):
                try:
                    formatted_timestamp = datetime.strptime(entry['Timestamp'], "%Y/%m/%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError as e:
                    logger.error(f"Invalid Timestamp format: {entry['Timestamp']} - {e}")
                    continue
                
                try:
                    cursor.execute('''
                        INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (entry['ZKID'], formatted_timestamp, entry['InorOut'], entry['attype'], entry['Device'], entry['SN'], entry.get('Devrec', '')))
                    logger.info(f"Inserted record: {entry}")
                except sqlite3.Error as e:
                    logger.error(f"Failed to insert record: {e}")

        try:
            conn.commit()
            self.rec_count += len(new_entries)
            self.settings["System para"][0]["Rec Count"] = str(self.rec_count)
            save_settings(self.settings)
            logger.info(f"Updated record count: {self.rec_count}")
        except sqlite3.Error as e:
            logger.error(f"Failed to commit transaction: {e}")
        finally:
            conn.close()

    def run(self):
        try:
            while True:
                self.process_attlog_file()
                time.sleep(10)  # Wait 10 seconds before checking again
        except KeyboardInterrupt:
            logger.info("Script stopped by user.")

if __name__ == "__main__":
    dbcon_script = Dbcon()
    dbcon_script.run()
