import sqlite3
import json
import requests
import os
import time
import logging
from datetime import datetime
from logger_setup import logger  # Import the improved logging setup

SETTINGS_FILE = "Settings.json"
DB_NAME = "PUSH.db"

DEFAULT_SETTINGS = {
    "logs": [
        {"DBID": ""},
        {"Token": ""}
    ]
}

def load_settings():
    """Load settings from the JSON file, reset to default if corrupted or missing."""
    if not os.path.exists(SETTINGS_FILE):
        logger.warning("⚠️ Settings.json not found. Creating a new one with default values.")
        save_settings(DEFAULT_SETTINGS)
        return DEFAULT_SETTINGS

    try:
        with open(SETTINGS_FILE, "r") as file:
            return json.load(file)
    except (json.JSONDecodeError, ValueError):
        logger.error("❌ Settings.json is corrupted. Resetting to default values.")
        save_settings(DEFAULT_SETTINGS)
        return DEFAULT_SETTINGS

def save_settings(data):
    """Save settings to the JSON file."""
    with open(SETTINGS_FILE, "w") as file:
        json.dump(data, file, indent=4)

class PostScript:
    def __init__(self, settings_file=SETTINGS_FILE, db_name=DB_NAME):
        self.config = load_settings()
        self.DBID = self.config["logs"][0].get("DBID", "")
        self.Token = self.config["logs"][1].get("Token", "")
        self.db_name = db_name
        self.last_processed_id = 0

    def fetch_new_records(self):
        """Fetch new attendance records that have not been processed."""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM attendance WHERE id > ? AND RESPONSE IS NULL", (self.last_processed_id,))
        records = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        conn.close()
        return records, column_names

    def post_and_update_records(self):
        """Post new records to the API and update the database with the response."""
        while True:
            records, column_names = self.fetch_new_records()
            
            for record in records:
                record_dict = {column: value for column, value in zip(column_names, record) if column not in ["id", "FTID", "RESPONSE", "KEY"]}
                
                # Convert timestamp format if necessary
                if isinstance(record_dict.get("Timestamp"), str) and '-' in record_dict["Timestamp"]:
                    try:
                        dt = datetime.strptime(record_dict["Timestamp"], "%Y-%m-%d %H:%M:%S")
                        record_dict["Timestamp"] = dt.strftime("%Y/%m/%d %H:%M:%S")
                    except ValueError:
                        continue
                
                record_json = json.dumps(record_dict)
                
                try:
                    response = requests.post(
                        f'https://appnostic.dbflex.net/secure/api/v2/{self.DBID}/{self.Token}/ZK_stage/create.json',
                        data=record_json,
                        headers={
                            'Content-Type': 'application/json',
                            'Authorization': f'Bearer {self.Token}'
                        }
                    )
                    
                    if response.status_code == 200:
                        response_data = response.json()[0]
                        conn = sqlite3.connect(self.db_name)
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE attendance 
                            SET RESPONSE = ?, KEY = ?, FTID = ? 
                            WHERE id = ?
                        """, (response_data.get('status'), response_data.get('key'), response_data.get('id'), record[0]))
                        conn.commit()
                        conn.close()
                        self.last_processed_id = record[0]
                        logger.info(f"✅ Successfully processed record ID: {record[0]}")
                    else:
                        logger.error(f"❌ Failed to process record ID: {record[0]} - HTTP {response.status_code}")
                
                except requests.exceptions.RequestException as e:
                    logger.error(f"❌ HTTP request failed: {e}")
            
            time.sleep(10)
    
    def run(self):
        """Start posting and updating records."""
        self.post_and_update_records()

if __name__ == "__main__":
    post_script = PostScript()
    post_script.run()
