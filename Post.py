import json
import requests
import sqlite3
import logging
import time
from datetime import datetime
import os

SETTINGS_FILE = "Settings.json"
DB_NAME = "PUSH.db"

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {"logs": [{"DBID": ""}, {"Token": ""}]}
    
    with open(SETTINGS_FILE, "r") as file:
        return json.load(file)

class PostScript:
    def __init__(self, settings_file=SETTINGS_FILE, db_name=DB_NAME):
        self.config = load_settings()
        self.DBID = self.config["logs"][0].get("DBID", "")
        self.Token = self.config["logs"][1].get("Token", "")
        self.db_name = db_name
        self.last_processed_id = 0

    def fetch_new_records(self):
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM attendance WHERE id > ? AND RESPONSE IS NULL", (self.last_processed_id,))
            records = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            conn.close()
            return records, column_names
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            return [], []

    def post_and_update_records(self):
        while True:
            records, column_names = self.fetch_new_records()
            if not records:
                logger.info("No new records to post.")
            
            for record in records:
                record_dict = {column: value for column, value in zip(column_names, record) if column not in ["string", "id", "FTID", "RESPONSE", "KEY"]}
                
                for key, value in record_dict.items():
                    if isinstance(value, str) and '-' in value and ':' in value:
                        try:
                            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                            record_dict[key] = dt.strftime("%Y/%m/%d %H:%M:%S")
                        except ValueError:
                            pass
                
                record_json = json.dumps(record_dict)
                logger.info(f"📤 Posting JSON: {record_json}")
                
                try:
                    response = requests.post(
                        f'https://appnostic.dbflex.net/secure/api/v2/{self.DBID}/{self.Token}/ZK_stage/create.json',
                        data=record_json,
                        headers={
                            'Content-Type': 'application/json',
                            'Authorization': f'Bearer {self.Token}'
                        }
                    )
                    
                    logger.info(f"HTTP Status Code: {response.status_code}")
                    logger.info(f"Response Text: {response.text}")
                    
                    if response.status_code == 200:
                        response_data = response.json()[0]
                        try:
                            conn = sqlite3.connect(self.db_name)
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE attendance 
                                SET RESPONSE = ?, KEY = ?, FTID = ? 
                                WHERE id = ?
                            """, (response_data['status'], response_data['key'], response_data['id'], record[column_names.index('id')]))
                            conn.commit()
                            conn.close()
                            logger.info(f"✅ Updated record ID {record[column_names.index('id')]} in database.")
                            self.last_processed_id = record[column_names.index('id')]
                        except sqlite3.Error as e:
                            logger.error(f"Failed to update record in database: {e}")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request failed: {e}")
            
            time.sleep(10)  # Poll every 10 seconds

    def run(self):
        self.post_and_update_records()

if __name__ == "__main__":
    post_script = PostScript()
    post_script.run()
