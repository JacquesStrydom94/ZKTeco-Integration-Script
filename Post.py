import sqlite3
import json
import requests
import time
import logging
from datetime import datetime
from logger_setup import logger  # improved logging

SETTINGS_FILE = "Settings.json"
DB_NAME = "PUSH.db"

DEFAULT_SETTINGS = {
    "logs": [
        {"DBID": ""},
        {"Token": ""}
    ]
}

def load_settings(settings_file=SETTINGS_FILE):
    try:
        with open(settings_file, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.error(f"❌ {settings_file} missing or corrupted. Using default settings.")
        return DEFAULT_SETTINGS

class PostScript:
    def __init__(self, settings_file=SETTINGS_FILE, db_name=DB_NAME):
        self.config = load_settings(settings_file)
        self.DBID = self.config["logs"][0].get("DBID", "")
        self.Token = self.config["logs"][1].get("Token", "")
        self.db_name = db_name
        self.last_processed_id = 0  # track last posted record's ID

    def fetch_new_records(self):
        """Fetch un-posted records from 'attendance' table."""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        # We rely on 'RESPONSE IS NULL' to know un-posted
        cursor.execute("""
            SELECT id, ZKID, Timestamp, InorOut, attype, Device, SN, Devrec
            FROM attendance
            WHERE (RESPONSE IS NULL OR RESPONSE = '')
              AND id > ?
            ORDER BY id ASC
        """, (self.last_processed_id,))
        records = cursor.fetchall()
        conn.close()
        return records

    def post_and_update_records(self):
        """Continuously post new records to the API and update DB with the response."""
        while True:
            records = self.fetch_new_records()

            for row in records:
                (rec_id, zkid, ts, inout, attyp, dev, sn, devrec) = row

                # Convert timestamp if needed
                # Already in 'YYYY-MM-DD HH:MM:SS'. If your API wants 'YYYY/MM/DD HH:MM:SS':
                try:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                    ts_for_api = dt.strftime("%Y/%m/%d %H:%M:%S")
                except ValueError:
                    ts_for_api = ts

                record_dict = {
                    "ZKID": zkid,
                    "Timestamp": ts_for_api,
                    "InorOut": inout,
                    "attype": attyp,
                    "Device": dev,
                    "SN": sn,
                    "Devrec": devrec
                }

                try:
                    response = requests.post(
                        f'https://appnostic.dbflex.net/secure/api/v2/{self.DBID}/{self.Token}/ZK_stage/create.json',
                        json=record_dict,
                        headers={
                            'Content-Type': 'application/json',
                            'Authorization': f'Bearer {self.Token}'
                        },
                        timeout=10
                    )

                    if response.status_code == 200:
                        # Expecting something like: [{"status": "...", "key": "...", "id": "..."}]
                        data = response.json()
                        if isinstance(data, list) and data:
                            status_val = data[0].get('status', '')
                            key_val = data[0].get('key', '')
                            ftid_val = data[0].get('id', '')

                            # Update DB
                            conn = sqlite3.connect(self.db_name)
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE attendance
                                SET RESPONSE = ?, KEY = ?, FTID = ?
                                WHERE id = ?
                            """, (status_val, key_val, ftid_val, rec_id))
                            conn.commit()
                            conn.close()

                            self.last_processed_id = rec_id
                            logger.info(f"✅ Successfully posted record ID {rec_id}")
                        else:
                            logger.error(f"❌ Unexpected response structure: {response.text}")
                    else:
                        logger.error(f"❌ Post failed (HTTP {response.status_code}): {response.text}")

                except requests.exceptions.RequestException as e:
                    logger.error(f"❌ HTTP error posting ID {rec_id}: {e}")

            time.sleep(10)

    def run(self):
        self.post_and_update_records()
