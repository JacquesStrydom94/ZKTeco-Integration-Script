import sqlite3
import json
import os
import logging
from datetime import datetime

logger = logging.getLogger()

class Dbcon:
    def __init__(self, attlog_file="attlog.json"):
        self.attlog_file = attlog_file

    def process_attlog_file(self):
        if not os.path.exists(self.attlog_file):
            logger.warning(f"{self.attlog_file} does not exist.")
            return

        with open(self.attlog_file, "r") as file:
            data = json.load(file)

        conn = sqlite3.connect('PUSH.db')
        cursor = conn.cursor()

        for record in data:
            try:
                cursor.execute('''
                INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec, RESPONSE, KEY, FTID)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record["ZKID"],
                    record["Timestamp"],
                    record["InorOut"],
                    record["attype"],
                    record["Device"],
                    record["SN"],
                    record["Devrec"],
                    record["RESPONSE"],
                    record["KEY"],
                    record["FTID"]
                ))
                logger.info(f"Record inserted: {record}")
            except sqlite3.Error as e:
                logger.error(f"Error inserting record: {e}")

        conn.commit()
        conn.close()

    def run(self):
        self.process_attlog_file()
