import sqlite3
import json
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class Dbcon:
    def __init__(self, attlog_file='attlog.json', db_name='PUSH.db'):
        self.attlog_file = attlog_file
        self.db_name = db_name
        self.processed_entries = set()

    def record_exists(self, cursor, devrec, timestamp):
        try:
            cursor.execute('SELECT 1 FROM attendance WHERE Devrec = ? AND Timestamp = ?', (devrec, timestamp))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Database query error: {e}")
            return False

    def process_attlog_file(self):
        # Check if the attlog.json file is empty
        if os.path.getsize(self.attlog_file) == 0:
            logger.info("attlog.json file is empty. Skipping processing.")
            return

        # Read the content of the attlog.json file
        try:
            with open(self.attlog_file, 'r') as file:
                content = json.load(file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error reading attlog.json: {e}")
            return

        # Connect to SQLite database (or create it if it doesn't exist)
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return

        # Create the attendance table if it doesn't exist
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
                Devrec TEXT,
                RESPONSE TEXT,
                KEY TEXT,
                FTID TEXT
            )
            ''')
        except sqlite3.Error as e:
            logger.error(f"Error creating attendance table: {e}")
            conn.close()
            return

        # Insert each record into the attendance table with appropriate column values
        for entry in content:
            logger.debug(f"Processing entry: {entry}")
            if all(key in entry for key in ('ZKID', 'Timestamp', 'InorOut', 'attype', 'Device', 'SN')):
                ZKID = entry['ZKID']
                Timestamp = entry['Timestamp']
                InorOut = entry['InorOut']
                attype = entry['attype']
                Device = entry['Device']
                SN = entry['SN']
                Devrec = entry.get('Devrec', '')

                unique_key = (ZKID, Timestamp, attype)
                
                if unique_key not in self.processed_entries:
                    # Convert Timestamp to a format that SQLite understands
                    try:
                        formatted_timestamp = datetime.strptime(Timestamp, "%Y/%m/%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError as e:
                        logger.error(f"Invalid Timestamp format: {Timestamp} - {e}")
                        continue

                    logger.debug(f"Inserting entry with formatted timestamp: {formatted_timestamp}")
                    if not self.record_exists(cursor, Devrec, formatted_timestamp):
                        try:
                            cursor.execute('''
                                INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (ZKID, formatted_timestamp, InorOut, attype, Device, SN, Devrec))
                            logger.info(f"Inserted record: {entry}")
                        except sqlite3.Error as e:
                            logger.error(f"Failed to insert record: {e}")
                    else:
                        logger.info(f"Record already exists: {entry}")

                    self.processed_entries.add(unique_key)
                else:
                    logger.debug(f"Duplicate entry found and skipped: {unique_key}")

        # Commit the transaction and close the connection
        try:
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to commit transaction: {e}")
        finally:
            conn.close()

        logger.info("Attendance records have been successfully inserted into the PUSH.db database.")

    def run(self):
        try:
            while True:
                self.process_attlog_file()
                time.sleep(10)  # Wait for 10 seconds before processing the file again
        except KeyboardInterrupt:
            logger.info("Script stopped by user.")

if __name__ == "__main__":
    dbcon_script = Dbcon()
    dbcon_script.run()
