import json
import sqlite3
import time
import os
import logging

logger = logging.getLogger()

class Dbcon:
    def __init__(self, attlog_file='attlog.json', db_name='PUSH.db'):
        self.attlog_file = attlog_file
        self.db_name = db_name

    def record_exists(self, cursor, devrec, timestamp):
        cursor.execute('SELECT 1 FROM attendance WHERE Devrec = ? AND Timestamp = ?', (devrec, timestamp))
        return cursor.fetchone() is not None

    def process_attlog_file(self):
        # Check if the attlog.json file is empty
        if os.path.getsize(self.attlog_file) == 0:
            logger.info("attlog.json file is empty. Skipping processing.")
            return

        # Read the content of the attlog.json file
        with open(self.attlog_file, 'r') as file:
            content = json.load(file)

        # Connect to SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Create the attendance table if it doesn't exist
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

        # Insert each record into the attendance table with appropriate column values
        for entry in content:
            attlog = entry['attlog']
            records = attlog.split('\n')
            if records:
                # Initialize Device and SN values
                Device = None
                SN = None

                # Extract Device and SN values from any record that has them
                for r in records:
                    values = r.split('\t')
                    if len(values) >= 11 and values[-2] and values[-1]:
                        Device = values[-2]
                        SN = values[-1]
                        break
                
                for record in records:
                    values = record.split('\t')
                    # Ensure there are enough values to avoid index out of range error
                    if len(values) >= 4:  # Ensure we have at least ZKID, Timestamp, InorOut, and attype
                        # Extract values for columns
                        ZKID = values[0]
                        Timestamp = values[1]
                        InorOut = values[2]
                        attype = values[3]
                        if len(values) >= 11:
                            Devrec = values[-3]
                        else:
                            Devrec = ''

                        # Use the previously extracted Device and SN values
                        if not self.record_exists(cursor, Devrec, Timestamp):
                            cursor.execute('''
                                INSERT INTO attendance (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (ZKID, Timestamp, InorOut, attype, Device, SN, Devrec))
                            logger.info(f"Inserted record: {values}")

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

        logger.info("Attendance Records have been successfully inserted into the PUSH.db database.")

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
