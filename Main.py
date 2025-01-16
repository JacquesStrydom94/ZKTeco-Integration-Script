import threading
import sqlite3
import os
import logging
from datetime import datetime
from Post import PostScript
from Dbcon import Dbcon
from TcpServer import TcpServer
import socket
import sys
from contextlib import redirect_stdout, redirect_stderr

import importlib.util
spec = importlib.util.spec_from_file_location("CmdScriptModule", "Cmd.py")
CmdScriptModule = importlib.util.module_from_spec(spec)
spec.loader.exec_module(CmdScriptModule)

# Setup logging
class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

# Configure logging to write to both console and server.log file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[
    logging.StreamHandler(sys.stdout)  # Ensure output goes to stdout as well
])
logger = logging.getLogger()
formatter = CustomFormatter()
for handler in logger.handlers:
    handler.setFormatter(formatter)

def check_db():
    db_name = 'PUSH.db'
    table_name = 'attendance'
    required_columns = [
        "id INTEGER PRIMARY KEY AUTOINCREMENT",
        "ZKID TEXT",
        "Timestamp TEXT",
        "InorOut INTEGER",
        "attype INTEGER",
        "Device TEXT",
        "SN TEXT",
        "Devrec TEXT",
        "RESPONSE TEXT",
        "KEY TEXT",
        "FTID TEXT"
    ]

    if os.path.exists(db_name):
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Check if the 'attendance' table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        result = cursor.fetchone()

        if result:
            # Get existing columns in the 'attendance' table
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [info[1] for info in cursor.fetchall()]

            for column_def in required_columns:
                column_name = column_def.split()[0]
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")
                    conn.commit()

            # Check and create the UNIQUE constraint
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            unique_index_exists = any('unique' in index[1].lower() for index in indexes)

            if not unique_index_exists:
                cursor.execute(f"CREATE UNIQUE INDEX idx_unique_ZKID_Timestamp ON {table_name} (ZKID, Timestamp)")
                conn.commit()
        else:
            # Create the 'attendance' table if it does not exist
            cursor.execute(f'''
            CREATE TABLE {table_name} (
                {", ".join(required_columns)},
                UNIQUE(ZKID, Timestamp)
            )
            ''')
            conn.commit()

        conn.close()
    else:
        # Create the 'PUSH.db' database and 'attendance' table if the database does not exist
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f'''
        CREATE TABLE {table_name} (
            {", ".join(required_columns)},
            UNIQUE(ZKID, Timestamp)
        )
        ''')
        conn.commit()
        conn.close()

def run_script(script_instance):
    script_instance.run()

if __name__ == "__main__":
    with open('server.log', 'a') as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            check_db()  # Ensure the database and table exist before running any threads

            logger.info("Starting scripts...")

            s1 = PostScript()
            s2 = Dbcon()
            s3 = TcpServer("devices.json", "attlog.json", "sanatise.json")
            s4 = CmdScriptModule.Cmd("set.json", "devices.json")

            t1 = threading.Thread(target=s1.post_and_update_records)
            t2 = threading.Thread(target=s2.run)
            t3 = threading.Thread(target=s3.run)
            t4 = threading.Thread(target=s4.wait_until_specified_time)

            t1.start()
            t2.start()
            t3.start()
            t4.start()

            t1.join()
            t2.join()
            t3.join()
            t4.join()

            logger.info("Scripts completed.")
