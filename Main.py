import asyncio
import sqlite3
import os
import logging
import sys
from contextlib import redirect_stdout, redirect_stderr
import importlib.util
from Post import PostScript
from Dbcon import Dbcon
from TcpServer import TcpServer

# Dynamic import of Cmd
spec = importlib.util.spec_from_file_location("CmdScriptModule", "Cmd.py")
CmdScriptModule = importlib.util.module_from_spec(spec)
spec.loader.exec_module(CmdScriptModule)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[
    logging.FileHandler("server.log"),
    logging.StreamHandler(sys.stdout)
])
logger = logging.getLogger()

def check_db():
    """Ensure the SQLite database and required tables exist."""
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

    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if cursor.fetchone():
            # Ensure required columns exist
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = {info[1] for info in cursor.fetchall()}
            for column_def in required_columns:
                column_name = column_def.split()[0]
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")
                    conn.commit()

            # Ensure unique index exists
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = {index[1].lower() for index in cursor.fetchall()}
            if "idx_unique_zkid_timestamp" not in indexes:
                cursor.execute(f"CREATE UNIQUE INDEX idx_unique_ZKID_Timestamp ON {table_name} (ZKID, Timestamp)")
                conn.commit()
        else:
            # Create table if it doesn't exist
            cursor.execute(f'''
            CREATE TABLE {table_name} (
                {", ".join(required_columns)},
                UNIQUE(ZKID, Timestamp)
            )
            ''')
            conn.commit()

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
    finally:
        conn.close()

async def main():
    """Main function to start all services with execution pausing until conditions are met."""
    event = asyncio.Event()  # Event to control pausing and resuming execution

    with open('server.log', 'a') as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            logger.info("Starting scripts...")
            
            # Check if the database exists and set up tables if needed
            check_db()

            settings_file = "Settings.json"

            # Initialize the required services
            s1 = PostScript(settings_file=settings_file)
            s2 = Dbcon()
            s3 = TcpServer(settings_file)
            s4 = CmdScriptModule.Cmd(settings_file, event)  # Pass event to Cmd

            # Wait until the specified time and conditions are met before starting other processes
            await s4.wait_until_specified_time()

            # After conditions are met, resume execution
            await asyncio.gather(
                asyncio.to_thread(s1.post_and_update_records),
                asyncio.to_thread(s2.run),
                asyncio.to_thread(s3.start_server),
            )

            logger.info("Scripts resumed and running normally.")

if __name__ == "__main__":
    asyncio.run(main())
