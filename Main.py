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
from logger_setup import logger  # Import improved logging setup

# Dynamic import of Cmd
spec = importlib.util.spec_from_file_location("CmdScriptModule", "Cmd.py")
CmdScriptModule = importlib.util.module_from_spec(spec)
spec.loader.exec_module(CmdScriptModule)

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
    """Main function to start all services but pause them at the specified time."""
    pause_event = asyncio.Event()  # Event to control pausing services
    pause_event.set()  # Initially allow execution

    with open('server.log', 'a') as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            logger.info("🚀 Starting scripts...")

            check_db()

            settings_file = "Settings.json"

            # Initialize the required services
            s1 = PostScript(settings_file=settings_file)
            s2 = Dbcon()
            s3 = TcpServer(settings_file)
            s4 = CmdScriptModule.Cmd(settings_file, pause_event)  # Pass event to Cmd

            # Start services immediately but allow pausing
            async def run_with_pause(task):
                while True:
                    await pause_event.wait()  # Wait until unpaused
                    await asyncio.to_thread(task)

            # Run all tasks, allowing pausing for PostScript, Dbcon, and TcpServer
            await asyncio.gather(
                run_with_pause(s1.post_and_update_records),
                run_with_pause(s2.run),
                run_with_pause(s3.start_server),
                s4.wait_until_specified_time(),  # Handles pausing/resuming logic
            )

            logger.info("✅ Scripts resumed and running normally.")

if __name__ == "__main__":
    asyncio.run(main())
