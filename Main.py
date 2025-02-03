import asyncio
import sqlite3
import os
import logging
import psutil
from datetime import datetime
from Post import PostScript
from Dbcon import Dbcon
from TcpServer import TcpServer
import sys
from contextlib import redirect_stdout, redirect_stderr
import importlib.util

spec = importlib.util.spec_from_file_location("CmdScriptModule", "Cmd.py")
CmdScriptModule = importlib.util.module_from_spec(spec)
spec.loader.exec_module(CmdScriptModule)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[
    logging.StreamHandler(sys.stdout)
])
logger = logging.getLogger()

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
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        result = cursor.fetchone()
        if result:
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [info[1] for info in cursor.fetchall()]
            for column_def in required_columns:
                column_name = column_def.split()[0]
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")
                    conn.commit()
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            unique_index_exists = any('unique' in index[1].lower() for index in indexes)
            if not unique_index_exists:
                cursor.execute(f"CREATE UNIQUE INDEX idx_unique_ZKID_Timestamp ON {table_name} (ZKID, Timestamp)")
                conn.commit()
        else:
            cursor.execute(f'''
            CREATE TABLE {table_name} (
                {", ".join(required_columns)},
                UNIQUE(ZKID, Timestamp)
            )
            ''')
            conn.commit()
        conn.close()
    else:
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

async def main():
    with open('server.log', 'a') as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            check_db()
            logger.info("Starting scripts...")
            settings_file = "Settings.json"
            s1 = PostScript(settings_file=settings_file)
            s2 = Dbcon()
            s3 = TcpServer(settings_file)
            s4 = CmdScriptModule.Cmd(settings_file)

            await asyncio.gather(
                asyncio.to_thread(s1.post_and_update_records),
                asyncio.to_thread(s2.run),
                asyncio.to_thread(s3.start_server),
                asyncio.to_thread(s4.wait_until_specified_time)
            )
            
            logger.info("Scripts completed.")

if __name__ == "__main__":
    asyncio.run(main())
