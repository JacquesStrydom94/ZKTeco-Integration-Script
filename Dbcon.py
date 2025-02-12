import sqlite3
import logging
import time

logger = logging.getLogger(__name__)

DB_NAME = "PUSH.db"
TABLE_NAME = "attendance"

class Dbcon:
    def __init__(self):
        self.db_name = DB_NAME
        self.table_name = TABLE_NAME

    def check_db(self):
        """Ensure the SQLite database and required columns exist."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
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
                    FTID TEXT,
                    UNIQUE(ZKID, Timestamp)  -- If needed to avoid duplicates
                )
            """)
            conn.commit()

            # Example: if you want to ensure columns exist even after the table is created
            required_columns = [
                "RESPONSE TEXT",
                "KEY TEXT",
                "FTID TEXT"
                # Add more as needed
            ]
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            existing_columns = {info[1] for info in cursor.fetchall()}

            for col_def in required_columns:
                col_name = col_def.split()[0]
                if col_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {col_def}")
                    conn.commit()

            logger.info(f"✅ Database '{self.db_name}' is ready with table '{self.table_name}'.")

        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
        finally:
            if conn:
                conn.close()

    def run(self):
        """
        (Optional) Periodically run housekeeping tasks on the DB.
        Currently, it just sleeps.
        """
        while True:
            # In case you want to do archival or cleanup
            time.sleep(30)
