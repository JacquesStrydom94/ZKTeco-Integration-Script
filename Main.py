import logging
import threading
import time

from Post import PostScript
from Dbcon import Dbcon
import Cmd  # This is your new multi-port TCP server script
from logger_setup import logger  # Your improved logging setup

def main():
    logger.info("🚀 Starting services...")

    # 1) Initialize and check the DB schema
    db_manager = Dbcon()
    db_manager.check_db()  # Ensures 'attendance' table/columns exist

    # 2) Start PostScript in a background thread
    #    It continuously fetches new DB records & posts them to your API
    post_script = PostScript()
    post_thread = threading.Thread(target=post_script.run, daemon=True)
    post_thread.start()

    # 3) Optionally, start Dbcon housekeeping in a background thread
    #    (Currently, Dbcon.run() just sleeps; adapt as needed)
    db_thread = threading.Thread(target=db_manager.run, daemon=True)
    db_thread.start()

    # 4) Start the Cmd server in a background thread
    #    Cmd.main() reads Settings.json, spawns a server for each (ip, port),
    #    and listens for device traffic.
    cmd_thread = threading.Thread(target=Cmd.main, daemon=True)
    cmd_thread.start()

    logger.info("✅ Services are now running in background threads.")

    # Keep the main thread alive indefinitely
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🔴 Main thread exiting due to KeyboardInterrupt.")
        # If needed, add cleanup steps here
        pass

if __name__ == "__main__":
    main()
