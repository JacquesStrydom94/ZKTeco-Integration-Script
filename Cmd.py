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

async def main():
    """Main function to start all services with execution pausing until conditions are met."""
    event = asyncio.Event()  # Event to pause and resume execution

    with open('server.log', 'a') as log_file:
        with redirect_stdout(log_file), redirect_stderr(log_file):
            logger.info("Starting scripts...")
            settings_file = "Settings.json"

            s1 = PostScript(settings_file=settings_file)
            s2 = Dbcon()
            s3 = TcpServer(settings_file)
            s4 = CmdScriptModule.Cmd(settings_file, event)  # Pass event to Cmd

            # Wait until conditions are met
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
