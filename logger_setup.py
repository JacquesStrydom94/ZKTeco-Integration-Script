import logging
import os

MAX_LOG_SIZE = 1 * 1024 * 1024 * 1024  # 1GB

class DeduplicationFilter(logging.Filter):
    """Filter to prevent logging duplicate messages consecutively."""
    def __init__(self):
        super().__init__()
        self.last_log = None

    def filter(self, record):
        current_log = record.getMessage()
        if current_log == self.last_log:
            return False
        self.last_log = current_log
        return True

def setup_logger():
    log_file = "server.log"

    # Truncate if large
    if os.path.exists(log_file) and os.path.getsize(log_file) >= MAX_LOG_SIZE:
        with open(log_file, "w"):
            pass
        print("Log file exceeded 1GB and has been cleared.")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    file_handler.addFilter(DeduplicationFilter())

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    console_handler.addFilter(DeduplicationFilter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()
