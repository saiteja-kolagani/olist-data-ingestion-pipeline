
import logging
from pathlib import Path

"""
    - Creating the folder for logs if not exists
    - Logging all the observation in console 
    - Storing the logs in a LOG_FILE file
"""


def setup_logger(log_file: str = "logs/ingest_orders.logs"):
    log_path = Path(log_file)
    log_path.parent.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )