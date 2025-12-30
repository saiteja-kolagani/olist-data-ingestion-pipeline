
import logging
from pathlib import Path

"""
    - Creating the folder for logs if not exists
    - Logging all the observation in console 
    - Storing the logs in a LOG_FILE file
"""


# def setup_logger(log_file: str = "logs/ingest_orders.log"):
#     log_path = Path(log_file)
#     log_path.parent.mkdir(exist_ok=True)
#
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s | %(levelname)s | %(message)s",
#         handlers=[
#             logging.FileHandler(log_path),
#             logging.StreamHandler()
#         ]
#     )

def setup_logger(log_path: str = "logs/ingest_orders.log"):
    log_file = Path(log_path)
    log_file.parent.mkdir(exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger