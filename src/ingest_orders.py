"""
    Ingestion script for Orders data.

    Responsibilities:
    - Ingests orders data from a raw CSV file
    - Validates the presence of required columns
    - Validates non-null values in critical fields
    - Separates records into valid and invalid buckets
    - Logs ingestion metrics and observations

No deduplication.
No transformations.
"""

#-----------------------------
# Importing required packages
#-----------------------------

import csv
from src.utils.logger.setup_logger import setup_logger
from pathlib import Path

#-----------------------------
# Configuration
#-----------------------------

"""
    - Storing the input file i.e., raw CSV file path in a DATA_FILE variable.
    - Storing the logs file path in a LOG_FILE variable.
    - Defining the required column names and storing in REQUIRED_COLUMNS variable as Set.
"""

DATA_FILE = Path("data/raw/olist_orders_dataset.csv")

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_purchase_timestamp"
}

#-----------------------------
# Logging
#-----------------------------

logger = setup_logger()

#-----------------------------
# Helper Functions
#-----------------------------

def validate_columns(fieldnames: list):
    """
        - The function validate_columns() validates the presence of required columns in input file i.e., CSV file
        - If the columns are not available then raises an error with the missing column names
    """
    missing_columns = REQUIRED_COLUMNS - set(fieldnames)

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise ValueError("CSV schema validation failed")

    logger.info("All required columns are present")


def is_valid_record(row: dict) -> bool:
    """
        - The function is_valid_record() validates each record whether it is valid or invalid
        - it identifies the null values in the critical fields
        - It quarantines the invalid records
    """
    for column in REQUIRED_COLUMNS:
        if not row.get(column):
            return False
    return True

#-----------------------------
# Ingestion Logic
#-----------------------------

def ingest_orders():

    print("Ingestion Logic Started")

    """
        - Checks presence of CSV file
        - Opens the CSV file safely
        - Reads the records converting into Dict
        - Quarantine bad data
        - Logging metrics
        - Returns valid & invalid records
    """
    if not DATA_FILE.exists():
        logger.error(f"CSV file is not found: {DATA_FILE}")
        raise FileNotFoundError(DATA_FILE)

    valid_records = []
    invalid_records = []

    with DATA_FILE.open(mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        validate_columns(reader.fieldnames)

        total_records = 0

        for row in reader:
            total_records += 1

            if is_valid_record(row):
                valid_records.append(row)
            else:
                invalid_records.append(row)


    logger.info(f"Total records: {total_records}")

    return valid_records, invalid_records



