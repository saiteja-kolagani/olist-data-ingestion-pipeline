"""
    Deduplication logic for orders data.

    Responsibilities:
    - Enforces business-key uniqueness on order_id
    - Retain latest record based on order_purchase_timestamp
    - Track deduplication matrics

    Assumptions:
    - Input records are already validated
    - order_id and order_purchase_timestamp are non-null
"""

from src.utils.logger.setup_logger import setup_logger
from datetime import datetime

logger = setup_logger()

from datetime import datetime

def parse_order_timestamp(ts: str) -> datetime | None:
    """
    Validates and parses order_purchase_timestamp.
    Returns datetime if valid, else None.
    """
    if not ts or not isinstance(ts, str):
        return None

    try:
        return datetime.strptime(ts, "%d-%m-%Y %H:%M")
    except ValueError:
        return None


def deduplication_orders(records: list[dict]) -> list[dict]:
    """
       Deduplicates order records based on order_id.

       Survivorship rule:
       - Keep the record with the latest order_purchase_timestamp

       Returns:
       - List of deduplicated records
       """

    dedup_store = {} # key: order_id, value: best record so far

    for record in records:
        order_id = record["order_id"]
        current_ts = parse_order_timestamp(record["order_purchase_timestamp"])

        if not current_ts:
            logger.warning(f"Invalid timestamp skipped for order_id={record.get('order_id')}")
            continue

        # Case 1: First time seeing this order_id
        if order_id not in dedup_store:
            dedup_store[order_id] = record
            continue

        # Case 2: Duplicate order_id → compare timestamps
        existing_record = dedup_store[order_id]
        existing_ts = parse_order_timestamp(existing_record["order_purchase_timestamp"])

        if current_ts > existing_ts:
            dedup_store[order_id] = record

    total_records = len(records)
    unique_records = len(dedup_store)
    duplicate_records = total_records - unique_records

    logger.info(f"Records before deduplication: {total_records}")
    logger.info(f"Unique orders after deduplication: {unique_records}")
    logger.info(f"Duplicate records removed: {duplicate_records}")

    return list(dedup_store.values())