import logging
from src.utils.logger.setup_logger import setup_logger
from src.ingest_orders import ingest_orders

#-----------------------------
#Entry Point
#-----------------------------

def main():
    print("Entry Point")
    logger = setup_logger()

    logger = logging.getLogger(__name__)
    logger.info("Starting Olist Order Ingestion")

    valid, invalid = ingest_orders()

    logger.info(f"Valid records   : {len(valid)}")
    logger.info(f"Invalid records : {len(invalid)}")
    logger.info("Orders ingestion completed successfully")

if __name__ == "__main__":
    main()