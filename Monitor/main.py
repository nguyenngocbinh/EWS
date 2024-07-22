import logging
from monitor.retail import retail
from monitor.nonRetail import nonRetail
from monitor.retailCC import retailCC
from monitor.retailMortgageV2 import retailMortgageV2
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("STARTING RETAIL PROCESSING...")
    retail()
    logger.info("RETAIL PROCESSING COMPLETED.")

    logger.info("STARTING NON-RETAIL PROCESSING...")
    nonRetail()
    logger.info("NON-RETAIL PROCESSING COMPLETED.")

    logger.info("STARTING RETAIL CREDITCARD PROCESSING...")
    retailCC()
    logger.info("RETAIL PROCESSING COMPLETED.")

    logger.info("STARTING RETAIL MORTGAGE V2 PROCESSING...")
    retailMortgageV2()
    logger.info("RETAIL MORTGAGE V2 PROCESSING COMPLETED.")