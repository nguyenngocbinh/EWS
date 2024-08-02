import logging
from monitor.giniRetail import giniRetail
from monitor.giniNonRetail import giniNonRetail
from monitor.giniRetailCC import giniRetailCC
from monitor.giniRetailMortgageV2 import giniRetailMortgageV2
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("STARTING RETAIL PROCESSING...")
    giniRetail()
    logger.info("RETAIL PROCESSING COMPLETED.")

    logger.info("STARTING NON-RETAIL PROCESSING...")
    giniNonRetail()
    logger.info("NON-RETAIL PROCESSING COMPLETED.")

    logger.info("STARTING RETAIL CREDITCARD PROCESSING...")
    giniRetailCC()
    logger.info("RETAIL PROCESSING COMPLETED.")

    logger.info("STARTING RETAIL MORTGAGE V2 PROCESSING...")
    giniRetailMortgageV2()
    logger.info("RETAIL MORTGAGE V2 PROCESSING COMPLETED.")