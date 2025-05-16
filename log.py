import logging

logging.basicConfig(
    filename='etl.log',filemode='a',level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("This is an info message")
logging.warning("This is a warning")
logging.error("This is an error message")
logging.debug("This is an message should go to the log file")

