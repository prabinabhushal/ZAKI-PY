import yaml
import argparse
from Extract1st import Extractzip
from ScrubExplode2nd import Scrubzip
from LoadSQL import Loadzip
from pyspark.sql import SparkSession
import logging

# Setup logger
logging.basicConfig(
    level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s'
    )
logger = logging.getLogger("getLogger")


class ExtractTransferLoad:
    def __init__(self):

        with open ("setup.yml",'r')as f:
            f1=yaml.safe_load(f)

       
        self.container1 = f1 ['SPARK']['EXECUTOR']['CORES']
        self.container2 = f1 ['SPARK']['EXECUTOR']['INSTANCES']
        self.container3 = f1 ['SPARK']['EXECUTOR']['MEMORY']
        self.container4 = f1['SPARK']['DRIVER']['MEMORY']
        
        #postgres configgg
        self.port = f1['POSTGRES']['port']
        self.host = f1['POSTGRES']['host']
        self.dbname = f1['POSTGRES']['dbname']
        self.user = f1['POSTGRES']['user']
        self.password= f1['POSTGRES']['password']

    def run(self):
        logger.info("Reading setup.yml configuration file...")
        parser = argparse.ArgumentParser(description="ETL for ZIP file with network and provider data.")
        parser.add_argument("--zip_file", required=True, help="Full path to the ZIP file to process")
        args = parser.parse_args()

        logger.info("Initializing Spark session")
        self.spark = SparkSession.builder\
                .appName('ETL')\
                .config("spark.driver.memory", self.container4).getOrCreate()
        
        logger.info("Initializing ETL process")

        logger.info("Starting extraction")
        net_path, pro_path = Extractzip.extract(args.zip_file)

        logger.info("Starting scrub step")
        net_parquet, pro_parquet = Scrubzip.scrub(net_path, pro_path, self)

        logger.info("Starting load to database")
        Loadzip.load(net_parquet, pro_parquet, self)

         
        logger.info("ETL process is complete")


