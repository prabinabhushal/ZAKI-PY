import yaml
import argparse
from Extract1st import Extract_nr_pr
from ScrubExplode2nd import Scrub_nr_pr
from LoadSQL import Load_nr_pr
from pyspark.sql import SparkSession

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

    def run(self,args,logger):
        logger.info("Initializing ETL process")

        logger.info("Starting extraction")
        net_path, pro_path = Extract_nr_pr.extract(args.zip_file)

        pdetail_file = args.pdetail_file

        logger.info("Initializing Spark session")        
        self.spark = SparkSession.builder.appName('ETL').config("spark.driver.memory", self.container4).getOrCreate()

        logger.info("Starting scrub step")
        net_parquet, pro_parquet = Scrub_nr_pr.scrub(net_path, pro_path,pdetail_file ,self)

        logger.info("Starting load to database")
        Load_nr_pr.load(net_parquet, pro_parquet, self)

        logger.info("ETL process is complete")


