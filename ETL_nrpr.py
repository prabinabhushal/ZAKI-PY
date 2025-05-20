import yaml
from Extract1st import Extract_nrpr
from ScrubExplode2nd import Scrub_nrpr
from LoadSQL import Load_nrpr
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
        logger.info("Initializing Spark session")
        
        logger.info("Initializing ETL process")

        logger.info("Starting extraction of nrpr gz file")
        nrpr_path,pro_path= Extract_nrpr.merge(args.zip_file,args.pdetail_file)

        
        self.spark = SparkSession.builder.appName('ETL').config("spark.driver.memory", self.container4).getOrCreate()

        logger.info("Starting scrub of nrpr step")
        Scrub_nrpr.scrub(nrpr_path, pro_path ,self)
    

