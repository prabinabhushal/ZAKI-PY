from pyspark.sql.functions import explode, col,array,when,concat_ws,expr
from pyspark.sql.types import ArrayType, IntegerType, ShortType,LongType
import zipfile

def scrub (nrpr_path,pro_path,etl):
    spark = etl.spark
    net_path = str(nrpr_path)
    prov_path=str(pro_path)

    provider=spark.read.parquet(prov_path)
    network=spark.read.json(net_path)

    network.printSchema() 
    provider.printSchema()      
    provider.show(5)
    network.show(5)      