
# Extract

import ijson
import json
import zipfile
from decimal import Decimal 


def extract(zip_file):
    data=[]
    rate=[]

    def con_decimal(obj):
     if isinstance(obj, Decimal):
            return float(obj)

    with zipfile.ZipFile(zip_file, 'r') as z:
        for name in z.namelist():
            with z.open(name,'r') as f:
                    for item in ijson.items(f, 'in_network.item'):
                        data.append(item)
            with z.open(name,'r') as f1:
                        for item1 in ijson.items(f1, 'provider_references.item'):
                             rate.append(item1)

    with open('/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/in_network_data.json', 'w') as out_file1:
        json.dump(data, out_file1, default=con_decimal)
        f.write('\n')

    with open('/home/prabina-bhushal/Desktop/PrabinaZaki/3ETL-prabinaBhushal/IgnoreFolder/provider_references_data.json', 'w') as out_file2:
        json.dump(rate, out_file2)
        f1.write('\n')



#main

# import sys
import argparse
from Extract1st import Extractzip
from ScrubExplode2nd import Scrubzip
from LoadSQL import Loadzip

def main ():
    #  zip_file = sys.argv[1]
 
    parser = argparse.ArgumentParser(description="ETL for ZIP file with network and provider data.") #argumentParse object
    parser.add_argument("--zip_file",required=True, help="Full path to the ZIP file to process")  #optional argument
    args = parser.parse_args()
   

    net_path,pro_path= Extractzip.extract(args.zip_file)
    net_parquet,pro_parquet=Scrubzip.scrub(net_path,pro_path)
    Loadzip.load(net_parquet,pro_parquet)


if __name__ == "__main__":
    main()

# import sys
import argparse
from Extract1st import Extractzip
from ScrubExplode2nd import Scrubzip
from LoadSQL import Loadzip
from ETL import ExtractTransferLoad
def main ():
    #  zip_file = sys.argv[1]
 
    parser = argparse.ArgumentParser(description="ETL for ZIP file with network and provider data.") #argumentParse object
    parser.add_argument("--zip_file",required=True, help="Full path to the ZIP file to process")  #optional argument
    
    args = parser.parse_args()
   
    etl = ExtractTransferLoad()

    net_path,pro_path= Extractzip.extract(args.zip_file)
    net_parquet,pro_parquet=Scrubzip.scrub(net_path,pro_path,etl)
    Loadzip.load(net_parquet,pro_parquet,etl)


if __name__ == "__main__":
    main()

#yml
        SPARK:
          EXECUTOR:
            CORES: 8           #lscpu | grep "^CPU(s):"
            INSTANCES: 1
            MEMORY: 1g         #grep -i "executor" ~/spark/conf/*
          DRIVER:
            MEMORY: 4g         #grep -iR "spark.driver.memory" .
        
        
        
        
        POSTGRES:
          port : 5432
          host : 'localhost'
          dbname : 'postgres'
          user : 'postgres'
          password : '123'
    
    SPARK:
  EXECUTOR:
    CORES: 8           #lscpu | grep "^CPU(s):"
    INSTANCES: 1
    MEMORY: 1g         #grep -i "executor" ~/spark/conf/*
  DRIVER:
    MEMORY: 4g         #grep -iR "spark.driver.memory" .

  


POSTGRES:
  port : 5432
  host : 'localhost'
  dbname : 'postgres'
  user : 'postgres'
  password : '123'
 


        
        #extract
        import ijson
        import zipfile
        import json
        from decimal import Decimal
        from pathlib import Path
        
        def extract(zip_file):
            script_dir = Path(__file__).resolve().parent
        
            output_dir = script_dir.parent / 'output'
        
            output_dir.mkdir(exist_ok=True)
        
            network = []
            provider = []
        
            def con_decimal(obj):
                if isinstance(obj, Decimal):
                    return float(obj)
        
            with zipfile.ZipFile(zip_file, 'r') as z:
                for name in z.namelist():
                    if name.endswith('.json'):
                        with z.open(name) as f:
                            for item in ijson.items(f, 'provider_references.item'):
                                provider.append(item)
        
                        with z.open(name) as f:
                            for item in ijson.items(f, 'in_network.item'):
                                network.append(item)
        
            net_path = output_dir / 'in_network.json'
            pro_path = output_dir / 'provider_table.json'
        
            with open(net_path, 'w') as f1:
                json.dump(network, f1, indent=4, default=con_decimal)
        
            with open(pro_path, 'w') as f2:
                json.dump(provider, f2, indent=4, default=con_decimal)
        
            return net_path, pro_path

        import ijson
import zipfile
import json
from decimal import Decimal
from pathlib import Path

def extract(zip_file):
    script_dir = Path(__file__).resolve().parent

    output_dir = script_dir.parent / 'output'

    output_dir.mkdir(exist_ok=True)

    network = []
    provider = []

    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)

    with zipfile.ZipFile(zip_file, 'r') as z:
        for name in z.namelist():
            if name.endswith('.json'):
                with z.open(name) as f:
                    for item in ijson.items(f, 'provider_references.item'):
                        provider.append(item)

                with z.open(name) as f:
                    for item in ijson.items(f, 'in_network.item'):
                        network.append(item)

    net_path = output_dir / 'in_network.json'
    pro_path = output_dir / 'provider_table.json'

    with open(net_path, 'w') as f1:
        json.dump(network, f1, indent=4, default=con_decimal)

    with open(pro_path, 'w') as f2:
        json.dump(provider, f2, indent=4, default=con_decimal)

    return net_path, pro_path

        
        # Scrub
        
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import explode, col,array,when
        from pyspark.sql.types import ArrayType, IntegerType, ShortType
        
        def scrub (net_path,pro_path):
            spark = SparkSession.builder\
            .appName('ETL')\
                .config("spark.driver.memory", "4g").getOrCreate()
            spark
        
            net_path = str(net_path)
            pro_path = str(pro_path)
        
            network = spark.read.option("multiline", "true").json(net_path)
            network.printSchema()
        
        
            network_exploded = network.withColumn("rates", explode("negotiated_rates"))
            id_exploded = network_exploded.withColumn("id", explode("rates.provider_references"))
            network_rates = id_exploded.withColumn("prices",explode("rates.negotiated_prices"))
        
            in_network= network_rates.select(
            "billing_code",
            "billing_code_type",
            "negotiation_arrangement",
            col("id").alias("provider_group_id"),
            col("prices.negotiated_type").alias("negotiated_type"),
            col("prices.negotiated_rate").alias("negotiated_rate"),
            col("prices.billing_class").alias("billing_class"),
            col("prices.billing_code_modifier").alias("billing_code_modifier"),
            col("prices.service_code").alias("service_code")
            )
        
        
            in_network.printSchema()
        
            in_network.show(10)
        
            provider = spark.read.option("multiline","true").json(pro_path)
            provider.printSchema()
        
            provider_exploded = provider.withColumn("provider", explode("provider_groups"))
            provider_npi = provider_exploded.withColumn("npi", explode("provider.npi"))
        
            in_provider = provider_npi.select(
            col("provider_group_id"),
            col("npi").alias("npi"),
            col("provider.tin.type").alias("tin_type"),
            col("provider.tin.value").alias("tin")
            )
        
            in_provider.show(truncate=False)
        
            provider_final=in_provider.withColumn("tin_type",when(col("tin_type")=="ein",1)
                                              .when(col("tin_type")== "npi",2))
            
            provider_final.show()
        
        
            #Remove null value
        
        
            from pyspark.sql.functions import expr
        
            in_network.count()
        
            provider_final.count()
        
            in_network.drop_duplicates().count()
        
            provider_final.drop_duplicates().count()
        
            new_network=in_network.filter(in_network.billing_code.isNotNull())
        
        
            # hash_network=remove_network.withColumn('service_code',hash('service_code'))
        
        
            in_provider_hyphen = provider_final.withColumn("tin",expr("REPLACE(tin,'-','')"))
        
            new_network.filter(in_network.billing_code_modifier.isNotNull()).count()
        
            # in_network_renamed=hash_network.withColumnRenamed('billing_class','bcIs')\
            #             .withColumnRenamed('billing_code','bC')\
            #             .withColumnRenamed('billing_code_type','bCT')\
            #             .withColumnRenamed('negotiated_rate','negR')\
            #                .withColumnRenamed('negotiated_type','negT')\
            #             .withColumnRenamed('negotiation_arrangement','negA')\
            #             .withColumnRenamed('service_code','poSH')
            # in_network_renamed.show()
        
            new_network.printSchema()
        
            #Cast to array of integer
        
            provider_cast = in_provider_hyphen.withColumn("tin_type",col("tin_type").cast(ShortType()))
        
        
            provider_cast.printSchema()
        
            in_network_cast = new_network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))
        
            in_network_cast.printSchema()
            in_network_cast.show(5)
            net_parquet ='output/NetworkScrub.parquet'
            pro_parquet ='output/ProviderScrub.parquet'
            
            in_network_cast.write.mode("overwrite").parquet(net_parquet)
            provider_cast.write.mode("overwrite").parquet(pro_parquet)
        
            return net_parquet,pro_parquet
    
    # Scrub
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,array,when
from pyspark.sql.types import ArrayType, IntegerType, ShortType

def scrub (net_path,pro_path,etl):
    spark = etl.spark


    net_path = str(net_path)
    pro_path = str(pro_path)

    network = spark.read.option("multiline", "true").json(net_path)
    network.printSchema()

    #Flatten network file
    network_exploded = network.withColumn("rates", explode("negotiated_rates"))
    id_exploded = network_exploded.withColumn("id", explode("rates.provider_references"))
    network_rates = id_exploded.withColumn("prices",explode("rates.negotiated_prices"))

    in_network= network_rates.select(
    "billing_code",
    "billing_code_type",
    "negotiation_arrangement",
    col("id").alias("provider_group_id"),
    col("prices.negotiated_type").alias("negotiated_type"),
    col("prices.negotiated_rate").alias("negotiated_rate"),
    col("prices.billing_class").alias("billing_class"),
    col("prices.billing_code_modifier").alias("billing_code_modifier"),
    col("prices.service_code").alias("service_code")
    )


    in_network.printSchema()

    in_network.show(10)

    provider = spark.read.option("multiline","true").json(pro_path)
    provider.printSchema()

    provider_exploded = provider.withColumn("provider", explode("provider_groups"))
    provider_npi = provider_exploded.withColumn("npi", explode("provider.npi"))

    in_provider = provider_npi.select(
    col("provider_group_id"),
    col("npi").alias("npi"),
    col("provider.tin.type").alias("tin_type"),
    col("provider.tin.value").alias("tin")
    )

    in_provider.show(truncate=False)

    provider_final=in_provider.withColumn("tin_type",when(col("tin_type")=="ein",1)
                                      .when(col("tin_type")== "npi",2))
    
    provider_final.show()


    #Remove null value
    from pyspark.sql.functions import expr

    in_network.count()

    provider_final.count()

    in_network.drop_duplicates().count()

    provider_final.drop_duplicates().count()

    new_network=in_network.filter(in_network.billing_code.isNotNull())


    # hash_network=remove_network.withColumn('service_code',hash('service_code'))


    in_provider_hyphen = provider_final.withColumn("tin",expr("REPLACE(tin,'-','')"))

    new_network.filter(in_network.billing_code_modifier.isNotNull()).count()

    # in_network_renamed=hash_network.withColumnRenamed('billing_class','bcIs')\
    #             .withColumnRenamed('billing_code','bC')\
    #             .withColumnRenamed('billing_code_type','bCT')\
    #             .withColumnRenamed('negotiated_rate','negR')\
    #                .withColumnRenamed('negotiated_type','negT')\
    #             .withColumnRenamed('negotiation_arrangement','negA')\
    #             .withColumnRenamed('service_code','poSH')
    # in_network_renamed.show()

    new_network.printSchema()

    #Cast to array of integer

    provider_cast = in_provider_hyphen.withColumn("tin_type",col("tin_type").cast(ShortType()))


    provider_cast.printSchema()

    in_network_cast = new_network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))

    in_network_cast.printSchema()
    in_network_cast.show(5)
    net_parquet ='output/NetworkScrub.parquet'
    pro_parquet ='output/ProviderScrub.parquet'
    
    in_network_cast.write.mode("overwrite").parquet(net_parquet)
    provider_cast.write.mode("overwrite").parquet(pro_parquet)

    return net_parquet,pro_parquet

        
        #load
        import psycopg2
        from pyspark.sql import SparkSession
        
        
        def load(net_parquet,pro_parquet): 
        
            import psycopg2
            from pyspark.sql import SparkSession
            spark = SparkSession.builder \
                .appName("PostgresSQLConn") \
                .getOrCreate()
            spark
            jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
            conn_properties = {
                "user" : "postgres",
                "password" : "123",
                "driver" :"org.postgresql.Driver"
            }
            conn = psycopg2.connect(
                port = 5432,
                host = "localhost",
                dbname='postgres',
                user ="postgres",
                password = "123",
            )
            cursor = conn.cursor()
            pprovider_table = """
            CREATE TABLE IF NOT EXISTS pprovider_table (
                provider_group_id INT,
                npi INT,
                tin_type SMALLINT,
                tin TEXT 
            );
            """
            cursor.execute(pprovider_table)
            conn.commit()
            spark2 = spark.read.parquet(pro_parquet)
            spark2.printSchema()
            spark2.write.jdbc(url=jdbc_url, table="pprovider_table", mode="append", properties= conn_properties)
            in_network_table = """
            CREATE TABLE IF NOT EXISTS in_network_table (
                billing_code TEXT,
                billing_code_type TEXT,
                negotiation_arrangement TEXT,
                provider_group_id INT,
                negotiated_type TEXT,
                negotiated_rate DOUBLE PRECISION,
                billing_class TEXT,
                billing_code_modifier TEXT[],
                service_code INTEGER[]
            );
            """
            cursor.execute(in_network_table)
            conn.commit()
            spark1 = spark.read.parquet(net_parquet)
            spark1.printSchema()
            spark1.write.jdbc(url=jdbc_url, table="in_network_table", mode="append", properties=conn_properties)
        
        
        import psycopg2
import yaml
from pyspark.sql import SparkSession


def load(net_parquet,pro_parquet,etl):
    spark = etl.spark
    port = etl.port
    host = etl.host
    dbname = etl.dbname
    user = etl.user
    password= etl.password

    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    conn_properties = {
        "user" : "postgres",
        "password" : "123",
        "driver" :"org.postgresql.Driver"
    }
    conn = psycopg2.connect(
        port = port,
        host = host,
        dbname= dbname,
        user = user,
        password = password,
    )
    cursor = conn.cursor()
    pprovider_table = """
    CREATE TABLE IF NOT EXISTS pprovider_table (
        provider_group_id INT,
        npi INT,
        tin_type SMALLINT,
        tin TEXT 
    );
    """
    cursor.execute(pprovider_table)
    conn.commit()
    spark2 = spark.read.parquet(pro_parquet)
    spark2.printSchema()
    spark2.write.jdbc(url=jdbc_url, table="pprovider_table", mode="append", properties= conn_properties)
    in_network_table = """
    CREATE TABLE IF NOT EXISTS in_network_table (
        billing_code TEXT,
        billing_code_type TEXT,
        negotiation_arrangement TEXT,
        provider_group_id INT,
        negotiated_type TEXT,
        negotiated_rate DOUBLE PRECISION,
        billing_class TEXT,
        billing_code_modifier TEXT[],
        service_code INTEGER[]
    );
    """
    cursor.execute(in_network_table)
    conn.commit()
    spark1 = spark.read.parquet(net_parquet)
    spark1.printSchema()
    spark1.write.jdbc(url=jdbc_url, table="in_network_table", mode="append", properties=conn_properties)



    #etl.py
    import yaml
from pyspark.sql import SparkSession

class ExtractTransferLoad:
    def __init__(self):
    
        with open ("setup.yml",'r')as f:
            f1=yaml.safe_load(f)

       
        self.container1 = f1 ['SPARK']['EXECUTOR']['CORES']
        self.container2 = f1 ['SPARK']['EXECUTOR']['INSTANCES']
        self.container3 = f1 ['SPARK']['EXECUTOR']['MEMORY']
        self.container4 = f1['SPARK']['DRIVER']['MEMORY']
        
        self.port = f1['POSTGRES']['port']
        self.host = f1['POSTGRES']['host']
        self.dbname = f1['POSTGRES']['dbname']
        self.user = f1['POSTGRES']['user']
        self.password= f1['POSTGRES']['password']
        self.spark = SparkSession.builder\
                .appName('ETL')\
                .config("spark.driver.memory", self.container4).getOrCreate()