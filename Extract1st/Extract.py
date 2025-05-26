
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
from ScrubExplode2nd import Scrub_nr_pr
from LoadSQL import Load_nr_pr

def main ():
    #  zip_file = sys.argv[1]
 
    parser = argparse.ArgumentParser(description="ETL for ZIP file with network and provider data.") #argumentParse object
    parser.add_argument("--zip_file",required=True, help="Full path to the ZIP file to process")  #optional argument
    args = parser.parse_args()
   

    net_path,pro_path= Extractzip.extract(args.zip_file)
    net_parquet,pro_parquet=Scrub_nr_pr.scrub(net_path,pro_path)
    Load_nr_pr.load(net_parquet,pro_parquet)


if __name__ == "__main__":
    main()

# import sys
import argparse
from Extract1st import Extractzip
from ScrubExplode2nd import Scrub_nr_pr
from LoadSQL import Load_nr_pr
from ETL import ExtractTransferLoad
def main ():
    #  zip_file = sys.argv[1]
 
    parser = argparse.ArgumentParser(description="ETL for ZIP file with network and provider data.") #argumentParse object
    parser.add_argument("--zip_file",required=True, help="Full path to the ZIP file to process")  #optional argument
    
    args = parser.parse_args()
   
    etl = ExtractTransferLoad()

    net_path,pro_path= Extractzip.extract(args.zip_file)
    net_parquet,pro_parquet=Scrub_nr_pr.scrub(net_path,pro_path,etl)
    Load_nr_pr.load(net_parquet,pro_parquet,etl)


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
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,array,when,concat_ws
from pyspark.sql.types import ArrayType, IntegerType, ShortType,LongType

def scrub (net_path,pro_path,pdetail_file,etl):
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
    
    provider_final.show(5)

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

    #New provider detail 
    #drop prv_fax, provider_name_prefix_text, prv_type_desc from the provider_detail data
    #prv_type_code value must be mapped to integer, when the value is 'P' map it to 1 and when it is 'F' map it to 2

    provider_detail = spark.read.json(pdetail_file)
    provider_detail.printSchema()

    provider_detail1= provider_detail.select ("*",
        col("loc.lat").alias("latitude"),
        col("loc.lon").alias("longitude")).drop(("loc"),("prv_fax"),("provider_name_prefix_text"),("prv_type_desc"))
    provider_detail1.printSchema()

    provider_detail2 = provider_detail1.withColumn("prv_type_code",
    when(col("prv_type_code") == "P", 1)
    .when(col("prv_type_code") == "F", 2)
    .otherwise(None)
    )



    provider_detail3 = provider_detail2.withColumn("provider_full_name", concat_ws(" ", "provider_first_name", "provider_middle_name", "provider_last_name"))\
                                        .withColumn("all_specialties",array("prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc")) \
                                        .withColumn("taxonomy_codes", array("prv_taxonomy_1_code", "prv_taxonomy_2_code", "prv_taxonomy_3_code"))
    provider_detail4 = provider_detail3.drop(("provider_first_name"),("provider_last_name"),("provider_middle_name"),("prv_taxonomy_1_code"),("prv_taxonomy_2_code"),("prv_taxonomy_3_code"),("prv_specialty_1_desc"),("prv_specialty_2_desc"),("prv_specialty_3_desc"))                                 

    provider_cast2 = provider_detail4.withColumn("npi",col("npi").cast(LongType())) \
              .withColumn("prv_type_code",col("prv_type_code").cast(IntegerType()))

    provider_cast2.printSchema()

    provider_cast2.count()

    provider_cast2.drop_duplicates().count()

    provider_cast2.show(truncate=False)

    provider_table2 = provider_cast.join(
    provider_cast2, provider_cast.npi == provider_cast2.npi, 'inner'
    ).drop(provider_cast2.npi)

    provider_table2.show(truncate=False)

    net_parquet ='output/NetworkScrub.parquet'
    pro_parquet ='output/ProviderScrub.parquet'
    
    in_network_cast.write.mode("overwrite").parquet(net_parquet)
    provider_table2.write.mode("overwrite").parquet(pro_parquet)

    return net_parquet,pro_parquet

import psycopg2

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
    in_provider_table = """
    CREATE TABLE IF NOT EXISTS in_provider_table (
        provider_group_id BIGINT,                
        npi BIGINT,                          
        tin_type SMALLINT,                    
        tin VARCHAR(12),                           
        prv_city VARCHAR(255),                 
        prv_phone VARCHAR(12),                   
        prv_state CHAR(2),                   
        prv_street_1 VARCHAR(255),            
        prv_zip VARCHAR(10),                      
        latitude DOUBLE PRECISION,              
        longitude DOUBLE PRECISION,            
        prv_type_code INT,           
        provider_full_name VARCHAR(255),      
        all_specialties TEXT[],                
        taxonomy_codes TEXT[]                  
    );
    """
    cursor.execute(in_provider_table)
    conn.commit()
    spark2 = spark.read.parquet(pro_parquet)
    spark2.printSchema()
    spark2.write.jdbc(url=jdbc_url, table="in_provider_table", mode="append", properties= conn_properties)
    in_network_table = """
    CREATE TABLE IF NOT EXISTS in_network_table (
        billing_code VARCHAR(10),
        billing_code_type VARCHAR(10),
        negotiation_arrangement VARCHAR(10),
        provider_group_id BIGINT,
        negotiated_type VARCHAR(12),
        negotiated_rate DOUBLE PRECISION,
        billing_class VARCHAR(15),
        billing_code_modifier TEXT[],
        service_code INTEGER[]
    );
    """
    cursor.execute(in_network_table)
    conn.commit()
    spark1 = spark.read.parquet(net_parquet)
    spark1.printSchema()
    spark1.write.jdbc(url=jdbc_url, table="in_network_table", mode="append", properties=conn_properties)

    # import sys
import logging
import argparse
from ETL import ExtractTransferLoad

def main ():
    #  zip_file = sys.argv[1]
    logging.basicConfig(filename='etl.log',
    level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("getLogger")

    parser = argparse.ArgumentParser(description="ETL for ZIP file or api for planet sort")
    parser.add_argument("--zip_file",help="Full path to the ZIP file to process")
    parser.add_argument("--pdetail_file",help="New path of provider detail")


    args = parser.parse_args()
    etl = ExtractTransferLoad()
    etl.run(args,logger)
 
if __name__ == "__main__":
    main()


import yaml
import argparse
from Extract1st import Extractzip
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
        logger.info("Initializing Spark session")
        
        logger.info("Initializing ETL process")

        logger.info("Starting extraction")
        net_path, pro_path = Extractzip.extract(args.zip_file)

        pdetail_file = args.pdetail_file
        
        self.spark = SparkSession.builder.appName('ETL').config("spark.driver.memory", self.container4).getOrCreate()

        logger.info("Starting scrub step")
        net_parquet, pro_parquet = Scrub_nr_pr.scrub(net_path, pro_path,pdetail_file ,self)

        logger.info("Starting load to database")
        Load_nr_pr.load(net_parquet, pro_parquet, self)

        logger.info("ETL process is complete")


import logging

logging.basicConfig(
    filename='etl.log',filemode='a',level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("This is an info message")
logging.warning("This is a warning")
logging.error("This is an error message")
logging.debug("This is an message should go to the log file")

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
 

# # Question:
# # api url = "https://api.le-systeme-solaire.net/rest/bodies/"
# # For all heavenly bodies check if they are parameter with key 'isPlanet'
# # If it is planet
# # get the name of planet with key 'englishName'
# # get the semi_major_axis (initially in km) using key 'semimajorAxis'
# # get the distance using formula semi_major_axis/1000000 (convert km to million km)
# # this distance is the distance from sun
# # populate Parent class that will have two attributes: name, distance
# # Using insertion sort, sort the planets by distance from the sun, display it in ascending or descending order depending on user input
# # user input must be either apiA or apiD - api denotes that the api related code must be executed and the ending A or D denotes the sorting order


import requests
import argparse

class Parent:
    def __init__(self, name, distance_km):
        self.name = name
        self.distance = distance_km  # in million km

    def __str__(self):
        return f"Planet: {self.name}, Distance: {self.distance:.2f} million km"

def new_planets():
    api_url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()

    planets = []
    for body in data['bodies']:
        if body.get('isPlanet') and body.get('semimajorAxis'):
            name = body['englishName']
            distance_million_km = body['semimajorAxis'] / 1000000
            planets.append(Parent(name, distance_million_km))
    return planets

def insertion_sort(planets, ascending=True):
    for i in range(1, len(planets)):
        p = planets[i]
        j = i - 1
        while j >= 0 and (
            (planets[j].distance > p.distance) if ascending else (planets[j].distance < p.distance)
        ):
            planets[j + 1] = planets[j]
            j -= 1
        planets[j + 1] = p
    return planets

def bubble_sort(planets, ascending=True):
    n = len(planets)
    for i in range(n):
        for j in range(0, n - i - 1):
            if (planets[j].distance > planets[j + 1].distance) if ascending else (planets[j].distance < planets[j + 1].distance):
                planets[j], planets[j + 1] = planets[j + 1], planets[j]
    return planets

def main():
    parser = argparse.ArgumentParser(description="Sort planets by distance from the sun.")
    parser.add_argument("--order", choices=["apiA", "apiD"], help="apiA = Ascending | apiD = Descending")
    parser.add_argument("--sort", choices=["bubble", "insertion"], help="Sorting method: bubble or insertion")

    args = parser.parse_args()

    # Ask user if arguments are missing
    if not args.order:
        args.order = input("Enter sorting order (apiA for ascending, apiD for descending): ").strip()
        if args.order not in ["apiA", "apiD"]:
            print("Invalid order input. Exiting.")
            return

    if not args.sort:
        args.sort = input("Enter sorting method (bubble or insertion): ").strip()
        if args.sort not in ["bubble", "insertion"]:
            print("Invalid sort method. Exiting.")
            return

    ascending = args.order == "apiA"
    planets = new_planets()

    if args.sort == "bubble":
        sorted_planets = bubble_sort(planets, ascending)
    else:
        sorted_planets = insertion_sort(planets, ascending)

    direction = "Ascending" if ascending else "Descending"
    print(f"\nPlanets sorted in {direction} order using {args.sort} sort:\n")
    for planet in sorted_planets:
        print(planet)

if __name__ == "__main__":
    main()


#zip file .gz
import ijson
import zipfile
import json
from decimal import Decimal
from pathlib import Path
from io import BytesIO
import gzip

def merge(zip_file):
    output_dir = Path('output_nrpr')
    output_dir.mkdir(exist_ok=True)

    nrpr_path = output_dir / 'combined_nrpr.json'

    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)

    with open(nrpr_path,'w') as nrpr_file:
            with zipfile.ZipFile(zip_file, 'r') as outer_zip:
                for name in outer_zip.namelist():
                    if name.endswith('.json.gz'):
                        with outer_zip.open(name) as zipped_file:
                            data = zipped_file.read()
                            with gzip.open(BytesIO(data), 'rt', encoding='utf-8') as f:
                                for item in ijson.items(f, 'in_network.item'):
                                    item['source'] = 'in_network'
                                    nrpr_file.write(json.dumps(item, default=con_decimal) + '\n')


    return nrpr_path
