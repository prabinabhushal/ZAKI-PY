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
    net_parquet ='output/NetworkScrub.parquet'
    pro_parquet ='output/ProviderScrub.parquet'
    
    in_network_cast.write.mode("overwrite").parquet(net_parquet)
    provider_cast.write.mode("overwrite").parquet(pro_parquet)

    return net_parquet,pro_parquet
