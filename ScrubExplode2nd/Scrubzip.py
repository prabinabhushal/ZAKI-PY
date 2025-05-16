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
