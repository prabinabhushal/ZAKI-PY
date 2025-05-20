# Scrub
from pyspark.sql.functions import explode, col,array,when,concat_ws,expr
from pyspark.sql.types import ArrayType, IntegerType, ShortType,LongType
def scrub (net_path,pro_path,pdetail_file,etl):
    spark = etl.spark
    net_path = str(net_path)
    pro_path = str(pro_path)
    
    network = spark.read.json(net_path)
    provider = spark.read.json(pro_path)
    provider_detail = spark.read.json(pdetail_file)
    network.printSchema()
    provider.printSchema()
    
    #Flatten network file use explode
    network_exploded = network.withColumn("rates", explode("negotiated_rates"))
    id_exploded = network_exploded.withColumn("id", explode("rates.provider_references"))
    network_rates = id_exploded.withColumn("prices",explode("rates.negotiated_prices"))
    in_network = network_rates.selectExpr(
    "billing_code",
    "billing_code_type",
    "negotiation_arrangement",
    "id as provider_group_id",
    "prices.negotiated_type as negotiated_type",
    "prices.negotiated_rate as negotiated_rate",
    "prices.billing_class as billing_class",
    "prices.billing_code_modifier as billing_code_modifier",
    "prices.service_code as service_code")
    new_network = in_network.filter(in_network.billing_code.isNotNull() & (in_network.billing_code != ""))
    in_network_cast = new_network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType()))) \
                                 .withColumn("provider_group_id", col("provider_group_id").cast(IntegerType())) 
    in_network_cast.printSchema()
    #provider
    
    provider_exploded = provider.selectExpr("provider_group_id", "explode(provider_groups) as provider")   \
                                .selectExpr("*", "explode(provider.npi) as provider_npi") \
        .selectExpr(
        "provider_group_id",
        "provider_npi as npi",
        "provider.tin.type as tin_type",
        "provider.tin.value as tin"
    )

    provider_renamed=provider_exploded.withColumn("tin_type",when(col("tin_type")=="ein",1)
                                      .when(col("tin_type")== "npi",2))
    # hash_network=remove_network.withColumn('service_code',hash('service_code'))
    in_provider_hyphen = provider_renamed.withColumn("tin",expr("REPLACE(tin,'-','')"))
    #Cast to array of integer
    provider_cast = in_provider_hyphen.withColumn("tin_type",col("tin_type").cast(ShortType()))\
                                      .withColumn("provider_group_id", col("provider_group_id").cast(IntegerType()))\
                                      .withColumn("tin", col("tin").cast(LongType()))
    #new provider detail
    
    provider_detail1 = provider_detail.selectExpr("*","loc.lat as latitude","loc.lon as longitude"
                                                 ).drop("loc", "prv_fax", "provider_name_prefix_text", "prv_type_desc")
    provider_detail2 = provider_detail1.withColumn("prv_type_code",when(col("prv_type_code") == "P", 1)
                                                                  .when(col("prv_type_code") == "F", 2)
                                                                  .otherwise(None))
    provider_detail3 = provider_detail2 \
        .withColumn("provider_full_name", concat_ws(" ", "provider_first_name", "provider_middle_name", "provider_last_name")) \
        .withColumn("all_specialties",array('prv_specialty_1_desc', 'prv_specialty_2_desc', 'prv_specialty_3_desc')) \
        .withColumn("taxonomy_codes",array('prv_taxonomy_1_code', 'prv_taxonomy_2_code', 'prv_taxonomy_3_code'))
    
    provider_detail4 = provider_detail3 \
    .withColumn("taxonomy_codes",expr("filter(taxonomy_codes, x -> x IS NOT NULL AND x != '')")) \
    .withColumn("all_specialties",expr("filter(all_specialties, x -> x IS NOT NULL AND x != '')"))
    
    provider_detail4 = provider_detail4.drop(
    "provider_first_name", "provider_last_name", "provider_middle_name",
    "prv_taxonomy_1_code", "prv_taxonomy_2_code", "prv_taxonomy_3_code",
    "prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc"
    )
    provider_cast2 = provider_detail4.withColumn("npi",col("npi").cast(LongType())) \
                                     .withColumn("prv_type_code",col("prv_type_code").cast(IntegerType()))
   #join two provider table
    provider_table2 = provider_cast.join(provider_cast2, on="npi", how= 'inner')
    left_npi = provider_cast.join(provider_cast2, on="npi", how= 'left_anti')  #make useless npi folder


    net_parquet ='output/NetworkScrub.parquet'
    pro_parquet ='output/ProviderScrub.parquet'
    left_npi_parquet ='output/left_npi.parquet'
    
    in_network_cast.write.mode("overwrite").parquet(net_parquet)
    provider_table2.write.mode("overwrite").parquet(pro_parquet)
    left_npi.write.mode('overwrite').parquet(left_npi_parquet)
    return net_parquet,pro_parquet