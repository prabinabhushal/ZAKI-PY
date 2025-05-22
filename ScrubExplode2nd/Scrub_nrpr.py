from pyspark.sql.functions import explode,concat,expr,hash,when,array,col,concat_ws,lpad
from pyspark.sql.types import ArrayType, IntegerType, ShortType,DoubleType

def scrub (nrpr_path,pro_path,etl):
    spark = etl.spark
    nrpr_path = str(nrpr_path)
    pro_path=str(pro_path)

    #Highwark File
    nrpr = spark.read.json(nrpr_path)
    exploded = nrpr.selectExpr("*", "explode(in_network) as n")
    exploded_rates = exploded.selectExpr("*", "explode(n.negotiated_rates) as rate")
    exploded_all = exploded_rates.selectExpr("*", "explode(rate.negotiated_prices) as price")
    exploded_provider = exploded_all.selectExpr("*", "explode(rate.provider_groups) as group")
    exploded_npi = exploded_provider.selectExpr("*", "explode(group.npi) as npi")

    network_explode = exploded_npi.selectExpr(
    "n.billing_code",
    "n.billing_code_type",
    "n.negotiation_arrangement",
    "price.billing_class as billing_class",
    "price.billing_code_modifier as billing_code_modifier",
    "price.negotiated_rate as negotiated_rate",
    "price.negotiated_type as negotiated_type",
    "price.service_code as service_code",
    "npi",
    "group.tin.type as tin_type",
    "group.tin.value as tin"
    )
    network_explode.printSchema()
    network_id=(network_explode.withColumn('tin', expr("REPLACE(tin, '-', '')"))
                      .withColumn('provider_group_id',concat("npi","tin"))
                      .withColumn('provider_group_id',hash('provider_group_id')))
    # network_id.select('npi','tin').distinct().count()
    # network_id.select('provider_group_id').distinct().count()
    nr = network_id.drop("npi","tin_type","tin")
    nr.printSchema() 

    #Select Provider items
    pr=network_id.select("provider_group_id","npi","tin_type","tin")

    #Provider detail
    nrpr_provider= spark.read.parquet(pro_path)
    provider_detail1 = (nrpr_provider.selectExpr("*","loc.lat as latitude","loc.lon as longitude"
                                                 ).drop("loc", "prv_fax", "provider_name_prefix_text", "prv_type_desc")
                                    .withColumn("prv_type_code",when(col("prv_type_code") == "P", 1)
                                                                  .when(col("prv_type_code") == "F", 2)
                                                                  .otherwise(None))
    
                                    .withColumn("provider_full_name", concat_ws(" ", "provider_first_name", "provider_middle_name", "provider_last_name")) 
                                    .withColumn("all_specialties",array('prv_specialty_1_desc', 'prv_specialty_2_desc', 'prv_specialty_3_desc')) 
                                    .withColumn("taxonomy_codes",array('prv_taxonomy_1_code', 'prv_taxonomy_2_code', 'prv_taxonomy_3_code'))
   
                                    .withColumn("taxonomy_codes",expr("filter(taxonomy_codes, x -> x IS NOT NULL AND x != '')")) 
                                    .withColumn("all_specialties",expr("filter(all_specialties, x -> x IS NOT NULL AND x != '')"))
                                    
                                    .drop("provider_first_name", "provider_last_name", "provider_middle_name",
                                          "prv_taxonomy_1_code", "prv_taxonomy_2_code", "prv_taxonomy_3_code",
                                          "prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc")
                                    .withColumn("prv_type_code",col("prv_type_code").cast(ShortType()))
                                    .withColumn("longitude",col("longitude").cast(DoubleType()))
                                    .withColumn("latitude",col("latitude").cast(DoubleType()))
                                    .withColumn("tin",col("tin").cast(ShortType())))
    provider_detail1.printSchema()
    #Network_scrubing
    network= nr.filter(nr.billing_code.isNotNull() & (nr.billing_code != ""))
    network_nr = network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType()))) 
    network_nr.printSchema()

    df = spark.read.csv("billing_taxonomy_list.csv", header=True, inferSchema=True)
    df.printSchema()

    df2 = (df.withColumn("billing_code", lpad(col("billing_code"), 5, "0"))
      .filter((col("billing_code").isNotNull()) & (col("billing_code") != ""))
      .drop("_c4", "_c5", "_c6")
    )
    df2.printSchema()
    df3 = df2.select('billing_code','taxonomy_list')
    df3.printSchema()


    return pr,provider_detail1,network_nr,df3,df2