from pyspark.sql.functions import explode,concat,expr,hash,when,array,col,concat_ws,lpad,regexp_replace
from pyspark.sql.types import ArrayType, IntegerType, ShortType,DoubleType,LongType

def scrub (nrpr_path,pro_path,etl):
    spark = etl.spark
    nrpr_path = str(nrpr_path)
    pro_path=str(pro_path)

    #Highwark File
    nrpr = spark.read.json(nrpr_path)
    network_explode = (nrpr.selectExpr("*", "explode(in_network) as n").drop("in_network")
                        .select("*", "n.*").drop("n")
                        .selectExpr("*", "explode(negotiated_rates) as rate").drop("negotiated_rates")
                        .selectExpr("*", "explode(rate.provider_groups) as group").drop("provider_groups")
                        .selectExpr("*", "explode(group.npi) as npi","group.tin.type as tin_type","group.tin.value as tin").drop("group")
                        .selectExpr("*", "explode(rate.negotiated_prices) as price").drop("negotiated_prices","rate")
                        .select("*","price.*").drop("price"))
    network_explode.printSchema()

    network_id= (network_explode.withColumn('tin', regexp_replace(col('tin'), '-', ''))
        .withColumn('provider_group_id',hash(concat("npi","tin"))))
    # network_id.select('npi','tin').distinct().count()
    # network_id.select('provider_group_id').distinct().count()
    
    nr = network_id.select("billing_code",
                       "billing_code_type",
                       "negotiation_arrangement",
                       "provider_group_id",
                       "billing_class",
                       "billing_code_modifier",
                       "negotiated_rate",
                       "negotiated_type",
                       "service_code"
                       )
    #Network_scrubing
    network= nr.filter(nr.billing_code.isNotNull() & (nr.billing_code != ""))
    network_nr = network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType()))) 
    network_nr.printSchema()

    #Select Provider items
    pr_df=network_id.select("provider_group_id","npi","tin_type","tin")

    pr = (pr_df.withColumn("tin_type",when(col("tin_type") == "ein", 1)
                        .when(col("tin_type") == "npi", 2))
                        .withColumn("tin_type", col("tin_type").cast(ShortType())) 
                        .withColumn("tin", col("tin").cast(LongType())))

    #Provider detail
    nrpr_provider= spark.read.parquet(pro_path)
    nrpr_provider.printSchema()

    provider_detail1 = (nrpr_provider.selectExpr("*","loc.lat as latitude","loc.lon as longitude")
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
                                          "prv_specialty_1_desc", "prv_specialty_2_desc", "prv_specialty_3_desc",
                                          "loc", "prv_fax", "provider_name_prefix_text", "prv_type_desc")
                                    .withColumn("prv_type_code",col("prv_type_code").cast(ShortType()))
                                    .withColumn("longitude",col("longitude").cast(DoubleType()))
                                    .withColumn("latitude",col("latitude").cast(DoubleType())))                           
    provider_detail1.printSchema()
 

    df = spark.read.csv("billing_taxonomy_list.csv", header=True, inferSchema=True)
    df.printSchema()

    df2 = (df.withColumn("billing_code", lpad(col("billing_code"), 5, "0"))
      .filter((col("billing_code").isNotNull()) & (col("billing_code") != ""))
      .drop("_c4", "_c5", "_c6")
      .withColumn("taxonomy_list",array(regexp_replace(col("taxonomy_list"), r"^\{|\}$", "")))
    )
    df2.printSchema()
    df3 = df2.select('billing_code','taxonomy_list')

    return pr,provider_detail1,network_nr,df3,df2