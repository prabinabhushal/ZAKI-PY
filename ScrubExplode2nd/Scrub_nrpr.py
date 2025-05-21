from pyspark.sql.functions import explode,concat,expr,hash,when,array,col,concat_ws
from pyspark.sql.types import ArrayType, IntegerType, ShortType,LongType

def scrub (nrpr_path,pro_path,bc,etl):
    spark = etl.spark
    nrpr_path = str(nrpr_path)
    pro_path=str(pro_path)

    #Highwark File
    nrpr = spark.read.json(nrpr_path)
    exploded=nrpr.selectExpr("*", "explode(in_network) as n").select("n.*")
    exploded_rates= exploded.withColumn("rates",explode("negotiated_rates"))
    exploded_all = exploded_rates.withColumn( "prices",explode("rates.negotiated_prices"))
    exploded_provider = exploded_all.withColumn( "groups",explode("rates.provider_groups"))
    exploded_provider = exploded_provider.withColumn( "npi",explode("groups.npi"))

    network_explode= exploded_provider.selectExpr(
        "billing_code",
        "billing_code_type",
        "negotiation_arrangement",
        "prices.billing_class as billing_class",
        "prices.billing_code_modifier as billing_code_modifier",
        "prices.negotiated_rate as negotiated_rate",
        "prices.negotiated_type as negotiated_type",
        "prices.service_code as service_code",
        "npi",
        "groups.tin.type as tin_type",
        "groups.tin.value as tin"
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
    provider=network_id.select("provider_group_id","npi","tin_type","tin")

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
                                    .withColumn("prv_type_code",col("prv_type_code").cast(ShortType())))

   #Join two provider table
    provider_nrpr = provider.join(provider_detail1, on="npi", how= 'inner')
    left_npi = provider.join(provider_detail1, on="npi", how= 'left_anti')  #make useless npi folder
    provider_nrpr.printSchema()

    #Network_scrubing
    nr = network_id.drop("npi","tin_type","tin")
    network = nr.filter(nr.billing_code.isNotNull() & (nr.billing_code != ""))
    in_network_cast = network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType()))) \
                                 .withColumn("provider_group_id", col("provider_group_id").cast(IntegerType())) 
    in_network_cast.printSchema()