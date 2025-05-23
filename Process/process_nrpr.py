from pyspark.sql.functions import  array_intersect, size,col
def process(pr,provider_detail1,network_nr,df3,logger):

    #Join two provider table

    nrpr_pr = pr.join(provider_detail1,on =["npi","tin"],how= "inner")
    left_npi = pr.join(provider_detail1,on ="npi",how= "left_anti")

    nrpr_pr.show(5)

    #Join network table
    nrpr_network = network_nr.join(df3, on="billing_code", how= 'inner')
    nrpr_network.printSchema()
  
    #Join nr and pr table 

    logger.info("After joining nr and pr, nrpr_final is filtered to keep only rows with matching taxonomy codes.")
    nrpr_final = nrpr_pr.join(nrpr_network ,on ="provider_group_id",how = "inner" )
    specialized_filter = nrpr_final.filter(size(array_intersect(col("taxonomy_codes"), col("taxonomy_list"))) > 0)
    specialized_filter.show(5)
    
    nr_parquet ='output_nrpr/nr'
    pr_parquet ='output_nrpr/pr'
    left__nrpr_npi_parquet ='output_nrpr/left_npi'
    
    nrpr_network.write.mode("overwrite").parquet(nr_parquet)
    nrpr_pr.write.mode("overwrite").parquet(pr_parquet)
    left_npi.write.mode('overwrite').parquet(left__nrpr_npi_parquet )
    
    return nr_parquet,pr_parquet