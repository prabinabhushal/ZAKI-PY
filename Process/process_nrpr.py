def process(pr,provider_detail1,network_nr,df3,etl):

    #Join two provider table
    nrpr_provider = pr.join(provider_detail1, on=["npi","tin"], how= 'inner')
    left_npi = pr.join(provider_detail1, on="npi", how= 'left_anti')  #make useless npi folder
    nrpr_provider.printSchema()

    #JOIN NETWORK
    nrpr_network = network_nr.join(df3, on=["billing_code"], how= 'inner')
    nrpr_network.printSchema()


    nr_parquet ='output_nrpr/nr'
    pr_parquet ='output_nrpr/pr'
    left__nrpr_npi_parquet ='output_nrpr/left_npi'
    
    nrpr_network.write.mode("overwrite").parquet(nr_parquet)
    nrpr_provider.write.mode("overwrite").parquet(pr_parquet)
    left_npi.write.mode('overwrite').parquet(left__nrpr_npi_parquet )
    
    return nr_parquet,pr_parquet