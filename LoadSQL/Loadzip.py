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