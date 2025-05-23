import psycopg2

def load(nr_parquet,pr_parquet,df2,etl):
    spark = etl.spark
    port = etl.port
    host = etl.host
    dbname = etl.dbname
    user = etl.user
    password= etl.password
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    conn_properties = {
        "user" : user,
        "password" : password,
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
    cursor.execute("DROP TABLE IF EXISTS pr_table;")
    cursor.execute("DROP TABLE IF EXISTS nr_table;")
    cursor.execute("DROP TABLE IF EXISTS taxonomy.billing_taxonomy;")

    # CREATE EXTENSION postgis;

    pr_table = """
    CREATE TABLE pr_table (
        provider_group_id BIGINT,                
        npi BIGINT,                          
        tin_type SMALLINT,                    
        tin BIGINT,                           
        prv_city VARCHAR,                 
        prv_phone VARCHAR,                   
        prv_state VARCHAR,                   
        prv_street_1 VARCHAR,            
        prv_zip VARCHAR,                      
        latitude DOUBLE PRECISION,              
        longitude DOUBLE PRECISION,            
        prv_type_code INT,           
        provider_full_name VARCHAR,      
        all_specialties TEXT[],                
        taxonomy_codes TEXT[],
        geom GEOGRAPHY(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography)STORED                
    );
    """
    cursor.execute(pr_table)
    conn.commit()
    spark2 = spark.read.parquet(pr_parquet)

    spark2.show(5)
    spark2.write.jdbc(url=jdbc_url, table="pr_table", mode="append", properties= conn_properties)
    nr_table = """
    CREATE TABLE nr_table (
        billing_code VARCHAR,
        billing_code_type VARCHAR,
        negotiation_arrangement VARCHAR,
        provider_group_id BIGINT,
        negotiated_type VARCHAR,
        negotiated_rate DOUBLE PRECISION,
        billing_class VARCHAR,
        billing_code_modifier TEXT[],
        service_code INTEGER[],
        taxonomy_list TEXT[]
         
    );
    """
    cursor.execute(nr_table)
    conn.commit()

    spark1 = spark.read.parquet(nr_parquet)
    spark1.show(5)
    spark1.printSchema()
    spark1.write.jdbc(url=jdbc_url, table="nr_table", mode="append", properties=conn_properties)
 
    cursor.execute("CREATE SCHEMA IF NOT EXISTS taxonomy;")
    conn.commit()
    create_table_query = """
    DROP TABLE IF EXISTS taxonomy.billing_taxonomy;
    CREATE TABLE IF NOT EXISTS taxonomy.billing_taxonomy (
        billing_code VARCHAR(5),
        billing_code_type VARCHAR,
        billing_description TEXT,
        taxonomy_list TEXT[]
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    df2.write.jdbc(url=jdbc_url,table="taxonomy.billing_taxonomy",mode="append", properties=conn_properties)
    cursor.close()