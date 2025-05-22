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
    cursor.execute("DROP TABLE IF EXISTS bill_table;")

    # CREATE EXTENSION postgis;

    pr_table = """
    CREATE TABLE pr_table (
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
        taxonomy_codes TEXT[],
        geos GEOGRAPHY(Point, 4326)                   
    );
    """
    cursor.execute(pr_table)
    conn.commit()
    spark2 = spark.read.parquet(pr_parquet)

    spark2.show(5)
    spark2.write.jdbc(url=jdbc_url, table="pr_table", mode="append", properties= conn_properties)
    nr_table = """
    CREATE TABLE nr_table (
        billing_code VARCHAR(5),
        billing_code_type VARCHAR(10),
        negotiation_arrangement VARCHAR(10),
        provider_group_id BIGINT,
        negotiated_type VARCHAR(12),
        negotiated_rate DOUBLE PRECISION,
        billing_class VARCHAR(15),
        billing_code_modifier TEXT[],
        service_code INTEGER[],
        taxonomy_list VARCHAR,
        geos GEOGRAPHY(Point, 4326)   
    );
    """
    cursor.execute(nr_table)
    conn.commit()
    spark1 = spark.read.parquet(nr_parquet)
    spark1.show(5)
    spark1.printSchema()
    spark1.write.jdbc(url=jdbc_url, table="nr_table", mode="append", properties=conn_properties)

    bill_table = """
    CREATE TABLE bill_table (
        billing_code VARCHAR(5),
        billing_code_type VARCHAR,
        billing_description VARCHAR,
        taxonomy_list VARCHAR,
        geos GEOGRAPHY(Point, 4326) 
    );
    """
    cursor.execute(bill_table)
    conn.commit()
    df2.write.jdbc(url=jdbc_url, table="bill_table", mode="append", properties=conn_properties)