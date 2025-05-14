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
    pprovider_table = """
    CREATE TABLE IF NOT EXISTS pprovider_table (
        provider_group_id INT,
        npi INT,
        tin_type SMALLINT,
        tin TEXT 
    );
    """
    cursor.execute(pprovider_table)
    conn.commit()
    spark2 = spark.read.parquet(pro_parquet)
    spark2.printSchema()
    spark2.write.jdbc(url=jdbc_url, table="pprovider_table", mode="append", properties= conn_properties)
    in_network_table = """
    CREATE TABLE IF NOT EXISTS in_network_table (
        billing_code TEXT,
        billing_code_type TEXT,
        negotiation_arrangement TEXT,
        provider_group_id INT,
        negotiated_type TEXT,
        negotiated_rate DOUBLE PRECISION,
        billing_class TEXT,
        billing_code_modifier TEXT[],
        service_code INTEGER[]
    );
    """
    cursor.execute(in_network_table)
    conn.commit()
    spark1 = spark.read.parquet(net_parquet)
    spark1.printSchema()
    spark1.write.jdbc(url=jdbc_url, table="in_network_table", mode="append", properties=conn_properties)