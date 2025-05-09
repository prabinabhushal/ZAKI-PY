import psycopg2
from pyspark.sql import SparkSession


def load(net_parquet,pro_parquet): 

    import psycopg2
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("PostgresSQLConn") \
        .config("spark.jars", "/home/prabina-bhushal/jars/postgresql-42.5.6.jar") \
        .getOrCreate()
    spark
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    conn_properties = {
        "user" : "postgres",
        "password" : "123",
        "driver" :"org.postgresql.Driver"
    }
    conn = psycopg2.connect(
        port = 5432,
        host = "localhost",
        dbname='postgres',
        user ="postgres",
        password = "123",
    )
    cursor = conn.cursor()
    provider_table = """
    CREATE TABLE IF NOT EXISTS provider_table (
        provider_group_id INT,
        npi INT,
        tin_type SMALLINT,
        tin TEXT 
    );
    """
    cursor.execute(provider_table)
    conn.commit()
    spark2 = spark.read.parquet(pro_parquet)
    spark2.printSchema()
    spark2.write.jdbc(url=jdbc_url, table="provider_table", mode="append", properties= conn_properties)
    in_network_table = """
    CREATE TABLE IF NOT EXISTS in_network (
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
    spark1.write.jdbc(url=jdbc_url, table="in_network", mode="append", properties=conn_properties)