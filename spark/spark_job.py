from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, BooleanType
import logging

import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .master("spark://spark:7077") \
            .appName('SparkDataStreaming') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.read \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafka:9092') \
            .option('encoding', 'UTF-8') \
            .option("delimiter", "\x01") \
            .option('subscribe', 'youtube') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("author", StringType(), False),
        StructField("updated_at", StringType(), False),
        StructField("like_count", IntegerType(), False),
        StructField("text", StringType(), False),
         StructField("video_id", StringType(), False),
        StructField("public", BooleanType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def read_data_from_hdfs():
    # for LLM model and streamlit
    spark_conn = create_spark_connection()
    df = spark_conn.read.parquet("hdfs://namenode:8020/hadoop/hdfs/youtube/data.parquet")
    df.show()


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        selection_df.show()

        selection_df.write.parquet("hdfs://namenode:8020/hadoop/hdfs/youtube1/data4.parquet")

        df = spark_conn.read.parquet("hdfs://namenode:8020/hadoop/hdfs/youtube1/data4.parquet")
        df.show()
