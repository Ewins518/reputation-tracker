from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, BooleanType
import logging
from datetime import datetime
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import sys
import os
import argparse

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.8"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.8"

today_date = datetime.now().strftime("%Y_%m_%d")


def parse_args():
    parser = argparse.ArgumentParser(description="Spark job to process data and save in HDFS")
    parser.add_argument("--product_name", type=str, help="Name of the product", default="DefaultProduct")
    args = parser.parse_args()
    return args


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
        StructField("updated_at", StringType(), False),
        StructField("text", StringType(), False),
        StructField("country", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    sel.show()

    return sel


def write_data_in_hdfs(spark_df, product_name):
    # Use the product name in the path where data is saved
    parquet_path = f"hdfs://namenode:8020/hadoop/hdfs/youtube/{product_name}.parquet"
    spark_df.write.mode('overwrite').parquet(parquet_path)


def read_data_from_hdfs(spark_conn,product_name):
    # for LLM model and streamlit
    df = spark_conn.read.parquet(f"hdfs://namenode:8020/hadoop/hdfs/youtube/{product_name}.parquet")
    return df

#La fonction UDF pour interroger reputation_inference
@udf(IntegerType())
def predict_sentiment(comment):
    url = 'http://reputation_inference:80/predict_sentiment/' + comment
    response = requests.post(url)
    result = response.json()
    print(result)
    return result

# La fonction pour recuperer les sentiments à passer a stremlit
def get_sentiment(df):
    df_with_sentiment = df.withColumn("sentiment", predict_sentiment(df["text"]))
    #retourner seulement la colonne sentiment
    sentiments = df_with_sentiment.select("sentiment")
    return sentiments

#recuperer la colonne text du dataframe
def get_text(df):
    #retourner seulement la colonne text
    texts = df.select("text")
    return texts

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    args = parse_args()

    if spark_conn is not None:
        #connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)


        df_100 = selection_df.limit(100)
        
        df_with_sentiment = df_100.withColumn("sentiment", predict_sentiment(df_100["text"]))

        write_data_in_hdfs(df_with_sentiment, args.product_name)

        df = read_data_from_hdfs(spark_conn,args.product_name)
        print("Let show data from HDFS") 
        df.show()
