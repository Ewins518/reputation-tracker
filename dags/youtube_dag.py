from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pull_youtube_data import stream_data
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['romeo.ewinsou@um6p.ma'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    'youtube_data',
    default_args=default_args,
    description='Pulling Youtube Data',
    schedule_interval=timedelta(days=15),
    start_date=datetime(year=2023, month=12, day=11), 
    catchup=False,
    tags=["youtube"]
)

#pull_data = PythonOperator(
#                                task_id='pull_youtube_comment',
#                                python_callable=stream_data,
#                                op_kwargs ={'product_name': 'hp'},
#                                dag = dag
#                    )

pull_data = ProduceToTopicOperator(
        task_id="pull_youtube_comment",
        kafka_config_id="kafka_default",
        topic="youtube",
        producer_function=stream_data,
        producer_function_kwargs={
            "product_name": "asus"
        },
)



spark_master = "spark://spark:7077"
spark_app_name = "Spark Hello World"

spark_data = SparkSubmitOperator(
        task_id="spark_job",
        application="/spark/spark_job.py",
        conn_id="spark_default",
        verbose=1,
        dag=dag
    )

pull_data >> spark_data