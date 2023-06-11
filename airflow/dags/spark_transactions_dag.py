import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
import pickle
from airflow.decorators import dag, task
import os

logger = logging.getLogger(__name__)

URL = os.environ.get('URL'),
DRIVER = os.environ.get('DRIVER'),
DBTABLE = os.environ.get('DBTABLE'),
USER = os.environ.get('USER'),
PASS = os.environ.get('PASSWORD')
filepath = os.environ.get('FILE_PATH_2')

def create_spark_session():
        """Function to create a spark session
        """
        return SparkSession.builder.config("spark.jars","mysql-connector-j-8.0.33.jar").master("local[*]").getOrCreate()


def read_frm_source(filepath,spark):
        """Function read data from source

        Args:
            filepath: source data file
            spark: spark session
        """
        df = spark.read.json(filepath)
        df = df.toJSON().collect()
        return df

    
def deserialize_spark_df(df,spark):
        """Function deserialize dataframe

        Args:
            df: dataframe
            spark: spark session
        """
        df = spark.read.json(spark.sparkContext.parallelize(eval(df)))
        return df
    
def transform_df(**dataframe):
        """Function to preform dataframe transformation
        """
        try:
                data = dict(dataframe.items())
                df = data['df']
                spark = data['spark']

                df = deserialize_spark_df(df,spark)
                
                transactions_exploded = df.select(F.col('UniqueId')
                        ,F.col("TransactionDateUTC")
                        ,F.col("Itinerary")
                        ,F.col("OriginAirportCode")
                        ,F.col("DestinationAirportCode")
                        ,F.col("OneWayOrReturn")
                        ,F.explode(F.col('Segment')))\
                .select("UniqueId"
                        ,"TransactionDateUTC"
                        ,"Itinerary"
                        ,"OriginAirportCode"
                        ,"DestinationAirportCode"
                        ,"OneWayOrReturn"
                        ,"col.*")
                return transactions_exploded.toJSON().collect()
        except Exception as e:
                logger.info("ERROR during data transfomration",e)

def insert_into_mysql(**kwargs):
        """Function load data into mysql
        """
        try:
                data = dict(kwargs.items())
                URL = data['URL']
                DRIVER = data['DRIVER']
                DBTABLE = data['DBTABLE']
                USER = data['USER']
                PASS = data['PASS']
                
                df = data['df']
                spark = data['spark']
                
                df = deserialize_spark_df(df,spark)
                df.write.format('jdbc').options(
                        url=URL,
                        driver=DRIVER,
                        dbtable=DBTABLE,
                        user=USER,
                        password=PASS).mode('append').save()
        except Exception as e:
                logger.info("ERROR while loading data",e)


spark = create_spark_session()
with DAG(
        'spark_data_pipline'
        , description='Transactions DAG'
        ,schedule_interval='0 12 * * *'
        ,start_date=datetime(2017, 3, 20)
        ,catchup=False
) as dag:
    get_df = PythonOperator(
        task_id='get_df',
        python_callable=read_frm_source,
        op_args=[filepath,spark],
        provide_context=True,
    )
    get_transformed_df = PythonOperator(
        task_id='get_transformed_df',
        python_callable=transform_df,
        op_kwargs={'df':"{{ ti.xcom_pull(task_ids='get_df') }}",'spark':spark},
        provide_context=True,
    )
    insert_in_mysql = PythonOperator(
        task_id='insert_in_mysql',
        python_callable=insert_into_mysql,
        op_kwargs={'df':"{{ ti.xcom_pull(task_ids='get_transformed_df') }}"
                   ,'spark':spark
                   ,'URL':URL
                   ,'DRIVER':DRIVER
                   ,'DBTABLE':DBTABLE
                   ,'USER':USER
                   ,'PASS':PASS}
    )
get_df >> get_transformed_df >> insert_in_mysql