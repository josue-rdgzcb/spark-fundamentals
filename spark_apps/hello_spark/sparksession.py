from pyspark.sql import SparkSession
from app_config import APP_NAME

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)