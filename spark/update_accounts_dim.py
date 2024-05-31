from pyspark.sql import SparkSession

appname = "update_accounts_dim"

spark = SparkSession.builder.appName(appname)\
    .getOrCreate()

spark.read\
  .format("iceberg")