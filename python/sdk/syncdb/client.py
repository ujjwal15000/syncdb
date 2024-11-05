# 1. init a client class
# 2. fetches metadata at startup
# 3. actions:
# a. read data
# b. write data using spark to s3 in msgpack format

# requires following jars for spark
# /io/github/cybercentrecanada/spark-msgpack-datasource-3.5_2.12/1.9.0/spark-msgpack-datasource-3.5_2.12-1.9.0.jar
# /org/msgpack/msgpack-core/0.9.8/msgpack-core-0.9.8.jar
# /org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
# /com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

from pyspark.sql import SparkSession
from sdk.syncdb.resources.config import *


# todo: fix these post server setup
# def init_spark():
#     return SparkSession.builder \
#         .appName("SyncDBWriter") \
#         .config("spark.hadoop.fs.s3a.access.key", "test") \
#         .config("spark.hadoop.fs.s3a.secret.key", "test") \
#         .config("spark.hadoop.fs.s3a.endpoint", "localhost:4566") \
#         .config("spark.hadoop.fs.s3a.path.style.access", True) \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False) \
#         .config("spark.jars", f"{spark_msgpack_jar},{msgpack_jar},{hdoop_aws_jar},{aws_sdk_v1_jar}") \
#         .getOrCreate()
