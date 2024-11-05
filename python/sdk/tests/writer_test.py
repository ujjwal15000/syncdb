from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from testcontainers.core.container import DockerContainer
import boto3
import unittest
import re

from sdk.syncdb.resources.config import spark_msgpack_jar, msgpack_jar, hdoop_aws_jar, aws_sdk_v1_jar
from sdk.syncdb.writer import write_spark_df


class MySparkAppTest(unittest.TestCase):
    s3_client: boto3.client = None
    localstack_container = None
    spark = None
    bucket = "test-bucket"

    @classmethod
    def setUpClass(cls):
        cls.localstack_container = (
            DockerContainer("localstack/localstack:latest")
            .with_env("SERVICES", "s3")
            .with_exposed_ports(4566)
        )
        cls.localstack_container.start()

        # Retrieve the mapped port for S3
        host = cls.localstack_container.get_container_host_ip()
        port = cls.localstack_container.get_exposed_port(4566)

        # Configure boto3 to use LocalStack for S3
        cls.s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{host}:{port}",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
        cls.s3_client.create_bucket(Bucket=cls.bucket)
        cls.spark = SparkSession.builder \
            .appName("SyncDBWriter") \
            .config("spark.hadoop.fs.s3a.access.key", "test") \
            .config("spark.hadoop.fs.s3a.secret.key", "test") \
            .config("spark.hadoop.fs.s3a.endpoint", f"{host}:{port}") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False) \
            .config("spark.jars", f"{spark_msgpack_jar},{msgpack_jar},{hdoop_aws_jar},{aws_sdk_v1_jar}") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_writer(cls):
        data = []
        for i in range(10):
            data.append(("key0" + str(i), "value0" + str(i)))
        schema = StructType([
            StructField("key", StringType(), False),
            StructField("value", StringType(), False)
        ])

        df = cls.spark.createDataFrame(data, schema=schema)

        path = "s3a://test-bucket/msgpack-test"
        path_prefix_key = "msgpack-test"
        timestamp = write_spark_df(df, num_partitions=1, path=path)
        test_file = open("../../../core/src/test/resources/sparkwritertestfiles/"
                         "part-00000-2a0a659c-7d18-4d3e-8a40-d7cf44d3fd4f-c000.mp", 'rb').read()

        pattern = r"^part-(\d+)-([a-f0-9\-]+)-c(\d+)\.mp$"
        regex = re.compile(pattern)
        prefix = f"{path_prefix_key}/{timestamp}/"
        response = cls.s3_client.list_objects_v2(Bucket=cls.bucket, Prefix=prefix)

        matching_files = []
        if "Contents" in response:
            matching_files = [
                obj["Key"].split(f"{path_prefix_key}/{timestamp}/")[1] for obj in response["Contents"] \
                if regex.match(obj["Key"].split(f"{path_prefix_key}/{timestamp}/")[1])
            ]
        assert len(matching_files) == 1

        uploaded_file = cls.s3_client \
            .get_object(Bucket=cls.bucket, Key=f"{path_prefix_key}/{timestamp}/{matching_files[0]}")['Body'] \
            .read()

        assert test_file == uploaded_file
