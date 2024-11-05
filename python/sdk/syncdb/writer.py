import time

from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, DataType, BinaryType
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, UTC

kv_schema = [
    StructField("key", DataType(), False),
    StructField("value", DataType(), False)
]


def to_bytearray(value):
    return bytearray(str(value), 'utf-8')


# writes in the given path inside a timestamp folder
# todo: first start as a temp_<start_timestamp> then update to just timestamp
def write_spark_df(df: DataFrame, num_partitions, path):
    assert_schema(df.schema)
    timestamp = int(datetime.now(UTC).timestamp() * 1000)

    to_bytearray_udf = udf(to_bytearray, BinaryType())
    df_bytearray = df.select([to_bytearray_udf(df[col]).alias(col) for col in df.columns])

    df_bytearray.repartition(num_partitions).write \
        .format("messagepack") \
        .mode("append") \
        .save(f"{path}/{str(timestamp)}")

    return timestamp


def assert_schema(schema):
    assert len(schema.fields) == len(kv_schema), "Schema does not have the expected number of fields"

    for schema_field, expected_field in zip(schema.fields, kv_schema):
        assert schema_field.name == expected_field.name, \
            f"Expected field name '{expected_field.name}', but got '{schema_field.name}'"
        assert schema_field.nullable == expected_field.nullable, \
                (f"Expected nullability '{expected_field.nullable}' "
                 f"for field '{schema_field.name}', but got '{schema_field.nullable}'")
