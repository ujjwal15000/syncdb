package com.syncdb.spark.writer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.util.Objects;

import static com.syncdb.spark.SyncDbDataSource.DEFAULT_SCHEMA;

public class SyncDbFileFormat implements FileFormat {

  @Override
  public Option<StructType> inferSchema(
      SparkSession sparkSession, Map<String, String> options, Seq<FileStatus> files) {
    return null;
  }

  @Override
  public OutputWriterFactory prepareWrite(
      SparkSession sparkSession, Job job, Map<String, String> options, StructType dataSchema) {
    if (!Objects.deepEquals(dataSchema, DEFAULT_SCHEMA))
      throw new RuntimeException(
          "schema mismatch detected. Expected schema should be:\n\t"
              + DEFAULT_SCHEMA
              + "\n\treceived schema:\n\t"
              + dataSchema);
    return new SyncDbOutputWriterFactory();
  }
}
