package com.syncdb.spark;

import com.syncdb.core.models.Record;
import com.syncdb.core.util.TestRecordsUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static com.syncdb.spark.SyncDbDataSource.DEFAULT_SCHEMA;

@Slf4j
public class SparkWriterTestIT {
  public static SparkSession spark;

  public static List<Record<String, String>> testRecords = TestRecordsUtils.getTestRecords(10);

  @BeforeAll
  @SneakyThrows
  public static void SetUp() {
    spark =
        SparkSession.builder()
            .appName("syncdb writer test")
            .master("local[*]")
            .config("spark.jars", "target/syncdb-writer-1.0.0-SNAPSHOT.jar")
            .getOrCreate();
    spark.conf().set("spark.sql.sources.package", "com.syncdb.spark");
  }

  @Test
  public void BatchWriteTest() {
    List<Row> rows =
        testRecords.stream()
            .map(
                r ->
                    RowFactory.create(
                        r.getKey().getBytes(StandardCharsets.UTF_8),
                        r.getValue().getBytes(StandardCharsets.UTF_8)))
            .collect(Collectors.toUnmodifiableList());

    Dataset<Row> df = spark.createDataFrame(rows, DEFAULT_SCHEMA);
    String outputPath = "target/test-writer";

    df.write().format("syncdb").option("path", outputPath).mode("append").save();
  }

  @AfterAll
  public static void stop() {
    spark.stop();
  }
}
