package com.syncdb.spark;

import com.syncdb.core.models.Record;
import com.syncdb.core.util.TestRecordsUtils;
import com.syncdb.spark.writer.SyncDbPartitioner;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SparkWriterTestIT {
  public static SparkSession spark;

  public static List<Record<String, String>> testRecords = TestRecordsUtils.getTestRecords(10);

  @BeforeAll
  @SneakyThrows
  public static void SetUp() {
    List<String> jars =
        Files.walk(Path.of("target/"))
            .map(Path::toString)
            .filter(r -> r.matches("^.*/syncdb-writer-(?!.*-tests\\.jar$).*\\.jar"))
            .collect(Collectors.toUnmodifiableList());
    assert jars.size() == 1;

    spark =
        SparkSession.builder()
            .appName("syncdb writer test")
            .master("local[*]")
            .config("spark.jars", jars.get(0))
            .getOrCreate();
    spark.conf().set("spark.sql.sources.package", "com.syncdb.spark");
  }

  @Test
  public void batchWriteTest() throws IOException, NoSuchAlgorithmException {
    List<Row> rows =
        testRecords.stream()
            .map(
                r ->
                    RowFactory.create(
                        r.getKey().getBytes(StandardCharsets.UTF_8),
                        r.getValue().getBytes(StandardCharsets.UTF_8)))
            .collect(Collectors.toUnmodifiableList());

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
              new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
            });
    Dataset<Row> df = spark.createDataFrame(rows, schema);

    df = SyncDbPartitioner.repartitionByKey(df, 4);

    Path outputPath = Files.createTempDirectory("temp_");

    df.write().format("syncdb-stream")
            .option("path", outputPath.toString())
            .mode("overwrite").save();

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    Set<byte[]> testFiles =
        Files.walk(Path.of("../core/src/test/resources/sparkwritertestfiles/"))
            .filter(r -> r.toString().endsWith(".sdb"))
            .map(
                r -> {
                  try {
                    return new FileInputStream(r.toString()).readAllBytes();
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .map(r -> digest.digest(r))
            .collect(Collectors.toSet());

    List<String> files =
        Files.walk(outputPath)
            .map(Path::toString)
            .filter(r -> r.endsWith(".sdb"))
            .collect(Collectors.toList());

    assert files.size() == 4;

    files.sort(String::compareTo);

    Set<byte[]> genFiles =
        files.stream()
            .map(
                r -> {
                  try {
                    return new FileInputStream(r).readAllBytes();
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .map(digest::digest)
            .collect(Collectors.toSet());


    assert genFiles.stream()
            .filter(testFiles::contains)
            .count() == 0;
    deleteTempDir(outputPath);
  }

  @Test
  public void sstWriteTest() throws IOException, NoSuchAlgorithmException {
    List<Row> rows =
            testRecords.stream()
                    .map(
                            r ->
                                    RowFactory.create(
                                            r.getKey().getBytes(StandardCharsets.UTF_8),
                                            r.getValue().getBytes(StandardCharsets.UTF_8)))
                    .collect(Collectors.toUnmodifiableList());

    StructType schema =
            new StructType(
                    new StructField[] {
                            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
                            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
                    });
    Dataset<Row> df = spark.createDataFrame(rows, schema);

    df = SyncDbPartitioner.repartitionByKey(df, 4);

    Path outputPath = Files.createTempDirectory("temp_");

    df.write().format("syncdb-sst")
            .option("path", outputPath.toString())
            .mode("overwrite").save();

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    Set<byte[]> testFiles =
            Files.walk(Path.of("../core/src/test/resources/sparksstwritertestfiles/"))
                    .filter(r -> r.toString().endsWith(".sdb"))
                    .map(
                            r -> {
                              try {
                                return new FileInputStream(r.toString()).readAllBytes();
                              } catch (IOException e) {
                                throw new RuntimeException(e);
                              }
                            })
                    .map(r -> digest.digest(r))
                    .collect(Collectors.toSet());

    List<String> files =
            Files.walk(outputPath)
                    .map(Path::toString)
                    .filter(r -> r.endsWith(".sst"))
                    .collect(Collectors.toList());

    assert files.size() == 4;

    files.sort(String::compareTo);

    Set<byte[]> genFiles =
            files.stream()
                    .map(
                            r -> {
                              try {
                                return new FileInputStream(r).readAllBytes();
                              } catch (IOException e) {
                                throw new RuntimeException(e);
                              }
                            })
                    .map(digest::digest)
                    .collect(Collectors.toSet());


    assert genFiles.stream()
            .filter(testFiles::contains)
            .count() == 0;
    deleteTempDir(outputPath);
  }

  @AfterAll
  public static void stop() {
    spark.stop();
  }

  private static void deleteTempDir(Path dir) throws IOException {
    Files.walk(dir).map(Path::toFile).forEach(File::delete);
  }
}
