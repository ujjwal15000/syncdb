package com.syncdb.spark;

import com.syncdb.core.models.Record;
import com.syncdb.core.util.TestRecordsUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.syncdb.spark.SyncDbDataSource.DEFAULT_SCHEMA;

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
  public void BatchWriteTest() throws IOException {
    List<Row> rows =
        testRecords.stream()
            .map(
                r ->
                    RowFactory.create(
                        r.getKey().getBytes(StandardCharsets.UTF_8),
                        r.getValue().getBytes(StandardCharsets.UTF_8)))
            .collect(Collectors.toUnmodifiableList());

    StructType schema = new StructType(new StructField[]{
            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
    });
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    Path outputPath = Files.createTempDirectory("temp_");

    df.repartition(1)
        .write()
        .format("syncdb")
        .option("path", outputPath.toString())
        .mode("overwrite")
        .save();
    byte[] testFile = null;
    try (FileInputStream f =
        new FileInputStream("../core/src/test/resources/sparkwritertestfiles/" +
                "part-00000-adaa3afb-d226-4a77-8909-05103e37d551-c000.sdb")) {
      testFile = f.readAllBytes();
    }
    catch (Exception e){
      log.error("error opening file");
    }
    assert testFile != null;
    List<String> files = Files.walk(outputPath)
            .map(Path::toString)
            .filter(r -> r.endsWith(".sdb"))
            .collect(Collectors.toUnmodifiableList());

    assert files.size() == 1;

    byte[] genFile = null;
    try (FileInputStream f =
                 new FileInputStream(files.get(0))) {
      genFile = f.readAllBytes();
    }
    catch (Exception e){
      log.error("error opening file");
    }
    assert genFile != null;

    assert Objects.deepEquals(genFile, testFile);
    deleteTempDir(outputPath);
  }



  @AfterAll
  public static void stop() {
    spark.stop();
  }

  private static void deleteTempDir(Path dir) throws IOException {
    Files.walk(dir)
            .map(Path::toFile)
            .forEach(File::delete);
  }
}
