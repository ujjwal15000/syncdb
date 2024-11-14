package com.syncdb.tablet;

import com.syncdb.core.models.Record;
import com.syncdb.core.partitioner.Murmur3Partitioner;
import com.syncdb.core.util.TestRecordsUtils;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.writer.S3StreamWriter;
import com.syncdb.tablet.models.PartitionConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Slf4j
public class TabletReadTest {

  private static String bucketName;
  private static String namespace;
  private static String awsRegion = "us-east-1";

  private static S3AsyncClient client;
  private static Integer numTestRecords = 10;
  private static Tablet tablet;
  private static Path tmpPath;
  private static Integer numPartitions = 4;
  private static Integer currentPartition = 1;
  private static Murmur3Partitioner partitioner = new Murmur3Partitioner(4);

  private static Long expectedBlocks;

  private static List<Record<String, String>> testRecords =
      TestRecordsUtils.getTestRecords(numTestRecords);

  private static final GenericContainer awsContainer =
      new GenericContainer("localstack/localstack:latest")
          .withExposedPorts(4566)
          .withEnv("SERVICES", "s3")
          .withReuse(false);

  @BeforeAll
  @SneakyThrows
  public static void SetUp() {
    awsContainer.start();
    int localstackPort = awsContainer.getMappedPort(4566);
    String localstackAddress = "http://localhost:" + localstackPort;

    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
    System.setProperty("aws.endpointUrl", localstackAddress);
    bucketName = "test-bucket";
    namespace = "tablet";
    client = S3Utils.getClient("us-east-1");
    S3Utils.createBucket(client, bucketName).blockingAwait();

      Files.walk(Path.of("../core/src/test/resources/sparkwritertestfiles/"))
          .filter(
              r -> !r.getFileName().toString().matches(S3StreamWriter.WriteState._SUCCESS.name()))
          .filter(r -> r.toString().endsWith(".sdb"))
          .forEach(
              r -> {
                try (FileInputStream f = new FileInputStream(r.toFile())) {
                  S3Utils.putS3Object(
                          client, bucketName, namespace + "/" + r.getFileName(), f.readAllBytes())
                      .blockingAwait();
                } catch (Exception e) {
                  log.error("error while uploading file: ", e);
                }
              });

    S3Utils.putS3Object(
        client,
        bucketName,
        namespace + "/" + S3StreamWriter.WriteState._SUCCESS.name(),
        new byte[0]);

    tmpPath = Files.createTempDirectory("temp_");

    PartitionConfig config = PartitionConfig.builder()
            .bucket(bucketName)
            .region(awsRegion)
            .namespace(namespace)
            .partitionId(currentPartition)
            .rocksDbPath(tmpPath + "/" + "main")
            .rocksDbSecondaryPath(tmpPath + "/" + "secondary")
            .batchSize(100)
            .build();
    Options options = new Options().setCreateIfMissing(true);
    tablet = new Tablet(config, options);
  }

  @Test
  public void testRead() throws InterruptedException {
    List<Record<String, String>> records = TestRecordsUtils.getTestRecords(10)
            .stream()
            .filter(r -> partitioner.getPartition(r.getKey().getBytes()) == currentPartition)
            .collect(Collectors.toUnmodifiableList());
    tablet.openIngestor();
    Thread.sleep(6_000);

    tablet.openReader();
    Thread.sleep(5_000);
    tablet.getReader().catchUp();
    Thread.sleep(2_000);

    List<String> reads = records.stream()
            .map(Record::getKey)
            .map(r -> {
                try {
                    return tablet.getReader().read(r.getBytes());
                } catch (RocksDBException e) {
                    log.error("error while getting key: ", e);
                }
                return new byte[0];
            })
            .map(String::new)
            .collect(Collectors.toUnmodifiableList());

    assert records.stream()
            .map(Record::getValue).allMatch(reads::contains);
  }

  @AfterAll
  @SneakyThrows
  public static void cleanUp() {
    awsContainer.stop();
    client.close();
    deleteTempDir(tmpPath);
  }

  private static void deleteTempDir(Path dir) throws IOException {
    Files.walk(dir).map(Path::toFile).forEach(File::delete);
  }

}
