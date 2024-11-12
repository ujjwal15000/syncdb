package com.syncdb.stream;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.serializer.ByteSerializer;
import com.syncdb.core.util.TestRecordsUtils;
import com.syncdb.stream.producer.StreamProducer;
import com.syncdb.stream.reader.S3StreamReader;
import com.syncdb.core.serde.deserializer.StringDeserializer;
import com.syncdb.core.serde.serializer.StringSerializer;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.writer.S3StreamWriter;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class S3StreamReaderWriterTest {
  private static StreamProducer<String, String> streamProducer;
  private static S3StreamWriter<String, String> s3StreamWriter;
  private static S3StreamReader<String, String> s3StreamReader;

  private static String bucketName;
  private static String rootPath;
  private static S3AsyncClient client;
  private static Integer numTestRecords = 10;
  private static Integer numRowsPerBlock = 4;
  private static final Long flushTimeout = 1_000L;
  private static final Integer producerBufferSize = 2;

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
    rootPath = "data";
    client = S3Utils.getClient("us-east-1");
    S3Utils.createBucket(client, bucketName).blockingAwait();

    // upload test files
    //    try (FileInputStream f =
    //        new FileInputStream("../core/src/test/resources/msgpacktestfiles/test.mp")) {
    //      S3Utils.putS3Object(
    //              client, bucketName, msgPackRootPath + "/" + msgPackTestFileName,
    // f.readAllBytes())
    //          .blockingAwait();
    //    } catch (Exception e) {
    //      log.error("error while uploading file: ", e);
    //      throw e;
    //    }

    int rowSize =
        Record.serialize(
                Record.<byte[], byte[]>builder()
                    .key("key01".getBytes())
                    .value("value01".getBytes())
                    .build(),
                new ByteSerializer(),
                new ByteSerializer())
            .length;

    // test record format: key01, value01 and 4 rows per block
    // 7 to make skewed buffers
    int blockSize = (rowSize + 7) * numRowsPerBlock;

    int requiredBytes = (4 + rowSize) * numTestRecords;

    expectedBlocks = (long) (requiredBytes / blockSize) + 1;

    s3StreamWriter =
        new S3StreamWriter<>(
            UUID.randomUUID().toString(),
            0,
            bucketName,
            "us-east-1",
            rootPath,
            new StringSerializer(),
            new StringSerializer(),
            blockSize,
            flushTimeout);
    s3StreamReader =
        new S3StreamReader<>(
            bucketName, "us-east-1", rootPath, new StringDeserializer(), new StringDeserializer());
    streamProducer = new StreamProducer<>(producerBufferSize);
  }

  @AfterAll
  @SneakyThrows
  public static void cleanUp() {
    awsContainer.stop();
    client.close();
    s3StreamWriter.close();
    s3StreamReader.close();
  }

//  @Test
//  public void writeStreamTest() {
//    // todo: use test scheduler here and fix timeout issues
//
//    List<Completable> futures = new ArrayList<>();
//    for (var x : testRecords)
//      futures.add(
//          Completable.fromCallable(
//              () ->
//                  streamProducer
//                      .send(new ProducerRecord<>("test", x.getKey(), x.getValue()))
//                      .get()));
//    s3StreamWriter
//        .writeStream(streamProducer.getRecordStream())
//        .subscribeOn(Schedulers.io())
//        .subscribe();
//    streamProducer.close(Duration.ofMillis(5_000));
//    Completable.merge(futures)
//        // let flushHandler invoke
//        .delay(flushTimeout + 1_000L, TimeUnit.MILLISECONDS)
//        .blockingAwait();
//
//    long numFiles = S3Utils.listObjects(client, bucketName, rootPath).blockingGet().size();
//    assert Objects.equals(numFiles, expectedBlocks);
//  }

    @Test
    public void writeStreamTest() {
      s3StreamWriter
              .writeStream(Flowable.fromIterable(testRecords), 0)
              .blockingAwait();

      long numFiles = S3Utils.listObjects(client, bucketName, rootPath).blockingGet().size();
      assert Objects.equals(numFiles, expectedBlocks);
    }

  @Test
  public void readStreamTest() {
    List<String> blockPrefix = S3Utils.listCommonPrefixes(client, bucketName, rootPath).blockingGet();
    assert blockPrefix.size() == 1;

    List<String> blockIds =
        S3Utils.listObjects(client, bucketName, blockPrefix.get(0)).blockingGet();
    List<Record<String, String>> records =
        Flowable.fromIterable(blockIds)
            .concatMap(blockId -> s3StreamReader.readBlock(blockId))
            .toList()
            .blockingGet();
    assert Objects.deepEquals(testRecords, records);
  }
}
