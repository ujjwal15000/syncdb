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
import io.reactivex.rxjava3.core.Flowable;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class S3StreamReaderWriterTest {
  private static StreamProducer<String, String> streamProducer;
  private static S3StreamWriter<String, String> s3StreamWriter;
  private static S3StreamReader<String, String> s3StreamReader;

  private static String bucketName;
  private static String writerNamespace;
  private static String readerNamespace;
  private static S3AsyncClient client;
  private static Integer numTestRecords = 10;
  private static Integer numRowsPerBlock = 4;
  private static List<byte[]> testFiles = new ArrayList<>();
  private static final Long flushTimeout = 1_000L;
  private static final Integer producerBufferSize = 2;
  private static final MessageDigest digest;

  static {
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

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
    writerNamespace = "writer";
    readerNamespace = "reader";
    client = S3Utils.getClient("us-east-1");
    S3Utils.createBucket(client, bucketName).blockingAwait();

    testFiles =
        Files.walk(Path.of("../core/src/test/resources/sparkwritertestfiles/"))
            .filter(
                r -> !r.getFileName().toString().matches(S3StreamWriter.WriteState._SUCCESS.name()))
            .filter(r -> r.toString().endsWith(".sdb"))
            .map(
                r -> {
                  byte[] res = null;
                  try (FileInputStream f = new FileInputStream(r.toFile())) {
                    res = f.readAllBytes();
                    S3Utils.putS3Object(
                            client, bucketName, readerNamespace + "/" + r.getFileName(), res)
                        .blockingAwait();
                  } catch (Exception e) {
                    log.error("error while uploading file: ", e);
                  }
                  return res;
                })
            .map(digest::digest)
            .collect(Collectors.toUnmodifiableList());

    S3Utils.putS3Object(
        client,
        bucketName,
        readerNamespace + "/" + S3StreamWriter.WriteState._SUCCESS.name(),
        new byte[0]);

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
            4,
            bucketName,
            "us-east-1",
            writerNamespace,
            new StringSerializer(),
            new StringSerializer(),
            blockSize,
            flushTimeout);
    s3StreamReader =
        new S3StreamReader<>(
            bucketName,
            "us-east-1",
            readerNamespace,
            new StringDeserializer(),
            new StringDeserializer());
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
  //    List<String> blockPrefix =
  //        S3Utils.listCommonPrefixes(client, bucketName, rootPath).blockingGet();
  //    assert blockPrefix.size() == 1;
  //
  //    long numFiles =
  //        S3Utils.listObjects(client, bucketName, blockPrefix.get(0)).blockingGet().size();
  //
  //    assert Objects.equals(numFiles, expectedBlocks);
  //  }

  @Test
  public void writeStreamTest() {
    s3StreamWriter.writeStream(Flowable.fromIterable(testRecords)).blockingAwait();

    List<String> allFiles = S3Utils.listObjects(client, bucketName, writerNamespace).blockingGet();

    long successFile =
        allFiles.stream()
            .map(r -> r.split("/")[r.split("/").length - 1])
            .filter(r -> r.matches(S3StreamWriter.WriteState._SUCCESS.name()))
            .count();

    assert Objects.equals(1L, successFile);

    Set<byte[]> successFiles =
        allFiles.stream()
            .map(r -> r.split("/")[r.split("/").length - 1])
            .filter(r -> !r.matches(S3StreamWriter.WriteState._SUCCESS.name()))
            .map(
                r ->
                    S3Utils.getS3Object(client, bucketName, writerNamespace + "/" + r)
                        .blockingGet())
            .map(digest::digest)
            .collect(Collectors.toSet());

    assert successFiles.size() == testFiles.size();

    assert successFiles.stream()
            .filter(testFiles::contains)
            .count() == 0;
  }

  @Test
  public void readStreamTest() throws InterruptedException {
    List<Record<String, String>> records = new ArrayList<>();
    var subscriber = getTestSubscriber(records);
    s3StreamReader.readRecord(subscriber, 0, 2_000);
    Thread.sleep(5_000);
    s3StreamReader.stop();
    records.sort(Comparator.comparing(Record::getKey));
    assert Objects.deepEquals(testRecords, records);
  }

  private static Subscriber<Record<String, String>> getTestSubscriber(
      List<Record<String, String>> records) {
    return new Subscriber<Record<String, String>>() {
      Subscription upstream;
      @Getter Boolean error = false;
      @Getter Boolean complete = false;

      @Override
      public void onSubscribe(Subscription s) {
        this.upstream = s;
        upstream.request(1);
      }

      @Override
      public void onNext(Record<String, String> stringStringRecord) {
        records.add(stringStringRecord);
        upstream.request(1);
      }

      @Override
      public void onError(Throwable t) {
        error = true;
      }

      @Override
      public void onComplete() {
        complete = false;
      }
    };
  }
}
