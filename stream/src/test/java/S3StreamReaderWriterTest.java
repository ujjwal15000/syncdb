import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.stream.models.Record;
import com.syncdb.stream.producer.StreamProducer;
import com.syncdb.stream.reader.S3StreamReader;
import com.syncdb.stream.serde.deserializer.StringDeserializer;
import com.syncdb.stream.serde.serializer.StringSerializer;
import com.syncdb.stream.util.ObjectMapperUtils;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.writer.S3StreamWriter;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.syncdb.stream.constant.Constants.STREAM_DELIMITER;

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

  // will write 2 blocks and start next for 3
  private static Long expectedLatestBlock = 3L;

  private static List<Record<String, String>> testRecords = getTestRecords(numTestRecords);

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

    ObjectMapper objectMapper = ObjectMapperUtils.getMsgPackObjectMapper();
        // test record format: key01, value01 and 4 rows per block
        Integer blockSize = (objectMapper.writeValueAsBytes(
                Record.<byte[], byte[]>builder()
                        .key("key01".getBytes())
                        .value("value01".getBytes())
                        .build())
                .length + STREAM_DELIMITER.getBytes().length)
                * 4;

    s3StreamWriter =
        new S3StreamWriter<>(
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

  @Test
  public void writeStreamTest(){
    List<Completable> futures = new ArrayList<>();
    for (var x : testRecords)
      futures.add(Completable.fromCallable(() -> streamProducer.send(new ProducerRecord<>("test", x.getKey(), x.getValue())).get()));
    s3StreamWriter
        .writeStream(streamProducer.getRecordStream())
        .subscribeOn(Schedulers.io())
        .subscribe();
    streamProducer.close(Duration.ofMillis(5_000));
    Completable.merge(futures)
            // let flushHandler invoke
            .delay(flushTimeout + 1_000L, TimeUnit.MILLISECONDS)
            .blockingAwait();
    assert Objects.equals(s3StreamWriter.getLatestBlockId(), expectedLatestBlock);
  }

  @Test
  public void readStreamTest() {
    Long finalBlockId = s3StreamReader.getStreamMetadata().blockingGet().getLatestBlockId();
    List<Record<String, String>> records = Flowable.range(0, finalBlockId.intValue())
            .concatMap(blockId -> s3StreamReader.readBlock(blockId.longValue()))
            .toList()
            .blockingGet();
    assert Objects.deepEquals(testRecords, records) ;
  }

  private static List<Record<String, String>> getTestRecords(int numRecords){
    assert numRecords < 100;

    List<Record<String, String>> li = new ArrayList<>();
    for(int i=0;i<numRecords;i++){
      li.add(
              Record.<String, String>builder()
                      .key("key" + (i < 10 ? "0" + i : i))
                      .value("value" + (i < 10 ? "0" + i : i))
                      .build());
    }
    return li;
  }
}
