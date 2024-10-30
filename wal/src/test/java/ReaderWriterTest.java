import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.wal.models.Record;
import com.syncdb.wal.reader.S3Reader;
import com.syncdb.wal.serde.deserializer.StringDeserializer;
import com.syncdb.wal.serde.serializer.StringSerializer;
import com.syncdb.wal.util.ObjectMapperUtils;
import com.syncdb.wal.util.S3Utils;
import com.syncdb.wal.writer.S3Writer;
import io.reactivex.rxjava3.core.Flowable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.syncdb.wal.constant.Constants.STREAM_DELIMITER;

@Slf4j
public class ReaderWriterTest {
  private static S3Writer<String, String> s3Writer;
  private static S3Reader<String, String> s3Reader;
  private static String bucketName;
  private static String rootPath;
  private static S3AsyncClient client;
  private static List<Record<String, String>> testRecords = getTestRecords(10);

  private static final String LOCAL_STACK_ADDRESS = "http://localhost:4566";

  // test record format: key01, value01 and 4 rows per block
  private static Integer blockSize;

  @BeforeAll
  @SneakyThrows
  public static void SetUp() {
    // todo start localstack container here
    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
    System.setProperty("aws.endpointUrl", LOCAL_STACK_ADDRESS);
    bucketName = "test-bucket";
    rootPath = "data";
    client = S3Utils.getClient("us-east-1");
    S3Utils.createBucket(client, bucketName).blockingAwait();

    ObjectMapper objectMapper = ObjectMapperUtils.getMsgPackObjectMapper();
    blockSize =
        (objectMapper.writeValueAsBytes(
                    Record.<byte[], byte[]>builder()
                        .key("key01".getBytes())
                        .value("value01".getBytes())
                        .build())
                .length + STREAM_DELIMITER.getBytes().length)
            * 4;

    s3Writer =
        new S3Writer<>(
            bucketName,
            "us-east-1",
            rootPath,
            new StringSerializer(),
            new StringSerializer(),
            blockSize);
    s3Reader =
        new S3Reader<>(
            bucketName, "us-east-1", rootPath, new StringDeserializer(), new StringDeserializer());
  }

  @AfterAll
  @SneakyThrows
  public static void CleanUp() {
    client.close();
    s3Writer.close();
    s3Reader.close();
  }

  @Test
  public void writeStreamTest() {
    s3Writer.writeStream(Flowable.fromIterable(testRecords)).blockingAwait();
    assert s3Writer.getBlockId() == 3;
  }

  @Test
  public void readStreamTest() {
    Integer finalBlockId = s3Writer.getBlockId();
    List<Record<String, String>> records = Flowable.range(1, finalBlockId)
            .concatMap(blockId -> s3Reader.readBlock(blockId))
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
