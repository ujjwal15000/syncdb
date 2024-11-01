import com.syncdb.core.models.Record;
import com.syncdb.core.serde.serializer.StringSerializer;
import com.syncdb.spark.dbwriter.S3PartitionWriter;
import com.syncdb.stream.util.S3Utils;
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
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class S3PartitionWriterTest {
  private static final GenericContainer awsContainer =
          new GenericContainer("localstack/localstack:latest")
                  .withExposedPorts(4566)
                  .withEnv("SERVICES", "s3")
                  .withReuse(false);

  private static String bucketName;
  private static String rootPath;
  private static S3AsyncClient client;
  private static SparkSession spark;
  private static Integer numTestRecords = 10;

  private static List<Record<String, String>> testRecords = getTestRecords(numTestRecords);
  private static Dataset<Row> df;
  private static S3PartitionWriter<String, String> writer;

  @BeforeAll
  public static void setUp(){
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

    spark = SparkSession.builder()
            .appName("S3PartitionWriterTest")
            .master("local[*]")
            .getOrCreate();
    spark.sparkContext().setLogLevel("DEBUG");

    StructType schema = new StructType(new StructField[]{
            new StructField("key", DataTypes.StringType, false, Metadata.empty()),
            new StructField("value", DataTypes.StringType, false, Metadata.empty())
    });

    List<Row> data = new ArrayList<>();
    for(var x : testRecords){
      data.add(RowFactory.create(x.getKey(), x.getValue()));
    }

    df = spark.createDataFrame(data, schema);

    writer = new S3PartitionWriter<>(bucketName,
            "us-east-1",
            rootPath,
            new StringSerializer(),
            new StringSerializer());
  }

  @AfterAll
  @SneakyThrows
  public static void cleanUp() {
    awsContainer.stop();
    client.close();
    spark.stop();
  }

  @Test
  public void testWrite(){
    df.foreachPartition(writer);
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
