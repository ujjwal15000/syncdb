package com.syncdb.client;

import com.syncdb.core.models.Record;
import com.syncdb.server.SyncDbServer;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.http.HttpClientResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ReadWriteTest {
  private static GenericContainer<?> zookeeperContainer =
      new GenericContainer<>("zookeeper:3.8.1").withExposedPorts(2181);

  private static SyncDbServer syncDbServer0;
  private static SyncDbServer syncDbServer1;
  private static SyncDbServer syncDbServer2;
  private static Path tempDir;
  private static Vertx vertx;

  @BeforeAll
  @SneakyThrows
  public static void setUp() {
    zookeeperContainer.start();

    tempDir = Files.createTempDirectory("syncdb");
    tempDir.toFile().deleteOnExit();

    System.setProperty("syncdb.zkHost", "localhost:" + zookeeperContainer.getMappedPort(2181));
    System.setProperty("syncdb.baseDir", tempDir.toFile().getPath());
    System.setProperty("syncdb.initRandomPort", "true");
    System.setProperty("syncdb.localCluster", "true");

    syncDbServer0 = new SyncDbServer();
    syncDbServer1 = new SyncDbServer();
    syncDbServer2 = new SyncDbServer();
    syncDbServer0
        .start()
        .subscribeOn(Schedulers.io())
        .blockingAwait();
    syncDbServer1
            .start()
            .subscribeOn(Schedulers.io())
            .blockingAwait();
    syncDbServer2
            .start()
            .delay(10_000, TimeUnit.MILLISECONDS)
            .subscribeOn(Schedulers.io())
            .blockingAwait();

    int controllerPort = Integer.parseInt(System.getProperty("syncdb.controllerPort"));
    vertx = Vertx.vertx();
    vertx
        .createHttpClient()
        .rxRequest(HttpMethod.POST, controllerPort, "localhost", "/namespace")
        .flatMap(
            request ->
                request
                    .putHeader("Content-Type", "application/json")
                    .rxSend(
                        new JsonObject()
                            .put("name", "n1")
                            .put("numPartitions", 12)
                            .put("numReplicas", 3)
                            .put("numNodes", 3)
                            .toString()))
        .flatMap(HttpClientResponse::rxBody)
        .doOnSuccess(r -> log.info(new String(r.getBytes())))
        .doOnError(e -> log.error("error while creating namespace", e))
        .ignoreElement()
        .delay(30_000, TimeUnit.MILLISECONDS)
        .blockingAwait();
  }

  @Test
  public void readWriteTest() {
    int serverPort = Integer.parseInt(System.getProperty("syncdb.serverPort"));
    SyncDBClientConfig config = SyncDBClientConfig.builder()
            .host("localhost")
            .port(serverPort)
            .numConnections(1)
            .build();
    SyncDBClient client = SyncDBClient.create(vertx, config);
    client.connect().blockingAwait();

    client
        .write(
            List.of(
                Record.<byte[], byte[]>builder()
                    .key("hello".getBytes(StandardCharsets.UTF_8))
                    .value("world".getBytes(StandardCharsets.UTF_8))
                    .build()),
            "n1")
        .delay(10_000, TimeUnit.MILLISECONDS)
        .blockingAwait();

    List<Record<byte[], byte[]>> li =
        client
            .read(List.of("hello".getBytes(StandardCharsets.UTF_8)), "n1")
            .blockingGet();
    assert li.size() == 1;
    assert new String(li.get(0).getValue()).equals("world");
  }

  @Test
  public void readWriteMultipleConnectionsTest() {
    int serverPort = Integer.parseInt(System.getProperty("syncdb.serverPort"));
    SyncDBClientConfig config = SyncDBClientConfig.builder()
            .host("localhost")
            .port(serverPort)
            .build();
    SyncDBClient client = SyncDBClient.create(vertx, config);
    client.connect().blockingAwait();

    client
            .write(
                    List.of(
                            Record.<byte[], byte[]>builder()
                                    .key("hello1".getBytes(StandardCharsets.UTF_8))
                                    .value("world1".getBytes(StandardCharsets.UTF_8))
                                    .build()),
                    "n1")
            .delay(10_000, TimeUnit.MILLISECONDS)
            .blockingAwait();

    List<Record<byte[], byte[]>> li =
            client
                    .read(List.of("hello1".getBytes(StandardCharsets.UTF_8)), "n1")
                    .blockingGet();
    assert li.size() == 1;
    assert new String(li.get(0).getValue()).equals("world1");
  }

  @AfterAll
  public static void close() {
    zookeeperContainer.stop();
    zookeeperContainer.close();
  }
}
