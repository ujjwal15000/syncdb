package com.syncdb.client;

import com.syncdb.core.models.Record;
import com.syncdb.server.SyncDbServer;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.http.HttpClientResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
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

  private static SyncDbServer syncDbServer;
  private static Path tempDir;
  private static Vertx vertx;

  @BeforeAll
  @SneakyThrows
  public static void setUp() {
    zookeeperContainer.start();

    tempDir = Files.createTempDirectory("syncdb");
    tempDir.toFile().deleteOnExit();

    System.setProperty("zkHost", "localhost:" + zookeeperContainer.getMappedPort(2181));
    System.setProperty("baseDir", tempDir.toFile().getPath());

    syncDbServer = new SyncDbServer();
    syncDbServer
        .start()
        .delay(10_000, TimeUnit.MILLISECONDS)
        .subscribeOn(Schedulers.io())
        .blockingAwait();
    vertx = Vertx.vertx();

    vertx
        .createHttpClient()
        .rxRequest(HttpMethod.POST, 8000, "localhost", "/namespace")
        .flatMap(
            request ->
                request
                    .putHeader("Content-Type", "application/json")
                    .rxSend(
                        new JsonObject()
                            .put("name", "n1")
                            .put("numPartitions", 4)
                            .put("numReplicas", 1)
                            .put("numNodes", 1)
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
    SyncDBClient client = SyncDBClient.create(vertx);
    client.connect("localhost", 9009).blockingAwait();

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
            .delay(10_000, TimeUnit.MILLISECONDS)
            .blockingGet();
    assert li.size() == 1;
    assert new String(li.get(0).getValue()).equals("world");
  }

  @AfterAll
  public static void close() {
    zookeeperContainer.stop();
    zookeeperContainer.close();
  }
}
