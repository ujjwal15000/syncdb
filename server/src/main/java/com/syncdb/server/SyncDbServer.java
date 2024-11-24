package com.syncdb.server;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import com.syncdb.server.factory.NamespaceConfig;
import com.syncdb.server.factory.NamespaceFactory;
import com.syncdb.server.factory.TabletFactory;
import com.syncdb.server.factory.TabletMailbox;
import com.syncdb.server.verticle.TabletVerticle;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;

import javax.xml.stream.events.Namespace;
import java.util.concurrent.TimeUnit;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;

@Slf4j
public class SyncDbServer {

  private final Vertx vertx;
  private final Thread shutdownHook = new Thread(() -> this.stop(30_000));

  public static void main(String[] args) {
    SyncDbServer syncDbServer = new SyncDbServer();
    syncDbServer.start();
  }

  public SyncDbServer() {
    this.vertx = initVertx();
  }

  private Vertx initVertx() {
    Vertx vertx =
        Vertx.vertx(
            new VertxOptions()
                .setMetricsOptions(
                    new MicrometerMetricsOptions()
                        .setPrometheusOptions(
                            new VertxPrometheusOptions()
                                .setEnabled(true)
                                .setStartEmbeddedServer(true)
                                .setEmbeddedServerOptions(new HttpServerOptions().setPort(9090))
                                .setEmbeddedServerEndpoint("/metrics")))
                .setEventLoopPoolSize(CpuCoreSensor.availableProcessors())
                .setPreferNativeTransport(true));

    RxJavaPlugins.setComputationSchedulerHandler(s -> RxHelper.scheduler(vertx));
    RxJavaPlugins.setIoSchedulerHandler(s -> RxHelper.scheduler(vertx));
    RxJavaPlugins.setNewThreadSchedulerHandler(s -> RxHelper.scheduler(vertx));
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    return vertx;
  }

  private void start() {
    String tmpPath = "target";
    PartitionConfig config =
        PartitionConfig.builder()
            .bucket("test")
            .region("us-east-1")
            .namespace("namespace")
            .partitionId(0)
            .rocksDbPath(tmpPath + "/" + "main")
            .rocksDbSecondaryPath(tmpPath + "/" + "secondary")
            .batchSize(100)
            .sstReaderBatchSize(2)
            .build();
    Options options = new Options().setCreateIfMissing(true);
    Tablet tablet = new Tablet(config, options);
    TabletFactory.add(tablet);

    NamespaceFactory.add(NamespaceConfig.create("namespace", 1));

    TabletMailbox mailbox = TabletMailbox.create(vertx, TabletConfig.create("namespace", 0));

    mailbox.startWriter();
    mailbox.startReader();

    vertx
        .rxDeployVerticle(
            TabletVerticle::new,
            new DeploymentOptions()
                .setInstances(CpuCoreSensor.availableProcessors())
                .setWorkerPoolName(WORKER_POOL_NAME))
        .ignoreElement()
        .subscribe(
            () -> log.info("successfully started server"),
            (e) -> log.error("application startup failed: ", e));
  }

  private void stop(int timeout) {
    Completable.complete()
        .delay(timeout, TimeUnit.MILLISECONDS)
        .andThen(vertx.rxClose())
        .subscribe(
            () -> log.info("successfully stopped server"),
            (e) -> log.error("error stopping server: ", e));
  }
}
