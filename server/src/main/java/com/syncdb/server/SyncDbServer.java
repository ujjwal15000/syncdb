package com.syncdb.server;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import com.syncdb.server.cluster.Controller;
import com.syncdb.server.cluster.Participant;
import com.syncdb.server.cluster.ZKAdmin;
import com.syncdb.server.cluster.config.HelixConfig;
import com.syncdb.server.cluster.statemodel.MasterSlaveStateModelFactory;
import com.syncdb.server.cluster.statemodel.OnlineOfflineStateModelFactory;
import com.syncdb.server.factory.NamespaceConfig;
import com.syncdb.server.factory.NamespaceFactory;
import com.syncdb.server.factory.TabletFactory;
import com.syncdb.server.factory.TabletMailbox;
import com.syncdb.server.verticle.ControllerVerticle;
import com.syncdb.server.verticle.TabletVerticle;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;

import javax.xml.stream.events.Namespace;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.syncdb.core.constant.Constants.HELIX_POOL_NAME;
import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;

@Slf4j
public class SyncDbServer {

  private final Vertx vertx;
  private final HelixConfig config;
  private final ZKAdmin zkAdmin;
  private final Controller controller;
  private final Participant participant;

  private final Thread shutdownHook = new Thread(() -> this.stop(30_000));

  public static void main(String[] args) throws Exception {
    SyncDbServer syncDbServer = new SyncDbServer();
    syncDbServer.start();
  }

  public SyncDbServer() throws Exception {

    String zkHost = System.getProperty("zkHost", null);
    assert !Objects.equals(zkHost, null);

    String nodeId =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(UUID.randomUUID().toString().getBytes());
    this.config = new HelixConfig(zkHost, "syncdb", nodeId);
    this.vertx = initVertx().blockingGet();

    this.zkAdmin = new ZKAdmin(vertx, config);
    this.controller = new Controller(vertx, config);
    controller.connect();

    this.participant = startParticipant();
  }

  private Participant startParticipant() throws Exception {
    Participant participant = new Participant(vertx, config);

    // todo: get tablet model factory here
    //    participant.connect();
    MasterSlaveStateModelFactory masterSlaveStateModelFactory =
        new MasterSlaveStateModelFactory(this.vertx, config.getInstanceName());
    OnlineOfflineStateModelFactory onlineOfflineStateModelFactory =
        new OnlineOfflineStateModelFactory(vertx, config.getInstanceName(), zkAdmin);

    participant.connect(masterSlaveStateModelFactory, onlineOfflineStateModelFactory);
    return participant;
  }

  private Single<Vertx> initVertx() {
    JsonObject zkConfig = new JsonObject();
    assert config != null;
    zkConfig.put("zookeeperHosts", config.getZhHost());
    zkConfig.put("rootPath", "io.vertx");
    zkConfig.put("retry", new JsonObject().put("initialSleepTime", 3000).put("maxTimes", 3));

    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);

    return Vertx.builder()
        .withClusterManager(mgr)
        .with(
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
                .setPreferNativeTransport(true))
        .buildClustered()
        .map(
            vertx -> {
              RxJavaPlugins.setComputationSchedulerHandler(s -> RxHelper.scheduler(vertx));
              RxJavaPlugins.setIoSchedulerHandler(s -> RxHelper.scheduler(vertx));
              RxJavaPlugins.setNewThreadSchedulerHandler(s -> RxHelper.scheduler(vertx));
              Runtime.getRuntime().addShutdownHook(shutdownHook);
              return vertx;
            });
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

    Completable.mergeArray(deployServerVerticle(), deployControllerVerticle())
        .subscribe(
            () -> log.info("successfully started server"),
            (e) -> log.error("application startup failed: ", e));
  }

  private Completable deployServerVerticle() {
    return vertx
        .rxDeployVerticle(
            TabletVerticle::new,
            new DeploymentOptions()
                .setInstances(CpuCoreSensor.availableProcessors())
                .setWorkerPoolName(WORKER_POOL_NAME))
        .ignoreElement();
  }

  private Completable deployControllerVerticle() {
    return vertx
        .rxDeployVerticle(
            new ControllerVerticle(controller, zkAdmin),
            new DeploymentOptions().setInstances(1).setWorkerPoolName(HELIX_POOL_NAME))
        .ignoreElement();
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
