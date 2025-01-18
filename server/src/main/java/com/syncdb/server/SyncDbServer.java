package com.syncdb.server;

import com.syncdb.core.util.NetUtils;
import com.syncdb.core.util.TimeUtils;
import com.syncdb.server.cluster.CleanupProcessor;
import com.syncdb.server.cluster.Controller;
import com.syncdb.server.cluster.Participant;
import com.syncdb.server.cluster.ZKAdmin;
import com.syncdb.server.cluster.config.HelixConfig;
import com.syncdb.server.cluster.factory.TabletMailboxFactory;
import com.syncdb.server.cluster.statemodel.PartitionStateModelFactory;
import com.syncdb.server.cluster.statemodel.ServerNodeStateModelFactory;
import com.syncdb.server.cluster.factory.NamespaceFactory;
import com.syncdb.server.verticle.ControllerVerticle;
import com.syncdb.server.verticle.ServerVerticle;
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
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.syncdb.core.constant.Constants.*;

// todo: fix flaky writers timing out!!!!
@Slf4j
public class SyncDbServer {
  static {
    RocksDB.loadLibrary();
  }

  private final Vertx vertx;
  private final HelixConfig config;
  private final ZKAdmin zkAdmin;
  private final Controller controller;
  private final Participant participant;
  private final String baseDir;
  private final LRUCache readerCache;
  private final TabletMailboxFactory mailboxFactory;
  private final CleanupProcessor cleanupProcessor;

  private final Thread shutdownHook = new Thread(() -> this.stop(30_000).subscribe());

  // todo: add metric factory
  public static void main(String[] args) throws Exception {
    TimeUtils.init();

    SyncDbServer syncDbServer = new SyncDbServer();
    syncDbServer.start().subscribe();
  }

  public SyncDbServer() throws Exception {

    String zkHost = System.getProperty("syncdb.zkHost", null);
    assert !Objects.equals(zkHost, null);

    String baseDirectory = System.getProperty("syncdb.baseDir", null);
    assert !Objects.equals(baseDirectory, null);
    baseDirectory = baseDirectory.replaceAll("/$", "");

    this.baseDir = baseDirectory;

    int cacheSize =
        Integer.parseInt(System.getProperty("syncdb.cacheSize", String.valueOf(64 * 1024 * 1024)));
    this.readerCache = new LRUCache(cacheSize);
    String nodeId;

    int availablePort;
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      availablePort = serverSocket.getLocalPort();
    }

    int serverPort = 9009;
    if (Boolean.parseBoolean(System.getProperty("syncdb.initRandomPort", "false"))) {
      serverPort = NetUtils.getRandomPort();
      System.setProperty("syncdb.serverPort", String.valueOf(serverPort));
    }

    if (Boolean.parseBoolean(System.getProperty("syncdb.localCluster", "false"))){
      nodeId = "localhost_" + serverPort;
    }
    else {
      // todo: verify this
      InetAddress localHost = InetAddress.getLocalHost();
      nodeId = localHost.getHostAddress() + "_" + serverPort;
    }
    this.config = new HelixConfig(zkHost, "syncdb__COMPUTE", nodeId);
    this.vertx = initVertx().blockingGet();
    this.mailboxFactory = TabletMailboxFactory.create();
    vertx
        .sharedData()
        .getLocalMap(FACTORY_MAP_NAME)
        .put(TabletMailboxFactory.FACTORY_NAME, mailboxFactory);

    this.zkAdmin = new ZKAdmin(vertx, config);
    this.controller = new Controller(vertx, config);
    controller.connect();
    NamespaceFactory.init(controller.getPropertyStore());
    if (controller.getManager().isLeader())
      ZKAdmin.addComputeClusterConfigs(controller.getManager());

    this.participant = startParticipant();
    this.cleanupProcessor = CleanupProcessor.create(vertx, controller, zkAdmin);
  }

  private Participant startParticipant() throws Exception {
    Participant participant = new Participant(vertx, config);
    PartitionStateModelFactory partitionStateModelFactory =
        new PartitionStateModelFactory(
            this.vertx, readerCache, mailboxFactory, config.getInstanceName(), baseDir, config);
    ServerNodeStateModelFactory serverNodeStateModelFactory =
        new ServerNodeStateModelFactory(vertx, config.getInstanceName(), zkAdmin, controller.getManager());

    participant.connect(partitionStateModelFactory, serverNodeStateModelFactory);
    return participant;
  }

  private Single<Vertx> initVertx() {
    System.setProperty(
        "vertx.logger-delegate-factory-class-name",
        "io.vertx.core.logging.SLF4JLogDelegateFactory");
    JsonObject zkConfig = new JsonObject();
    assert config != null;
    zkConfig.put("zookeeperHosts", config.getZhHost());
    zkConfig.put("rootPath", "io.vertx");
    zkConfig.put("retry", new JsonObject().put("initialSleepTime", 3000).put("maxTimes", 3));

    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);

    VertxOptions options =
        new VertxOptions()
            .setMetricsOptions(
                new MicrometerMetricsOptions()
                    .setPrometheusOptions(
                        new VertxPrometheusOptions()
                            .setEnabled(true)
                            .setStartEmbeddedServer(true)
                            .setEmbeddedServerOptions(
                                new HttpServerOptions().setHost("0.0.0.0").setPort(9090))
                            .setEmbeddedServerEndpoint("/metrics")))
            .setEventLoopPoolSize(CpuCoreSensor.availableProcessors())
            .setPreferNativeTransport(true);

    if (Boolean.parseBoolean(System.getProperty("syncdb.localCluster", "false")))
      options.getEventBusOptions().setHost("localhost").setPort(0);

    return Vertx.builder()
        .withClusterManager(mgr)
        .with(options)
        .rxBuildClustered()
        .map(
            vertx -> {
              RxJavaPlugins.setComputationSchedulerHandler(s -> RxHelper.scheduler(vertx));
              RxJavaPlugins.setIoSchedulerHandler(s -> RxHelper.scheduler(vertx));
              RxJavaPlugins.setNewThreadSchedulerHandler(s -> RxHelper.scheduler(vertx));
              Runtime.getRuntime().addShutdownHook(shutdownHook);
              return vertx;
            });
  }

  public Completable start() {
    cleanupProcessor.start();
    return deployControllerVerticle()
        .doOnComplete(() -> log.info("successfully started server"))
        .doOnError((e) -> log.error("application startup failed: ", e));
  }

  private Completable deployControllerVerticle() {
    return vertx
        .rxDeployVerticle(
            new ControllerVerticle(controller, zkAdmin),
            new DeploymentOptions().setInstances(1).setWorkerPoolName(HELIX_POOL_NAME))
        .ignoreElement();
  }

  public Completable stop(int timeout) {
    cleanupProcessor.close();
    readerCache.close();
    return Completable.complete()
        .delay(timeout, TimeUnit.MILLISECONDS)
        .andThen(vertx.rxClose())
        .doOnComplete(() -> log.info("successfully stopped server"))
        .doOnError(e -> log.error("error stopping server: ", e));
  }
}
