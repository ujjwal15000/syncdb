package com.syncdb.server;

import com.syncdb.core.util.TimeUtils;
import com.syncdb.cluster.Controller;
import com.syncdb.cluster.Participant;
import com.syncdb.cluster.ZKAdmin;
import com.syncdb.cluster.config.HelixConfig;
import com.syncdb.cluster.statemodel.PartitionStateModelFactory;
import com.syncdb.cluster.statemodel.ServerNodeStateModelFactory;
import com.syncdb.cluster.factory.NamespaceFactory;
import com.syncdb.server.verticle.ControllerVerticle;
import com.syncdb.server.verticle.SocketVerticle;
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

  // todo: add metric factory
  public static void main(String[] args) throws Exception {
    TimeUtils.init();

    SyncDbServer syncDbServer = new SyncDbServer();
    syncDbServer.start();
  }

  // todo: add node type!!!
  public SyncDbServer() throws Exception {

    String zkHost = System.getProperty("zkHost", null);
    assert !Objects.equals(zkHost, null);

    String nodeId = UUID.randomUUID().toString();
    this.config = new HelixConfig(zkHost, "syncdb__COMPUTE", nodeId, HelixConfig.NODE_TYPE.COMPUTE);
    this.vertx = initVertx().blockingGet();

    this.zkAdmin = new ZKAdmin(vertx, config);
    this.controller = new Controller(vertx, config);
    controller.connect();
    NamespaceFactory.init(controller.getPropertyStore());
    if(controller.getManager().isLeader())
      ZKAdmin.addComputeClusterConfigs(controller.getManager());
    this.participant = startParticipant();
  }

  private Participant startParticipant() throws Exception {
    Participant participant = new Participant(vertx, config);
    PartitionStateModelFactory partitionStateModelFactory =
        new PartitionStateModelFactory(this.vertx, config.getInstanceName());
    ServerNodeStateModelFactory serverNodeStateModelFactory =
        new ServerNodeStateModelFactory(vertx, config.getInstanceName(), zkAdmin);

    participant.connect(partitionStateModelFactory, serverNodeStateModelFactory);
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
    Completable.mergeArray(deploySocketVerticle(), deployControllerVerticle())
        .subscribe(
            () -> log.info("successfully started server"),
            (e) -> log.error("application startup failed: ", e));
  }

  private Completable deploySocketVerticle() {
    return vertx
        .rxDeployVerticle(
            SocketVerticle::new,
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
