package com.syncdb.storage;

import com.syncdb.cluster.statemodel.StorageNodeStateModelFactory;
import com.syncdb.core.util.TimeUtils;
import com.syncdb.cluster.Controller;
import com.syncdb.cluster.Participant;
import com.syncdb.cluster.ZKAdmin;
import com.syncdb.cluster.config.HelixConfig;
import com.syncdb.cluster.statemodel.ServerNodeStateModelFactory;
import com.syncdb.cluster.factory.NamespaceFactory;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import lombok.extern.slf4j.Slf4j;
import org.dcache.oncrpc4j.portmap.OncRpcEmbeddedPortmap;

@Slf4j
public class SyncDbStorageServer {

  private final Vertx vertx;
  private final HelixConfig config;
  private final ZKAdmin zkAdmin;
  private final Controller controller;
  private final Participant participant;
  private NfsRpcServer nfsRpcServer;

  private final Thread shutdownHook = new Thread(() -> {
      try {
          this.stop(30_000);
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
  });

  // todo: add metric factory
  public static void main(String[] args) throws Exception {
    TimeUtils.init();

    SyncDbStorageServer syncDbStorageServer = new SyncDbStorageServer();
    syncDbStorageServer.start();
  }

  // todo: add node type!!!
  public SyncDbStorageServer() throws Exception {

    String zkHost = System.getProperty("zkHost", null);
    assert !Objects.equals(zkHost, null);

    String nodeId = UUID.randomUUID().toString();
    this.config = new HelixConfig(zkHost, "syncdb", nodeId, HelixConfig.NODE_TYPE.STORAGE);
    this.vertx = initVertx().blockingGet();

    this.zkAdmin = new ZKAdmin(vertx, config);
    this.controller = new Controller(vertx, config);
    controller.connect();
    NamespaceFactory.init(controller.getPropertyStore());

    this.participant = startParticipant();
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

  private Participant startParticipant() throws Exception {
    Participant participant = new Participant(vertx, config);

    StorageNodeStateModelFactory storageNodeStateModelFactory =
        new StorageNodeStateModelFactory(this.vertx, config.getInstanceName());
    ServerNodeStateModelFactory serverNodeStateModelFactory =
        new ServerNodeStateModelFactory(vertx, config.getInstanceName(), zkAdmin);

    participant.connect(storageNodeStateModelFactory, serverNodeStateModelFactory);
    return participant;
  }

  private void start() throws IOException {
    new OncRpcEmbeddedPortmap();

    nfsRpcServer = new NfsRpcServer(Path.of("/tmp/nfs"));
    nfsRpcServer.start();
  }

  private void stop(long timeout) throws IOException {
    nfsRpcServer.close();
  }
}
