package com.syncdb.server.cluster.statemodel;

import com.syncdb.server.cluster.ZKAdmin;
import com.syncdb.server.cluster.factory.ConnectionFactory;
import com.syncdb.server.verticle.ServerVerticle;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.analysis.function.Sin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

import static com.syncdb.core.constant.Constants.*;

@Slf4j
public class ServerNodeStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;
  private final ZKAdmin zkAdmin;
  private final HelixManager manager;

  public ServerNodeStateModelFactory(
      Vertx vertx, String instanceName, ZKAdmin zkAdmin, HelixManager manager) {
    this.instanceName = instanceName;
    this.vertx = vertx;
    this.zkAdmin = zkAdmin;
    this.manager = manager;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    log.debug(
        "Creating new StateModel for resource: {} and partition: {}", resourceName, partitionName);
    return new OnlineOfflineStateModel(
        vertx, zkAdmin, manager, instanceName, resourceName, partitionName);
  }

  public static class OnlineOfflineStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final String resourceName;
    private final Vertx vertx;
    private final ZKAdmin zkAdmin;
    private final HelixManager manager;
    private ConnectionFactory connectionFactory;
    private String deploymentId;

    public OnlineOfflineStateModel(
        Vertx vertx,
        ZKAdmin zkAdmin,
        HelixManager manager,
        String instanceName,
        String resourceName,
        String partitionName) {
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;
      this.zkAdmin = zkAdmin;
      this.manager = manager;
      log.info(
          "Initialized OnlineOfflineStateModel for instance: {}, resource: {}, partition: {}",
          instanceName,
          resourceName,
          partitionName);
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context)
        throws Exception {
      log.info(
          "Transitioning from OFFLINE to ONLINE for resource: {}, partition: {}",
          resourceName,
          partitionName);
      zkAdmin.addInstanceToNamespaceCluster(this.resourceName.split("__")[0]);
      log.debug("Instance added to namespace cluster for resource: {}", resourceName);
      this.connectionFactory =
          ConnectionFactory.create(vertx, manager, this.resourceName.split("__")[0]);
      vertx
          .sharedData()
          .getLocalMap(CONNECTION_FACTORY_MAP_NAME)
          .put(CONNECTION_FACTORY_NAME, connectionFactory);
      this.deploySocketVerticles()
          .subscribe(
              r -> this.deploymentId = r,
              e ->
                  log.error(
                      String.format(
                          "error deploying socket verticles for instance %s", instanceName), e));
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      log.info(
          "Transitioning from ONLINE to OFFLINE for resource: {}, partition: {}",
          resourceName,
          partitionName);
      zkAdmin.removeInstanceFromNamespaceCluster(this.resourceName);
      log.debug("Instance removed from namespace cluster for resource: {}", resourceName);
      this.unDeploySocketVerticles().subscribe();
      this.connectionFactory.close();
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      log.warn(
          "Transitioning from OFFLINE to DROPPED for resource: {}, partition: {}",
          resourceName,
          partitionName);
      // Add cleanup logic here if necessary.
    }

    private Single<String> deploySocketVerticles() {
      return vertx.rxDeployVerticle(
          ServerVerticle::new,
          new DeploymentOptions()
              .setInstances(CpuCoreSensor.availableProcessors())
              .setWorkerPoolName(WORKER_POOL_NAME));
    }

    private Completable unDeploySocketVerticles() {
      return vertx.rxUndeploy(deploymentId);
    }
  }
}
