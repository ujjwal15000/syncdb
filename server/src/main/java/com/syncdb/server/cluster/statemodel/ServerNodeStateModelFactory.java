package com.syncdb.server.cluster.statemodel;

import com.syncdb.server.cluster.ZKAdmin;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

@Slf4j
public class ServerNodeStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;
  private final ZKAdmin zkAdmin;

  public ServerNodeStateModelFactory(Vertx vertx, String instanceName, ZKAdmin zkAdmin) {
    this.instanceName = instanceName;
    this.vertx = vertx;
    this.zkAdmin = zkAdmin;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    log.debug("Creating new StateModel for resource: {} and partition: {}", resourceName, partitionName);
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel(vertx, zkAdmin, instanceName, resourceName, partitionName);
    return stateModel;
  }

  public static class OnlineOfflineStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final String resourceName;
    private final Vertx vertx;
    private final ZKAdmin zkAdmin;

    public OnlineOfflineStateModel(Vertx vertx, ZKAdmin zkAdmin, String instanceName, String resourceName, String partitionName){
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;
      this.zkAdmin = zkAdmin;
      log.info("Initialized OnlineOfflineStateModel for instance: {}, resource: {}, partition: {}", instanceName, resourceName, partitionName);
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      log.info("Transitioning from OFFLINE to ONLINE for resource: {}, partition: {}", resourceName, partitionName);
      zkAdmin.addInstanceToNamespaceCluster(this.resourceName.split("__")[0]);
      log.debug("Instance added to namespace cluster for resource: {}", resourceName);
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      log.info("Transitioning from ONLINE to OFFLINE for resource: {}, partition: {}", resourceName, partitionName);
      zkAdmin.removeInstanceFromNamespaceCluster(this.resourceName);
      log.debug("Instance removed from namespace cluster for resource: {}", resourceName);
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      log.warn("Transitioning from OFFLINE to DROPPED for resource: {}, partition: {}", resourceName, partitionName);
      // Add cleanup logic here if necessary.
    }
  }
}