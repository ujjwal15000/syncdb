package com.syncdb.server.cluster.statemodel;

import com.syncdb.server.cluster.ZKAdmin;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class OnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;
  private final ZKAdmin zkAdmin;

  public OnlineOfflineStateModelFactory(Vertx vertx, String instanceName, ZKAdmin zkAdmin) {
    this.instanceName = instanceName;
    this.vertx = vertx;
    this.zkAdmin = zkAdmin;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
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
    }

    // add tag to instance to add in namespace an isolation group
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      zkAdmin.addInstanceToNamespaceCluster(this.resourceName);
    }

    // remove tag from instance to remove from namespace isolation group
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      zkAdmin.addInstanceFromNamespaceCluster(this.resourceName);
    }

    // todo: check this; close something?!
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {

    }

  }
}
