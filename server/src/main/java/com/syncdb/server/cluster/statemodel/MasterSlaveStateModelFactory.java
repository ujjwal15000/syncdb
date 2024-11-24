package com.syncdb.server.cluster.statemodel;

import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class MasterSlaveStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;

  public MasterSlaveStateModelFactory(Vertx vertx, String instanceName) {
    this.instanceName = instanceName;
    this.vertx = vertx;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LeaderStandbyStateModel stateModel = new LeaderStandbyStateModel(vertx, instanceName, resourceName, partitionName);
    return stateModel;
  }

  public static class LeaderStandbyStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final String resourceName;
    private final Vertx vertx;

    public LeaderStandbyStateModel(Vertx vertx, String instanceName, String resourceName, String partitionName){
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;
    }

    // open only the tablet reader and attach vertx reader address for this partition
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {

    }

    // open the tablet ingestor and attach vertx writer address for this partition
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {

    }

    // close the tablet ingestor and vertx writer consumer
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {

    }

    // close the tablet remove and vertx reader consumer
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {

    }

    // remove the tablet from tablet config map
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {

    }
  }
}
