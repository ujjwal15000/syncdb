package com.syncdb.cluster.statemodel;

import com.syncdb.cluster.ZKAdmin;
import com.syncdb.cluster.factory.TabletFactory;
import com.syncdb.cluster.factory.TabletMailbox;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import com.syncdb.tablet.models.PartitionConfig;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

public class StorageNodeStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;

  public StorageNodeStateModelFactory(Vertx vertx, String instanceName) {
    this.instanceName = instanceName;
    this.vertx = vertx;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    PartitionStateModelFactory.MasterSlaveStateModel stateModel = null;
    try {
      stateModel = new PartitionStateModelFactory.MasterSlaveStateModel(vertx, instanceName, resourceName, partitionName);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    return stateModel;
  }

  public static class MasterSlaveStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final String resourceName;
    private final Vertx vertx;

    public MasterSlaveStateModel(Vertx vertx, String instanceName, String resourceName, String partitionName) throws RocksDBException {
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;
    }

    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {

    }

    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {

    }

    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {

    }

    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {

    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      // todo: figure this out
    }
  }
}
