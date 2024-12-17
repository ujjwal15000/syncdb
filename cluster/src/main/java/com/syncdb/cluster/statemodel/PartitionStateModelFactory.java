package com.syncdb.cluster.statemodel;

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

public class PartitionStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;

  public PartitionStateModelFactory(Vertx vertx, String instanceName) {
    this.instanceName = instanceName;
    this.vertx = vertx;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
      MasterSlaveStateModel stateModel = null;
      try {
          stateModel = new MasterSlaveStateModel(vertx, instanceName, resourceName, partitionName);
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
    private final TabletMailbox mailbox;

    public MasterSlaveStateModel(Vertx vertx, String instanceName, String resourceName, String partitionName) throws RocksDBException {
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;

      String namespace = resourceName.split("__")[0];
      int partitionId = Integer.parseInt(partitionName.split("_")[partitionName.split("_").length - 1]);
      String tmpPath = "/tmp/nfs/mnt";
//      String tmpPath = "target";
      PartitionConfig config =
              PartitionConfig.builder()
                      .bucket("test")
                      .region("us-east-1")
                      .namespace(namespace)
                      .partitionId(partitionId)
                      .rocksDbPath(tmpPath + "/" + "main" + "_" + partitionId)
                      .rocksDbSecondaryPath(tmpPath + "/" + "secondary" + "_" + partitionId)
                      .batchSize(100)
                      .sstReaderBatchSize(2)
                      .build();
      Options options = new Options().setCreateIfMissing(true);
      Tablet tablet = new Tablet(config, options);
      TabletFactory.add(tablet);

      this.mailbox = TabletMailbox.create(vertx, TabletConfig.create(namespace,  partitionId));
    }

    // open only the tablet reader and attach vertx reader address for this partition
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      mailbox.startReader();
    }

    // open the tablet ingestor and attach vertx writer address for this partition
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      mailbox.startWriter();
    }

    // close the tablet ingestor and vertx writer consumer
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      mailbox.closeWriter();
    }

    // close the tablet remove and vertx reader consumer
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      mailbox.closeReader();
    }

    // remove the tablet from tablet config map
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      // todo: figure this out
    }
  }
}
