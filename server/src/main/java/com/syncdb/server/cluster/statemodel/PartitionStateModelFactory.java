package com.syncdb.server.cluster.statemodel;

import com.syncdb.server.cluster.factory.*;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import com.syncdb.tablet.models.PartitionConfig;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.rocksdb.*;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class PartitionStateModelFactory extends StateModelFactory<StateModel> {
  private final String instanceName;
  private final Vertx vertx;
  private final String baseDir;
  private final LRUCache readerCache;
  private final TabletMailboxFactory mailboxFactory;

  public PartitionStateModelFactory(
      Vertx vertx,
      LRUCache readerCache,
      TabletMailboxFactory mailboxFactory,
      String instanceName,
      String baseDir) {
    this.vertx = vertx;
    this.readerCache = readerCache;
    this.mailboxFactory = mailboxFactory;
    this.instanceName = instanceName;
    this.baseDir = baseDir;
    log.info("PartitionStateModelFactory initialized for instance: {}", instanceName);
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    log.info(
        "Creating new state model for resource: {}, partition: {}", resourceName, partitionName);
    MasterSlaveStateModel stateModel;
    try {
      stateModel =
          new MasterSlaveStateModel(
              vertx,
              instanceName,
              resourceName,
              partitionName,
              baseDir,
              readerCache,
              mailboxFactory);
    } catch (RocksDBException e) {
      log.error(
          "Error creating state model for namespace: {}, partition: {}",
          resourceName,
          partitionName,
          e);
      throw new RuntimeException(e);
    }
    log.info("State model created for namespace: {}, partition: {}", resourceName, partitionName);
    return stateModel;
  }

  public static class MasterSlaveStateModel extends StateModel {
    private final String instanceName;
    private final String partitionName;
    private final Integer partitionId;
    private final String resourceName;
    private final String namespace;
    private final Vertx vertx;
    private final TabletMailbox mailbox;
    private final TabletMailboxFactory mailboxFactory;
    private final TabletConfig tabletConfig;
    private List<String> cfNames;
    private Long namespacePollerTimerId;

    public MasterSlaveStateModel(
        Vertx vertx,
        String instanceName,
        String resourceName,
        String partitionName,
        String baseDir,
        LRUCache readerCache,
        TabletMailboxFactory mailboxFactory)
        throws RocksDBException {
      super();
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.vertx = vertx;
      this.mailboxFactory = mailboxFactory;
      this.namespace = resourceName.split("__")[0];
      this.partitionId =
          Integer.parseInt(partitionName.split("_")[partitionName.split("_").length - 1]);

      log.info(
          "Initializing MasterSlaveStateModel for namespace: {}, partition: {}",
          resourceName,
          partitionId);
      PartitionConfig config =
          PartitionConfig.builder()
              .namespace(namespace)
              .partitionId(partitionId)
              .rocksDbPath(baseDir + "/" + "main" + "_" + partitionId)
              .rocksDbSecondaryPath(baseDir + "/" + "secondary" + "_" + partitionId)
              .build();

      Options options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
      NamespaceMetadata metadata = NamespaceFactory.getMetadata(namespace);
      this.cfNames =
          metadata.getBucketConfigs().stream()
              .map(BucketConfig::getName)
              .collect(Collectors.toUnmodifiableList());
      List<Integer> cfTtls =
          metadata.getBucketConfigs().stream()
              .map(BucketConfig::getTtl)
              .collect(Collectors.toUnmodifiableList());

      Tablet tablet = new Tablet(config, options, readerCache, cfNames, cfTtls);
      this.tabletConfig = TabletConfig.create(namespace, partitionId);
      this.mailbox = TabletMailbox.create(vertx, tablet, tabletConfig);
      mailboxFactory.addToFactory(tabletConfig, this.mailbox);

      log.info(
          "MasterSlaveStateModel initialized for namespace: {}, partition: {}",
          resourceName,
          partitionId);
    }

    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
        throws RocksDBException {
      log.info(
          "Transitioning from OFFLINE to SLAVE for namespace: {} and partition: {}",
          namespace,
          partitionId);
      mailbox.startReader();
    }

    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws RocksDBException {
      log.info(
          "Transitioning from SLAVE to MASTER for namespace: {} partition: {}",
          namespace,
          partitionId);
      mailbox.startWriter();
      this.namespacePollerTimerId = vertx.setPeriodic(10_000, id -> this.updateBuckets());
    }

    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      log.info(
          "Transitioning from MASTER to SLAVE for namespace: {} partition: {}",
          namespace,
          partitionId);
      if (this.namespacePollerTimerId != null) {
        vertx.cancelTimer(namespacePollerTimerId);
        log.info(
            "Cancelled namespace poller timer for namespace: {} partition: {}",
            namespace,
            partitionId);
      }
      mailbox.closeWriter();
    }

    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      log.info(
          "Transitioning from SLAVE to OFFLINE for namespace: {} partition: {}",
          namespace,
          partitionId);
      mailbox.closeReader();
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      log.info("Dropping namespace: {} partition: {}", namespace, partitionId);
      mailbox.close();
      mailboxFactory.removeFromFactory(this.tabletConfig);
    }

    private void updateBuckets() {
      log.info("Updating buckets for namespace: {}", namespace);
      NamespaceMetadata metadata = NamespaceFactory.getMetadata(this.namespace);
      List<String> newCfs =
          metadata.getBucketConfigs().stream()
              .map(BucketConfig::getName)
              .collect(Collectors.toUnmodifiableList());
      List<Integer> newTtls =
          metadata.getBucketConfigs().stream()
              .map(BucketConfig::getTtl)
              .collect(Collectors.toUnmodifiableList());

      for (String cf : this.cfNames) {
        if (!newCfs.contains(cf)) {
          try {
            this.mailbox.getTablet().dropColumnFamily(cf);
            log.info(
                "Dropped bucket: {} for namespace: {} for partition: {}",
                cf,
                namespace,
                partitionId);
          } catch (Exception e) {
            log.error(
                "Error removing bucket: {} config for namespace: {} and partitionId: {}",
                cf,
                this.namespace,
                this.partitionId,
                e);
          }
        }
      }

      for (String cf : newCfs) {
        if (!cfNames.contains(cf)) {
          Integer ttl = newTtls.get(newCfs.indexOf(cf));
          try {
            this.mailbox.getTablet().createColumnFamily(cf, ttl);
            log.info(
                "Created bucket: {} for namespace: {} with TTL: {} for partition: {}",
                cf,
                namespace,
                ttl,
                partitionId);
          } catch (Exception e) {
            log.error(
                "Error adding bucket: {} config for namespace: {} and partition: {}",
                cf,
                this.resourceName,
                this.partitionId,
                e);
          }
        }
      }
      this.cfNames = newCfs;
    }
  }
}
