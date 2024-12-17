package com.syncdb.cluster;

import com.syncdb.cluster.config.HelixConfig;
import com.syncdb.cluster.statemodel.PartitionStateModelFactory;
import com.syncdb.cluster.statemodel.ServerNodeStateModelFactory;
import com.syncdb.cluster.statemodel.StorageNodeStateModelFactory;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.participant.StateMachineEngine;

public class Participant {
  private final Vertx vertx;
  private final HelixConfig config;
  private final HelixManager manager;

  public Participant(Vertx vertx, HelixConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.manager =
        HelixManagerFactory.getZKHelixManager(
            config.getClusterName(),
            config.getInstanceName(),
            InstanceType.PARTICIPANT,
            config.getZhHost());
  }

  public void connect(
      PartitionStateModelFactory partitionStateModelFactory,
      ServerNodeStateModelFactory serverNodeStateModelFactory)
      throws Exception {
    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(
        MasterSlaveSMD.name,
            partitionStateModelFactory);
    stateMach.registerStateModelFactory(
        OnlineOfflineSMD.name,
            serverNodeStateModelFactory);
    manager.connect();
    this.manager.connect();
  }

  public void connect(
          StorageNodeStateModelFactory storageNodeStateModelFactory,
          ServerNodeStateModelFactory serverNodeStateModelFactory)
          throws Exception {
    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(
            MasterSlaveSMD.name,
            storageNodeStateModelFactory);
    stateMach.registerStateModelFactory(
            OnlineOfflineSMD.name,
            serverNodeStateModelFactory);
    manager.connect();
    this.manager.connect();
  }
}
