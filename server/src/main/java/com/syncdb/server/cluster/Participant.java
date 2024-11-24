package com.syncdb.server.cluster;

import com.syncdb.server.cluster.config.HelixConfig;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class Participant {
  private final HelixConfig config;
  private final HelixManager manager;

  public Participant(HelixConfig config) {
    this.config = config;
    this.manager =
        HelixManagerFactory.getZKHelixManager(
            config.getClusterName(),
            config.getInstanceName(),
            InstanceType.PARTICIPANT,
            config.getZhHost());
  }

  public void connect(StateModelFactory<StateModel> stateModelFactory) throws Exception {
    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(MasterSlaveSMD.name, stateModelFactory);
    manager.connect();
    this.manager.connect();
  }
}
