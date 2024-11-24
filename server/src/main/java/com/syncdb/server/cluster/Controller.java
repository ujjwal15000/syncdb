package com.syncdb.server.cluster;

import com.syncdb.server.cluster.config.HelixConfig;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.GenericHelixController;

public class Controller {
  private final HelixConfig config;
  private final HelixManager manager;

  public Controller(HelixConfig config) {
    this.config = config;
    this.manager =
        HelixManagerFactory.getZKHelixManager(
            config.getClusterName(),
            config.getInstanceName(),
            InstanceType.CONTROLLER,
            config.getZhHost());
  }

  public void connect() throws Exception {
      this.manager.connect();
      GenericHelixController controller = new GenericHelixController();
      manager.addControllerListener(controller);
      manager.addInstanceConfigChangeListener(controller);
      manager.addResourceConfigChangeListener(controller);
      manager.addClusterfigChangeListener(controller);
      manager.addCustomizedStateConfigChangeListener(controller);
      manager.addLiveInstanceChangeListener(controller);
      manager.addIdealStateChangeListener(controller);
  }
}
