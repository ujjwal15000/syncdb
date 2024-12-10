package com.syncdb.server.cluster;

import com.syncdb.server.cluster.config.HelixConfig;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

public class Controller {
  private final HelixConfig config;
  private final HelixManager manager;

  public Controller(Vertx vertx, HelixConfig config) {
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

  public ZkHelixPropertyStore<ZNRecord> getPropertyStore(){
      return manager.getHelixPropertyStore();
  }
}