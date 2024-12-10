package com.syncdb.server.cluster;

import com.syncdb.server.cluster.config.HelixConfig;
import com.syncdb.server.factory.NamespaceFactory;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Map;

public class ZKAdmin {
  private final HelixConfig config;
  private final ZKHelixAdmin zkHelixAdmin;
  private final Vertx vertx;

  public ZKAdmin(Vertx vertx, HelixConfig config) throws IOException {
    this.vertx = vertx;
    this.config = config;
    this.zkHelixAdmin = new ZKHelixAdmin(config.getZhHost());

    this.initCluster();
    this.addCurrentNode();
  }

  public void initCluster() {
    zkHelixAdmin.addCluster(config.getClusterName());
    zkHelixAdmin.addStateModelDef(
        config.getClusterName(), OnlineOfflineSMD.name, OnlineOfflineSMD.build());
    zkHelixAdmin.addStateModelDef(
        config.getClusterName(), MasterSlaveSMD.name, MasterSlaveSMD.build());
  }

  public void addNamespace(String name, int numNodes, int numPartitions, int numReplicas) {
    // add resource to global cluster to add one namespace per node
    // add capacity
    addNamespaceNodes(name, numNodes);

    //    // create namespace cluster
    //    zkHelixAdmin.addCluster(config.getClusterName() + "__namespace__" + name);
    //    zkHelixAdmin.addStateModelDef(
    //        config.getClusterName() + "__namespace__" + name,
    //        MasterSlaveSMD.name,
    //        MasterSlaveSMD.build());

    // add partitions
    addPartitions(name, numPartitions, numReplicas);
  }

  private void addNamespaceNodes(String name, int numNodes) {
    IdealState idealState = new IdealState(name + "__NODES");
    idealState.setNumPartitions(numNodes);
    idealState.setStateModelDefRef(OnlineOfflineSMD.name);
    IdealState.RebalanceMode mode =
        idealState.rebalanceModeFromString(
            IdealState.RebalanceMode.FULL_AUTO.name(), IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setRebalanceMode(mode);
    idealState.setRebalanceStrategy(
        "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
    idealState.setReplicas("1");
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());

    ResourceConfig.Builder builder = new ResourceConfig.Builder(name + "__NODES");
    builder.setPartitionCapacity(Map.of("NUM_NAMESPACE", 1));

    zkHelixAdmin.addResourceWithWeight(config.getClusterName(), idealState, builder.build());

    // rebalance nodes
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__NODES", 1);
  }

  private void addPartitions(String name, int numPartitions, int numReplicas) {
    IdealState idealState = new IdealState(name + "__PARTITIONS");
    idealState.setNumPartitions(numPartitions);
    idealState.setStateModelDefRef(MasterSlaveSMD.name);
    IdealState.RebalanceMode mode =
        idealState.rebalanceModeFromString(
            IdealState.RebalanceMode.FULL_AUTO.name(), IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setRebalanceMode(mode);
    // todo: check this
    //    idealState.setRebalanceStrategy(
    //            "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
    idealState.setReplicas(String.valueOf(numReplicas));
    idealState.setInstanceGroupTag("NAMESPACE__" + name);

    zkHelixAdmin.addResource(config.getClusterName(), name + "__PARTITIONS", idealState);
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__PARTITIONS", numReplicas);
  }

  public void addCurrentNode() throws IOException {
    InstanceConfig instanceConfig = new InstanceConfig(config.getInstanceName());
    instanceConfig.setInstanceCapacityMap(Map.of("NUM_NAMESPACE", 1));
    instanceConfig.setHostName(InetAddress.getLocalHost().getHostAddress());
    instanceConfig.setPort(String.valueOf(new ServerSocket(0).getLocalPort()));
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);

    zkHelixAdmin.addInstance(config.getClusterName(), instanceConfig);
  }

  public void addInstanceToNamespaceCluster(String name) {
    zkHelixAdmin.addInstanceTag(
        config.getClusterName(), config.getInstanceName(), "NAMESPACE__" + name);

    int replicas = NamespaceFactory.get(name).getNumReplicas();
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__PARTITIONS", replicas);
  }

  public void addInstanceFromNamespaceCluster(String name) {
    zkHelixAdmin.removeInstanceTag(
        config.getClusterName(), config.getInstanceName(), "NAMESPACE__" + name);

    int replicas =
            Integer.parseInt(
                    zkHelixAdmin
                            .getResourceIdealState(config.getClusterName(), name + "__PARTITIONS")
                            .getReplicas());
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__PARTITIONS", replicas);
  }
}
