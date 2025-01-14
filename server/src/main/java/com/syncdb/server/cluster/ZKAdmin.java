package com.syncdb.server.cluster;

import com.syncdb.server.cluster.config.HelixConfig;
import com.syncdb.server.cluster.factory.NamespaceFactory;
import com.syncdb.server.cluster.factory.NamespaceMetadata;
import com.syncdb.server.cluster.factory.NamespaceStatus;
import io.vertx.rxjava3.core.Vertx;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
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

  // todo: add atleast left node validation
  public void addNamespace(String name, int numNodes, int numPartitions, int numReplicas) {
    addNamespaceNodes(name, numNodes);
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
    idealState.setReplicas("1");
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    ResourceConfig.Builder builder = new ResourceConfig.Builder(name + "__NODES");
    builder.setPartitionCapacity(Map.of("COMPUTE_UNITS", 1));

    zkHelixAdmin.addResourceWithWeight(config.getClusterName(), idealState, builder.build());
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
    idealState.setRebalanceStrategy(
        "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
    idealState.setReplicas(String.valueOf(numReplicas));
    idealState.setInstanceGroupTag("NAMESPACE__" + name);

    zkHelixAdmin.addResource(config.getClusterName(), name + "__PARTITIONS", idealState);
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__PARTITIONS", numReplicas);
  }

  public void addCurrentNode() throws IOException {
    InstanceConfig instanceConfig = new InstanceConfig(config.getInstanceName());
    instanceConfig.setInstanceCapacityMap(Map.of("COMPUTE_UNITS", 1));

    instanceConfig.setHostName(config.getInstanceName().split("_")[0]);
    instanceConfig.setPort(config.getInstanceName().split("_")[1]);
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);

    zkHelixAdmin.addInstance(config.getClusterName(), instanceConfig);
  }

  public void addInstanceToNamespaceCluster(String name) {
    zkHelixAdmin.addInstanceTag(
        config.getClusterName(), config.getInstanceName(), "NAMESPACE__" + name);

    int replicas = NamespaceFactory.get(name).getNumReplicas();
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__PARTITIONS", replicas);
  }

  public void removeInstanceFromNamespaceCluster(String name) {
    zkHelixAdmin.removeInstanceTag(
        config.getClusterName(), config.getInstanceName(), "NAMESPACE__" + name);

    int replicas =
        Integer.parseInt(
            zkHelixAdmin
                .getResourceIdealState(config.getClusterName(), name + "__PARTITIONS")
                .getReplicas());
    zkHelixAdmin.rebalance(config.getClusterName(), name + "__PARTITIONS", replicas);
  }

  public static void addComputeClusterConfigs(HelixManager manager) {
    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(List.of("COMPUTE_UNITS"));
    clusterConfig.setOfflineDurationForPurge(5 * 60 * 1_000);
    dataAccessor.setProperty(dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  public NamespaceStatus.StatusHostMapPair getNamespaceStatus(NamespaceMetadata metadata) {
    String name = metadata.getName();

    int assignedNodes =
        zkHelixAdmin
            .getInstancesInClusterWithTag(config.getClusterName(), "NAMESPACE__" + name)
            .size();

    int nodeStatus;
    if (assignedNodes < metadata.getNumNodes()) nodeStatus = 0;
    else nodeStatus = 1;

    IdealState idealState =
            zkHelixAdmin.getResourceIdealState(config.getClusterName(), name + "__PARTITIONS");
    ExternalView externalView =
            zkHelixAdmin.getResourceExternalView(config.getClusterName(), name + "__PARTITIONS");

    int partitionStatus = getStatus(idealState, externalView);

    if (partitionStatus == -1) {
      return NamespaceStatus.StatusHostMapPair.create(new HashMap<>(), NamespaceStatus.Status.FAILURE);
    }
    if (nodeStatus == 0) {
      return NamespaceStatus.StatusHostMapPair.create(new HashMap<>(), NamespaceStatus.Status.NODE_ASSIGNMENT);
    }
    if (partitionStatus == 0) {
      return NamespaceStatus.StatusHostMapPair.create(new HashMap<>(), NamespaceStatus.Status.PARTITION_ASSIGNMENT);
    }
    return NamespaceStatus.StatusHostMapPair.create(parseHostMap(externalView), NamespaceStatus.Status.STABLE);
  }

  private Map<String, Map<String, String>> parseHostMap(ExternalView externalView) {
    return externalView.getRecord().getMapFields();
  }

  private int getStatus(IdealState idealState, ExternalView externalView) {
    if (idealState != null && externalView != null) {
      return idealState
              .getRecord()
              .getMapFields()
              .keySet()
              .equals(externalView.getRecord().getMapFields().keySet())
          ? 1
          : 0;
    }
    return -1;
  }
}
