package com.syncdb.server.cluster.factory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NamespaceStatus {
  private String name;
  private Integer numNodes;
  private Integer numPartitions;
  private Integer numReplicas;
  private List<BucketConfig> bucketConfigs;
  private Status status;
  private Map<String, Map<String, String>> hostMap;

  public static NamespaceStatus create(
      NamespaceMetadata metadata, Status status, Map<String, Map<String, String>> hostMap) {
    return new NamespaceStatus(
        metadata.getName(),
        metadata.getNumNodes(),
        metadata.getNumPartitions(),
        metadata.getNumReplicas(),
        metadata.getBucketConfigs(),
        status,
        hostMap);
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class StatusHostMapPair{
    private Map<String, Map<String, String>> hostMap;
    private Status status;

    public static StatusHostMapPair create(Map<String, Map<String, String>> hostMap, Status status){
      return new StatusHostMapPair(hostMap, status);
    }
  }

  public enum Status {
    INITIALIZING,
    NODE_ASSIGNMENT,
    PARTITION_ASSIGNMENT,
    PARTITION_ERROR,
    STABLE,
    FAILURE
  }
}
