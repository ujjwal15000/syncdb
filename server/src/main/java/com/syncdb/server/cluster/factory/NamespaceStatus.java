package com.syncdb.server.cluster.factory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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

  public static NamespaceStatus create(NamespaceMetadata metadata, Status status) {
    return new NamespaceStatus(
        metadata.getName(),
        metadata.getNumNodes(),
        metadata.getNumPartitions(),
        metadata.getNumReplicas(),
        metadata.getBucketConfigs(),
        status);
  }

  public enum Status {
    INITIALIZING,
    NODE_ASSIGNMENT,
    PARTITION_ASSIGNMENT,
    STABLE,
    FAILURE
  }
}
