package com.syncdb.server.cluster.factory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NamespaceMetadata {
  private static final List<BucketConfig> DEFAULT_BUCKETS =
      new ArrayList<>(List.of(new BucketConfig(new String(RocksDB.DEFAULT_COLUMN_FAMILY), 0)));

  private String name;
  private Integer numNodes;
  private Integer numPartitions;
  private Integer numReplicas;
  private List<BucketConfig> bucketConfigs = DEFAULT_BUCKETS;

  public static NamespaceMetadata create(
      String name, Integer numNodes, Integer numPartitions, Integer numReplicas) {
    return new NamespaceMetadata(name, numNodes, numPartitions, numReplicas, DEFAULT_BUCKETS);
  }

  public static NamespaceMetadata create(
      String name,
      Integer numNodes,
      Integer numPartitions,
      Integer numReplicas,
      List<BucketConfig> bucketConfigs) {
    return new NamespaceMetadata(name, numNodes, numPartitions, numReplicas, bucketConfigs);
  }
}
