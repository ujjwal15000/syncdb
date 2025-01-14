package com.syncdb.server.cluster.factory;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import lombok.Data;

import java.util.*;

@Data
public class NamespaceConfig {
  private final String name;
  private final Integer numPartitions;
  private final Integer numReplicas;
  private final Murmur3Partitioner partitioner;
  private final Map<String, BucketConfig> buckets;

  NamespaceConfig(String name, Integer numPartitions, Integer numReplicas, List<BucketConfig> buckets) {
    this.name = name;
    this.numPartitions = numPartitions;
    this.numReplicas = numReplicas;
    this.partitioner = new Murmur3Partitioner(numPartitions);
    this.buckets = new HashMap<>();
    buckets.forEach(r -> this.buckets.put(r.getName(), r));
  }

  public static NamespaceConfig create(
      String name, Integer numPartitions, Integer numReplicas, List<BucketConfig> buckets) {
    return new NamespaceConfig(name, numPartitions, numReplicas, buckets);
  }
}
