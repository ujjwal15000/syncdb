package com.syncdb.server.cluster.factory;

import lombok.extern.slf4j.Slf4j;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.helix.AccessOption.PERSISTENT;

@Slf4j
public class NamespaceFactory {
  private static final String BASE_PATH = "/NAMESPACE_METADATA";

  private static ZkHelixPropertyStore<ZNRecord> propertyStore;

  public static void init(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    NamespaceFactory.propertyStore = propertyStore;
  }

  public static void add(
      ZkHelixPropertyStore<ZNRecord> propertyStore, NamespaceMetadata namespace) {
    List<String> names = propertyStore.getChildNames(BASE_PATH, PERSISTENT);
    if (names != null && names.contains(namespace.getName()))
      throw new RuntimeException("namespace already exists");

    ZNRecord record = new ZNRecord(namespace.getName());
    record.setSimpleField("name", namespace.getName());
    record.setIntField("num-partitions", namespace.getNumPartitions());
    record.setIntField("num-replicas", namespace.getNumReplicas());
    record.setIntField("num-nodes", namespace.getNumNodes());
    record.setListField(
        "bucket-configs",
        namespace.getBucketConfigs().stream()
            .map(BucketConfig::serialize)
            .map(String::new)
            .collect(Collectors.toUnmodifiableList()));

    propertyStore.create(getNamespaceNodePath(namespace.getName()), record, PERSISTENT);
  }

  public static void addBucket(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String namespace, BucketConfig config) {
    List<String> names = propertyStore.getChildNames(BASE_PATH, PERSISTENT);
    if (names == null || !names.contains(namespace))
      throw new RuntimeException("namespace does not exist");

    propertyStore.update(
        getNamespaceNodePath(namespace),
        record -> {
          List<String> buckets =
              record.getListField("bucket-configs").stream()
                  .map(String::getBytes)
                  .map(BucketConfig::deserialize)
                  .map(BucketConfig::getName)
                  .collect(Collectors.toList());
          if (buckets.contains(config.getName()))
            throw new RuntimeException(
                String.format(
                    "bucket: %s already exists for namespace: %s", config.getName(), namespace));

          List<BucketConfig> bucketConfigs =
              record.getListField("bucket-configs").stream()
                  .map(String::getBytes)
                  .map(BucketConfig::deserialize)
                  .collect(Collectors.toList());
          bucketConfigs.add(config);

          record.setListField(
              "bucket-configs",
              bucketConfigs.stream()
                  .map(BucketConfig::serialize)
                  .map(String::new)
                  .collect(Collectors.toUnmodifiableList()));
          return record;
        },
        PERSISTENT);
  }

  public static void dropBucket(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String namespace, String name) {
    List<String> names = propertyStore.getChildNames(BASE_PATH, PERSISTENT);
    if (names == null || !names.contains(namespace))
      throw new RuntimeException("namespace does not exist");

    propertyStore.update(
        getNamespaceNodePath(namespace),
        record -> {
          List<String> buckets =
              record.getListField("bucket-configs").stream()
                  .map(String::getBytes)
                  .map(BucketConfig::deserialize)
                  .map(BucketConfig::getName)
                  .collect(Collectors.toList());
          if (!buckets.contains(name))
            throw new RuntimeException(
                String.format("bucket: %s does not exist for namespace: %s", name, namespace));

          List<BucketConfig> bucketConfigs =
              record.getListField("bucket-configs").stream()
                  .map(String::getBytes)
                  .map(BucketConfig::deserialize)
                  .filter(r -> !r.getName().equals(name))
                  .collect(Collectors.toList());

          record.setListField(
              "bucket-configs",
              bucketConfigs.stream()
                  .map(BucketConfig::serialize)
                  .map(String::new)
                  .collect(Collectors.toUnmodifiableList()));
          return record;
        },
        PERSISTENT);
  }

  private static String getNamespaceNodePath(String name) {
    return String.format("%s/%s", BASE_PATH, name);
  }

  public static NamespaceConfig get(String name) {
    NamespaceMetadata metadata = getMetadata(name);
    return NamespaceConfig.create(
        name,
        metadata.getNumPartitions(),
        metadata.getNumReplicas(),
        metadata.getBucketConfigs().stream()
            .collect(Collectors.toUnmodifiableList()));
  }

  public static NamespaceMetadata getMetadata(String name) {
    return getMetadata(propertyStore, name);
  }

  public static NamespaceMetadata getMetadata(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String name) {
    List<String> names = propertyStore.getChildNames(BASE_PATH, PERSISTENT);
    if (names == null || !names.contains(name))
      throw new RuntimeException("namespace does not exist");
    Stat stat = new Stat();
    ZNRecord record = propertyStore.get(getNamespaceNodePath(name), stat, 0);
    int numPartitions = record.getIntField("num-partitions", -1);
    int numNodes = record.getIntField("num-nodes", -1);
    int numReplicas = record.getIntField("num-replicas", -1);
    List<BucketConfig> bucketConfigs =
        record.getListField("bucket-configs").stream()
            .map(String::getBytes)
            .map(BucketConfig::deserialize)
            .collect(Collectors.toUnmodifiableList());

    assert numPartitions != -1;
    return NamespaceMetadata.create(name, numNodes, numPartitions, numReplicas, bucketConfigs);
  }
}
