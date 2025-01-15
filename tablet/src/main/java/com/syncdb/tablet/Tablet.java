package com.syncdb.tablet;

import com.syncdb.tablet.ingestor.Ingestor;
import com.syncdb.tablet.models.PartitionConfig;
import com.syncdb.tablet.reader.Secondary;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.syncdb.tablet.ingestor.Ingestor.takeRocksdbOwnership;

@Slf4j
// todo: add tablet metrics
// todo: add block cache
public class Tablet {
  /*
      expects rocks db instances to be on a distributed file system
      can be extended in future to support one per instance but will need a replicator process
      or writes with consistency level 'all'
      container class that opens a rocks db primary and secondary instance
      the class holding this object is supposed to manage the opening of only one primary per partition
      ingestor can be openend later by the openIngestor() function
      multiple secondary can be opened to increase read performance

      1. contains an ingestor that writes a stream to rocksdb (primary instance)
      2. contains a reader that reads from the rocksdb (secondary instance)
      3. the reader only reads from a snapshot
      4. the class holding this object is supposed to call the updateSnapshot() function which in turn calls
         catchUpWithPrimary() function from rocksdb lib preferably after ingestor is done writing
      5. the class holding this object is responsible for the consensus on the snapshot state of the replicas

      load lib at process start
      static {
          RocksDB.loadLibrary();
      }
  */

  @Getter private final PartitionConfig partitionConfig;
  private final String path;

  // used to store logs for different secondary instances
  private final String secondaryPath;
  @Getter private Secondary secondary;
  private final Options options;
  private final LRUCache readerCache;
  @Getter private final TabletConfig tabletConfig;
  @Getter private Ingestor ingestor;

  private final List<String> cfNames;
  private final List<Integer> cfTtls;

  public Tablet(
      PartitionConfig partitionConfig,
      Options options,
      LRUCache readerCache,
      List<String> cfNames,
      List<Integer> cfTtls)
      throws RocksDBException {
    this.partitionConfig = partitionConfig;
    this.path = partitionConfig.getRocksDbPath();
    this.secondaryPath = partitionConfig.getRocksDbSecondaryPath();
    this.options = options;
    this.readerCache = readerCache;
    this.cfNames = cfNames;
    this.cfTtls = cfTtls;

    this.tabletConfig =
        TabletConfig.create(partitionConfig.getNamespace(), partitionConfig.getPartitionId());

    // todo: might need to repair here
    // ensures boot up of db
    takeRocksdbOwnership(path);
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<ColumnFamilyDescriptor> descriptors = cfNames.stream()
            .map(String::getBytes)
            .map(ColumnFamilyDescriptor::new)
            .collect(Collectors.toUnmodifiableList());
    TtlDB.open(new DBOptions(options), path, descriptors, handles, cfTtls, false).close();
  }

  public void openIngestor() {
    if (ingestor != null) throw new RuntimeException("ingestor already opened!");
    this.ingestor = new Ingestor(partitionConfig, options, path, cfNames, cfTtls);
  }

  public void openReader() {
    if (secondary != null) throw new RuntimeException("reader already opened!");
    this.secondary = new Secondary(options, readerCache, path, secondaryPath);
  }

  public void closeIngestor() {
    if (ingestor == null) throw new RuntimeException("ingestor is not opened yet!");
    this.ingestor.close();
    this.ingestor = null;
  }

  public void closeReader() {
    if (secondary == null) throw new RuntimeException("reader is not opened yet!");
    this.secondary.close();
  }

  public void createColumnFamily(String name, int ttl) throws RocksDBException {
    this.ingestor.createColumnFamily(name, ttl);
  }

  public void dropColumnFamily(String name) throws RocksDBException {
    this.ingestor.dropColumnFamily(name);
  }

  public void close() {
    if (ingestor != null) ingestor.close();
    secondary.close();
  }
}
