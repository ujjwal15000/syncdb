package com.syncdb.tablet;

import com.syncdb.stream.models.SparkBlock;
import com.syncdb.tablet.ingestor.Ingestor;
import com.syncdb.tablet.models.PartitionConfig;
import com.syncdb.tablet.reader.Reader;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
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

  // todo: implement a metric service for both ingestor and reader

  private final PartitionConfig partitionConfig;
  private final String path;

  // used to store logs for different secondary instances
  private final String secondaryPath;
  @Getter private final Reader reader;
  private final Options options;

  @Getter private Ingestor ingestor;

  public Tablet(PartitionConfig partitionConfig, Options options) {
    this.partitionConfig = partitionConfig;
    this.path = partitionConfig.getPath();
    this.secondaryPath = partitionConfig.getSecondaryPath();
    this.options = options;
    this.reader = new Reader(options, path, secondaryPath);
  }

  public void openIngestor(SparkBlock.CommitMetadata commitMetadata, Consumer<SparkBlock.CommitMetadata> commitFunction) {
    if (ingestor != null) throw new RuntimeException("ingestor already opened!");
    this.ingestor = new Ingestor(partitionConfig, options, path, commitMetadata, commitFunction);
  }

  public void closeIngestor() {
    if (ingestor == null) throw new RuntimeException("ingestor is not opened yet!");
    this.ingestor.close();
    this.ingestor = null;
  }

  public void close() {
    if (ingestor != null) ingestor.close();
    reader.close();
  }
}
