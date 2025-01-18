package com.syncdb.tablet.ingestor;

import com.syncdb.core.models.Record;
import com.syncdb.tablet.models.PartitionConfig;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Ingestor {
  /*
      path for the main read/write instance
      most efficient if writes are key ordered and single threaded
  */
  private static final String DEFAULT_CF = new String(RocksDB.DEFAULT_COLUMN_FAMILY);
  private static final Long DEFAULT_POLLING_TIME = 5_000L;
  private static final WriteOptions DEFAULT_WRITE_OPTIONS = new WriteOptions();

  private final ConcurrentHashMap<String, ColumnFamilyHandle> cfMap = new ConcurrentHashMap<>();
  private final PartitionConfig partitionConfig;
  private final Options options;
  private final String path;
  @Getter private final TtlDB rocksDB;

  // todo figure this out
  // todo might need to remove this
  @Getter
  private final RateLimiter rateLimiter =
      new RateLimiter(100 * 1024 * 1024, 100_000, 10, RateLimiterMode.WRITES_ONLY, true);

  @SneakyThrows
  public Ingestor(
      PartitionConfig partitionConfig,
      Options options,
      String path,
      List<String> cfNames,
      List<Integer> cfTtls) {
    this.partitionConfig = partitionConfig;
    this.options = options;
    //    this.options.setRateLimiter(rateLimiter);

    this.path = path;
    takeRocksdbOwnership(path);

    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<ColumnFamilyDescriptor> descriptors = cfNames.stream()
            .map(String::getBytes)
            .map(ColumnFamilyDescriptor::new)
            .collect(Collectors.toUnmodifiableList());
    this.rocksDB = TtlDB.open(new DBOptions(options), path, descriptors, handles, cfTtls, false);
    for(String name : cfNames){
      cfMap.put(name, handles.get(cfNames.indexOf(name)));
    }
  }

  // might need to tweak options
  public void createColumnFamily(String name, int ttl) throws RocksDBException {
    cfMap.put(
        name,
        rocksDB.createColumnFamilyWithTtl(
            new ColumnFamilyDescriptor(name.getBytes(), new ColumnFamilyOptions()), ttl));
  }

  public void dropColumnFamily(String name) throws RocksDBException {
    if (cfMap.containsKey(name)){
      rocksDB.dropColumnFamily(cfMap.get(name));
      cfMap.remove(name).close();
    }
  }

  // todo: add cf validations
  public void write(List<Record<byte[], byte[]>> records) throws RocksDBException {
    WriteBatch batch = new WriteBatch();
    for (Record<byte[], byte[]> record : records)
      batch.put(cfMap.get(DEFAULT_CF), record.getKey(), record.getValue());
    rocksDB.write(DEFAULT_WRITE_OPTIONS, batch);
  }

  public void write(WriteOptions writeOptions, List<Record<byte[], byte[]>> records) throws RocksDBException {
    WriteBatch batch = new WriteBatch();
    for (Record<byte[], byte[]> record : records)
      batch.put(cfMap.get(DEFAULT_CF), record.getKey(), record.getValue());
    rocksDB.write(writeOptions, batch);
  }

  public void close() {
    this.rocksDB.close();
  }

  public static void takeRocksdbOwnership(String path) {
    Path filePath = Path.of(path + "/LOCK");
    try{
      Boolean deleted = Files.deleteIfExists(filePath);
    }
    catch (Exception e){
      throw new RuntimeException("lock file deletion failed ", e);
    }
  }
}
