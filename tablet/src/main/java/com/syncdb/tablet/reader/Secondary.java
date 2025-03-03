package com.syncdb.tablet.reader;

import io.vertx.core.impl.ConcurrentHashSet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.syncdb.tablet.Tablet.DEFAULT_CF;

// todo: fix ttl butchering values!!!
public class Secondary {
  private Options options;
  private String path;
  private String secondaryPath;
  private final LRUCache readerCache;
  private final ConcurrentHashMap<String, CfDbPair> cfMap = new ConcurrentHashMap<>();
  private final ConcurrentHashSet<RocksDB> dbSet = new ConcurrentHashSet<>();

  @SneakyThrows
  public Secondary(
      Options options,
      LRUCache readerCache,
      String path,
      String secondaryPath,
      List<String> cfNames) {
    this.options = options;
    this.readerCache = readerCache;
    this.options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(readerCache));
    this.path = path;
    this.secondaryPath = secondaryPath;

    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<ColumnFamilyDescriptor> descriptors =
        cfNames.stream()
            .map(String::getBytes)
            .map(ColumnFamilyDescriptor::new)
            .collect(Collectors.toUnmodifiableList());
    RocksDB rocksDB =
        TtlDB.openAsSecondary(new DBOptions(options), path, secondaryPath, descriptors, handles);
    dbSet.add(rocksDB);
    for (String name : cfNames) {
      cfMap.put(name, new CfDbPair(rocksDB, handles.get(cfNames.indexOf(name))));
    }
  }

  public void close() throws RocksDBException {
    this.cfMap.values().stream()
        .map(r -> r.columnFamilyHandle)
        .forEach(AbstractImmutableNativeReference::close);

    for (RocksDB rocksDB : dbSet) {
      rocksDB.closeE();
    }
  }

  public Boolean isCfPresent(String name) {
    return cfMap.containsKey(name);
  }

  public byte[] read(byte[] key) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.get(key);
  }

  public byte[] read(byte[] key, String bucket) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    return cfDbPair.rocksDB.get(handle, key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.get(readOptions, key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key, String bucket) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    return cfDbPair.rocksDB.get(handle, readOptions, key);
  }

  public List<byte[]> bulkRead(List<byte[]> keys) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.multiGetAsList(keys);
  }

  public List<byte[]> bulkRead(List<byte[]> keys, String bucket) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); i++) columnFamilyHandles.add(handle);
    return cfDbPair.rocksDB.multiGetAsList(columnFamilyHandles, keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.multiGetAsList(readOptions, keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys, String bucket)
      throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); i++) columnFamilyHandles.add(handle);
    return cfDbPair.rocksDB.multiGetAsList(readOptions, columnFamilyHandles, keys);
  }

  @SneakyThrows
  public void catchUp() {
    for (RocksDB rocksDB : dbSet) {
      rocksDB.tryCatchUpWithPrimary();
    }
  }

  @Data
  @AllArgsConstructor
  private static class CfDbPair {
    private RocksDB rocksDB;
    private ColumnFamilyHandle columnFamilyHandle;
  }
}
