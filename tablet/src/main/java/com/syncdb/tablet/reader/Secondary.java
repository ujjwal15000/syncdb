package com.syncdb.tablet.reader;

import io.vertx.core.impl.ConcurrentHashSet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import org.rocksdb.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.syncdb.tablet.Tablet.DEFAULT_CF;

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
    this.readerCache = readerCache;

    // todo: check if this should be global
    this.options = new Options(options);
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
      cfMap.put(
          name, new CfDbPair(new AtomicReference<>(rocksDB), handles.get(cfNames.indexOf(name))));
    }
  }

  public void close() throws RocksDBException {
    this.cfMap.values().stream()
        .map(r -> r.columnFamilyHandle)
        .forEach(AbstractImmutableNativeReference::close);

    for (RocksDB rocksDB : dbSet) {
      rocksDB.closeE();
    }
    cfMap.clear();
    dbSet.clear();
  }

  public Boolean isCfPresent(String name) {
    return cfMap.containsKey(name);
  }

  public void openCf(String name) throws RocksDBException {
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<ColumnFamilyDescriptor> descriptors = List.of(new ColumnFamilyDescriptor(name.getBytes()));
    RocksDB rocksDB =
        TtlDB.openAsSecondary(new DBOptions(options), path, secondaryPath, descriptors, handles);
    dbSet.add(rocksDB);
    cfMap.put(name, new CfDbPair(new AtomicReference<>(rocksDB), handles.get(0)));
  }

  public byte[] read(byte[] key) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.get().get(key);
  }

  public byte[] read(byte[] key, String bucket) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    return cfDbPair.rocksDB.get().get(handle, key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.get().get(readOptions, key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key, String bucket) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    return cfDbPair.rocksDB.get().get(handle, readOptions, key);
  }

  public List<byte[]> bulkRead(List<byte[]> keys) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.get().multiGetAsList(keys);
  }

  public List<byte[]> bulkRead(List<byte[]> keys, String bucket) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); i++) columnFamilyHandles.add(handle);
    return cfDbPair.rocksDB.get().multiGetAsList(columnFamilyHandles, keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(DEFAULT_CF);
    return cfDbPair.rocksDB.get().multiGetAsList(readOptions, keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys, String bucket)
      throws RocksDBException {
    CfDbPair cfDbPair = cfMap.get(bucket);
    ColumnFamilyHandle handle = cfDbPair.columnFamilyHandle;
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); i++) columnFamilyHandles.add(handle);
    return cfDbPair.rocksDB.get().multiGetAsList(readOptions, columnFamilyHandles, keys);
  }

  public void mergeCf() throws RocksDBException {
    if (dbSet.size() > 1) {
      List<String> cfNames = new ArrayList<>(cfMap.keySet());
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      List<ColumnFamilyDescriptor> descriptors =
          cfNames.stream()
              .map(String::getBytes)
              .map(ColumnFamilyDescriptor::new)
              .collect(Collectors.toUnmodifiableList());
      RocksDB rocksDB =
          TtlDB.openAsSecondary(new DBOptions(options), path, secondaryPath, descriptors, handles);
      dbSet.add(rocksDB);
      Set<RocksDB> oldDbs = new HashSet<>();
      for (String name : cfMap.keySet()) {
        oldDbs.add(cfMap.get(name).rocksDB.getAndSet(rocksDB));
        cfMap.put(
            name, new CfDbPair(new AtomicReference<>(rocksDB), handles.get(cfNames.indexOf(name))));
      }

      dbSet.removeAll(oldDbs);
      oldDbs.forEach(RocksDB::close);
    }
  }

  @SneakyThrows
  public void catchUp() {
    this.mergeCf();
    dbSet.parallelStream().forEach(RocksDB::tryCatchUpWithPrimary);
  }

  @Data
  @AllArgsConstructor
  private static class CfDbPair {
    private AtomicReference<RocksDB> rocksDB;
    private ColumnFamilyHandle columnFamilyHandle;
  }
}
