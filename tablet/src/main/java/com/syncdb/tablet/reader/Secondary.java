package com.syncdb.tablet.reader;

import com.syncdb.core.models.ColumnFamilyConfig;
import lombok.SneakyThrows;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

// todo: fix ttl butchering values!!!
public class Secondary {
  private Options options;
  private String path;
  private String secondaryPath;
  private final RocksDB rocksDB;
  private final LRUCache readerCache;
  private final ConcurrentHashMap<String, ColumnFamilyHandle> cfMap = new ConcurrentHashMap<>();

  @SneakyThrows
  public Secondary(Options options, LRUCache readerCache, String path, String secondaryPath, List<String> cfNames) {
    this.options = options;
    this.readerCache = readerCache;
    this.options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(readerCache));
    this.path = path;
    this.secondaryPath = secondaryPath;

    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<ColumnFamilyDescriptor> descriptors = cfNames.stream()
            .map(String::getBytes)
            .map(ColumnFamilyDescriptor::new)
            .collect(Collectors.toUnmodifiableList());
    this.rocksDB = TtlDB.openAsSecondary(new DBOptions(options), path, secondaryPath, descriptors, handles);
    for(String name : cfNames){
      cfMap.put(name, handles.get(cfNames.indexOf(name)));
    }
  }

  public void close() {
    this.rocksDB.close();
  }

  public byte[] read(byte[] key) throws RocksDBException {
    return rocksDB.get(key);
  }

  public byte[] read(byte[] key, String bucket) throws RocksDBException {
    ColumnFamilyHandle handle = cfMap.get(bucket);
    return rocksDB.get(handle, key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key) throws RocksDBException {
    return rocksDB.get(readOptions, key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key, String bucket) throws RocksDBException {
    ColumnFamilyHandle handle = cfMap.get(bucket);
    return rocksDB.get(handle, readOptions, key);
  }

  public List<byte[]> bulkRead(List<byte[]> keys) throws RocksDBException {
    return rocksDB.multiGetAsList(keys);
  }

  public List<byte[]> bulkRead(List<byte[]> keys, String bucket) throws RocksDBException {
    ColumnFamilyHandle handle = cfMap.get(bucket);
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
    for (int i=0;i<keys.size();i++)
      columnFamilyHandles.add(handle);
    return rocksDB.multiGetAsList(columnFamilyHandles, keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
    return rocksDB.multiGetAsList(readOptions, keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys, String bucket) throws RocksDBException {
    ColumnFamilyHandle handle = cfMap.get(bucket);
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(keys.size());
    for (int i=0;i<keys.size();i++)
      columnFamilyHandles.add(handle);
    return rocksDB.multiGetAsList(readOptions, columnFamilyHandles, keys);
  }

  @SneakyThrows
  public void catchUp() {
    this.rocksDB.tryCatchUpWithPrimary();
  }
}
