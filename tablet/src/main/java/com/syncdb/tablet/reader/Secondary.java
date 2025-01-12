package com.syncdb.tablet.reader;

import lombok.SneakyThrows;
import org.rocksdb.*;

import java.util.List;

// todo: add block cache
public class Secondary {
  private Options options;
  private String path;
  private String secondaryPath;
  private final RocksDB rocksDB;
  private final LRUCache readerCache;

  @SneakyThrows
  public Secondary(Options options, LRUCache readerCache, String path, String secondaryPath) {
    this.options = options;
    this.readerCache = readerCache;
    this.options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(readerCache));
    this.path = path;
    this.secondaryPath = secondaryPath;
    this.rocksDB = RocksDB.openAsSecondary(options, path, secondaryPath);
  }

  public void close() {
    this.rocksDB.close();
  }

  public byte[] read(byte[] key) throws RocksDBException {
    return rocksDB.get(key);
  }

  public byte[] read(ReadOptions readOptions, byte[] key) throws RocksDBException {
    return rocksDB.get(readOptions, key);
  }

  public List<byte[]> bulkRead(List<byte[]> keys) throws RocksDBException {
    return rocksDB.multiGetAsList(keys);
  }

  public List<byte[]> bulkRead(ReadOptions readOptions, List<byte[]> keys) throws RocksDBException {
    return rocksDB.multiGetAsList(readOptions, keys);
  }

  @SneakyThrows
  public void catchUp() {
    this.rocksDB.tryCatchUpWithPrimary();
  }
}
