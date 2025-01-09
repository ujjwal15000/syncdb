package com.syncdb.tablet.reader;

import java.util.List;
import lombok.SneakyThrows;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Reader {
  private final Options options;
  private final String path;
  private final RocksDB rocksDB;

  @SneakyThrows
  public Reader(Options options, String path) {
    this.options = options;
    this.path = path;
    this.rocksDB = RocksDB.openReadOnly(options, path);
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
    this.rocksDB.getSnapshot();
  }
}
