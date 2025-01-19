package com.syncdb.core.protocol.writer;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import org.rocksdb.RocksDB;

import java.util.List;

public class ProtocolWriter {

  public static ProtocolMessage createReadMessage(int seq, byte[] key, String namespace) {
    return new ReadMessage(
        seq, List.of(key), namespace, new String(RocksDB.DEFAULT_COLUMN_FAMILY), -1);
  }

  public static ProtocolMessage createReadMessage(
      int seq, byte[] key, String namespace, String bucket) {
    return new ReadMessage(seq, List.of(key), namespace, bucket, -1);
  }

  public static ProtocolMessage createWriteMessage(
      int seq, Record<byte[], byte[]> key, String namespace) {
    return new WriteMessage(
        seq, List.of(key), namespace, new String(RocksDB.DEFAULT_COLUMN_FAMILY), -1);
  }

  public static ProtocolMessage createWriteMessage(
      int seq, Record<byte[], byte[]> key, String namespace, String bucket) {
    return new WriteMessage(seq, List.of(key), namespace, bucket, -1);
  }

  public static ProtocolMessage createReadMessage(int seq, List<byte[]> keys, String namespace) {
    return new ReadMessage(seq, keys, namespace, new String(RocksDB.DEFAULT_COLUMN_FAMILY), -1);
  }

  public static ProtocolMessage createReadMessage(int seq, List<byte[]> keys, String namespace, Integer partition) {
    return new ReadMessage(seq, keys, namespace, new String(RocksDB.DEFAULT_COLUMN_FAMILY), partition);
  }

  public static ProtocolMessage createReadMessage(
      int seq, List<byte[]> keys, String namespace, String bucket) {
    return new ReadMessage(seq, keys, namespace, bucket, -1);
  }

  public static ProtocolMessage createReadMessage(
          int seq, List<byte[]> keys, String namespace, String bucket, Integer partition) {
    return new ReadMessage(seq, keys, namespace, bucket, partition);
  }

  public static ProtocolMessage createWriteMessage(
      int seq, List<Record<byte[], byte[]>> keys, String namespace) {
    return new WriteMessage(seq, keys, namespace, new String(RocksDB.DEFAULT_COLUMN_FAMILY), -1);
  }

  public static ProtocolMessage createWriteMessage(
          int seq, List<Record<byte[], byte[]>> keys, String namespace, Integer partition) {
    return new WriteMessage(seq, keys, namespace, new String(RocksDB.DEFAULT_COLUMN_FAMILY), partition);
  }

  public static ProtocolMessage createWriteMessage(
          int seq, List<Record<byte[], byte[]>> keys, String namespace, String bucket) {
    return new WriteMessage(seq, keys, namespace, bucket, -1);
  }

  public static ProtocolMessage createWriteMessage(
          int seq, List<Record<byte[], byte[]>> keys, String namespace, String bucket, Integer partition) {
    return new WriteMessage(seq, keys, namespace, bucket, partition);
  }
}
