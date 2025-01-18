package com.syncdb.core.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import lombok.Data;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// todo: add empty and null checks
public class ReadMessage extends ProtocolMessage {

  public ReadMessage(int seq, List<byte[]> keys, String namespace, String bucket, Integer partition) {
    super(MESSAGE_TYPE.READ, seq, serializePayload(keys, namespace, bucket, partition));
  }

  private static byte[] serializePayload(List<byte[]> keys, String namespace, String bucket, Integer partition) {
    int finalSize = 0;
    finalSize += 4;
    finalSize += namespace.getBytes().length;

    finalSize += 4;
    finalSize += bucket.length();

    finalSize += 4;

    finalSize += 4;
    for (byte[] key: keys)
      finalSize += 4 + key.length;

    ByteBuffer buffer =
            ByteBuffer.allocate(finalSize);

    buffer.putInt(namespace.getBytes().length);
    buffer.put(namespace.getBytes());

    buffer.putInt(bucket.length());
    buffer.put(bucket.getBytes());

    buffer.putInt(partition);

    buffer.putInt(keys.size());
    for (byte[] key: keys){
      buffer.putInt(key.length);
      buffer.put(key);
    }

    buffer.flip();
    byte[] res = buffer.array();
    buffer.clear();
    return res;
  }

  public static Message deserializePayload(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    byte[] namespace = new byte[buffer.getInt()];
    buffer.get(namespace);

    byte[] bucket = new byte[buffer.getInt()];
    buffer.get(bucket);

    Integer partition = buffer.getInt();

    List<byte[]> res = new ArrayList<>(buffer.getInt());

    while (buffer.hasRemaining()){
      int len = buffer.getInt();
      byte[] key = new byte[len];
      buffer.get(key);
      res.add(key);
    }

    return new Message(res, new String(namespace), new String(bucket), partition);
  }

  @Data
  public static class Message{
    private String namespace;
    private String bucket;
    private Integer partition;
    private List<byte[]> keys;

    public Message(List<byte[]> keys, String namespace, String bucket, Integer partition){
      this.keys = keys;
      this.namespace = namespace;
      this.bucket = bucket;
      this.partition = partition;
    }
  }
}
