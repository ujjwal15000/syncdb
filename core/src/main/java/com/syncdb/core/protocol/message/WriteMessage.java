package com.syncdb.core.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.core.serde.serializer.ByteSerializer;
import com.syncdb.core.protocol.ProtocolMessage;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// todo: add empty and null checks
public class WriteMessage extends ProtocolMessage {
  private static final ByteDeserializer BYTE_DESERIALIZER = new ByteDeserializer();
  private static final ByteSerializer BYTE_SERIALIZER = new ByteSerializer();

  public WriteMessage(int seq, List<Record<byte[], byte[]>> records, String namespace, String bucket, Integer partition) {
    super(MESSAGE_TYPE.WRITE, seq, serializePayload(records, namespace, bucket, partition));
  }

  private static byte[] serializePayload(List<Record<byte[], byte[]>> records, String namespace, String bucket, Integer partition) {
    int finalSize = 0;
    finalSize += 4;
    finalSize += namespace.getBytes().length;

    finalSize += 4;
    finalSize += bucket.length();

    finalSize += 4;

    finalSize += 4;
    List<byte[]> serializedRecords = new ArrayList<>();

    for (Record<byte[], byte[]> record : records) {
      byte[] serializedRecord = Record.serialize(record, BYTE_SERIALIZER, BYTE_SERIALIZER);
      finalSize += 4;
      finalSize += serializedRecord.length;
      serializedRecords.add(serializedRecord);
    }

    ByteBuffer buffer = ByteBuffer.allocate(finalSize);

    buffer.putInt(namespace.getBytes().length);
    buffer.put(namespace.getBytes());

    buffer.putInt(bucket.length());
    buffer.put(bucket.getBytes());

    buffer.putInt(partition);

    buffer.putInt(serializedRecords.size());
    for (byte[] record : serializedRecords) {
      buffer.putInt(record.length);
      buffer.put(record);
    }

    buffer.flip();
    byte[] res = buffer.array();
    buffer.clear();
    return res;
  }

  public static Message deserializePayload(byte[] message) {
    ByteBuffer buffer = ByteBuffer.wrap(message);

    byte[] namespace = new byte[buffer.getInt()];
    buffer.get(namespace);

    byte[] bucket = new byte[buffer.getInt()];
    buffer.get(bucket);

    Integer partition = buffer.getInt();

    List<Record<byte[], byte[]>> records = new ArrayList<>(buffer.getInt());

    while (buffer.hasRemaining()) {
      int len = buffer.getInt();
      byte[] record = new byte[len];
      buffer.get(record);
      records.add(Record.deserialize(record, BYTE_DESERIALIZER, BYTE_DESERIALIZER));
    }
    return new Message(records, new String(namespace), new String(bucket), partition);
  }

  @Data
  public static class Message{
    private String namespace;
    private String bucket;
    private Integer partition;
    private List<Record<byte[], byte[]>> records;

    public Message(List<Record<byte[], byte[]>> records, String namespace, String bucket, Integer partition){
      this.records = records;
      this.namespace = namespace;
      this.bucket = bucket;
      this.partition = partition;
    }
  }
}
