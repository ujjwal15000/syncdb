package com.syncdb.core.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ReadMessage extends ProtocolMessage {

  public ReadMessage(int seq, List<byte[]> keys, String namespace) {
    super(MESSAGE_TYPE.READ, seq, serializePayload(keys, namespace));
  }

  private static byte[] serializePayload(List<byte[]> keys, String namespace) {
    int finalSize = 0;
    finalSize += 4;
    finalSize += namespace.getBytes().length;

    finalSize += 4;
    for (byte[] key: keys)
      finalSize += 4 + key.length;

    ByteBuffer buffer =
            ByteBuffer.allocate(finalSize);

    buffer.putInt(namespace.getBytes().length);
    buffer.put(namespace.getBytes());

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

    List<byte[]> res = new ArrayList<>(buffer.getInt());

    while (buffer.hasRemaining()){
      int len = buffer.getInt();
      byte[] key = new byte[len];
      buffer.get(key);
      res.add(key);
    }
    return new Message(res, new String(namespace));
  }

  @Data
  public static class Message{
    private String namespace;
    private List<byte[]> keys;

    public Message(List<byte[]> keys, String namespace){
      this.keys = keys;
      this.namespace = namespace;
    }
  }
}
