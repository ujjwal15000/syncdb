package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BulkReadMessage extends ProtocolMessage {
  List<byte[]> keys;

  public BulkReadMessage(int seq, List<byte[]> keys) {
    super(MESSAGE_TYPE.BULK_READ, seq, serializePayload(keys));
  }

  private static byte[] serializePayload(List<byte[]> keys) {
    ByteBuffer buffer =
        ByteBuffer.allocate(
            4 + keys.size() * 4 + keys.stream().map(r -> r.length).reduce(0, Integer::sum));
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

  public static List<byte[]> deserializePayload(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    List<byte[]> res = new ArrayList<>(buffer.getInt());

    while (buffer.hasRemaining()){
      int len = buffer.getInt();
      byte[] key = new byte[len];
      res.add(key);
    }
    return res;
  }
}
