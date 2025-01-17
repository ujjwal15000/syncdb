package com.syncdb.core.protocol.writer;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import java.util.List;

public class ProtocolWriter {

  public static ProtocolMessage createReadMessage(int seq, byte[] key, String namespace) {
    return new ReadMessage(seq, List.of(key), namespace);
  }

  public static ProtocolMessage createWriteMessage(
      int seq, Record<byte[], byte[]> key, String namespace) {
    return new WriteMessage(seq, List.of(key), namespace);
  }

  public static ProtocolMessage createReadMessage(int seq, List<byte[]> keys, String namespace) {
    return new ReadMessage(seq, keys, namespace);
  }

  public static ProtocolMessage createWriteMessage(
      int seq, List<Record<byte[], byte[]>> keys, String namespace) {
    return new WriteMessage(seq, keys, namespace);
  }
}
