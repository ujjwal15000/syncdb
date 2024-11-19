package com.syncdb.client.writer;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;

import java.util.List;

public class ProtocolWriter {

  public static ProtocolMessage createMetadataMessage(ClientMetadata metadata) {
    return new MetadataMessage(metadata);
  }

  public static ProtocolMessage createReadMessage(int seq, byte[] key) {
    return new ReadMessage(seq, key);
  }

  public static ProtocolMessage createWriteMessage(int seq, Record<byte[], byte[]> key) {
    return new WriteMessage(seq, key);
  }

  public static ProtocolMessage createBulkReadMessage(int seq, List<byte[]> keys) {
    return new BulkReadMessage(seq, keys);
  }

  public static ProtocolMessage createBulkWriteMessage(int seq, List<Record<byte[], byte[]>> keys) {
    return new BulkWriteMessage(seq, keys);
  }

  public static ProtocolMessage createStreamingWriteMessage(
      int seq, List<Record<byte[], byte[]>> keys) {
    return new StreamingWriteMessage(seq, keys);
  }

  public static ProtocolMessage createEndStreamMessage() {
    return new EndStreamMessage();
  }
}
