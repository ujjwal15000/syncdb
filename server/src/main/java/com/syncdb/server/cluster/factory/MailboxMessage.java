package com.syncdb.server.cluster.factory;

import lombok.Data;

import java.nio.ByteBuffer;

@Data
public class MailboxMessage {
  private final byte[] payload;
  private final String errorMessage;

  MailboxMessage(byte[] payload, String errorMessage) {
    this.payload = payload;
    this.errorMessage = errorMessage;
  }

  public static MailboxMessage success(byte[] payload) {
    return new MailboxMessage(payload, "");
  }

  public static MailboxMessage failed(String errorMessage) {
    return new MailboxMessage(new byte[0], errorMessage);
  }

  public static byte[] serialize(MailboxMessage message) {
    int len = 4 + message.payload.length + 4 + message.errorMessage.getBytes().length;
    ByteBuffer buffer = ByteBuffer.allocate(len);
    buffer.putInt(message.payload.length);
    buffer.put(message.payload);
    buffer.putInt(message.errorMessage.getBytes().length);
    buffer.put(message.errorMessage.getBytes());
    buffer.flip();
    return buffer.array();
  }

  public static MailboxMessage deserialize(byte[] message) {
    ByteBuffer buffer = ByteBuffer.wrap(message);
    byte[] payload = new byte[buffer.getInt()];
    buffer.get(payload);
    byte[] errorMessage = new byte[buffer.getInt()];
    buffer.get(errorMessage);

    return new MailboxMessage(payload, new String(errorMessage));
  }
}
