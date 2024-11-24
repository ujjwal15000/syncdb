package com.syncdb.core.protocol;

import com.syncdb.core.protocol.message.*;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class ProtocolMessage {

  //    +---------+---------+------------+------------+------------------+---------+
  //    | Header  | Seq     | Msg Type   | Payload Len| Payload          | CRC     |
  //    | 2 Bytes | 4 bytes | 1 Byte     | 4 Bytes    | Variable Length  | 2 Bytes |
  //    +---------+---------+------------+------------+------------------+---------+

  public static final byte[] VERSION_V1 = "v1".getBytes();
  public static final int FIXED_LENGTH = 2 + 4 + 1 + 4 + 2;

  private final byte[] header;
  @Getter private final int seq;
  @Getter private final MESSAGE_TYPE messageType;
  @Getter private final byte[] payload;

  public ProtocolMessage(MESSAGE_TYPE messageType, int seq, byte[] payload) {
    this.header = VERSION_V1;
    this.seq = seq;
    this.messageType = messageType;
    this.payload = payload;
  }

  public enum MESSAGE_TYPE {
    NOOP((byte) 0, NoopMessage.class),
    READ((byte) 1, ReadMessage.class),
    WRITE((byte) 2, WriteMessage.class),
    READ_ACK((byte) 3, ReadAckMessage.class),
    WRITE_ACK((byte) 4, WriteAckMessage.class),
    REFRESH_BUFFER((byte) 5, RefreshBufferMessage.class), // sends only latest seq number and new buffer size
    KILL_STREAM((byte) 6, KillStreamMessage.class),
    ERROR((byte) 7, ErrorMessage.class),
    END_STREAM((byte) 8, EndStreamMessage.class),
    METADATA((byte) 9, MetadataMessage.class);

    @Getter private final byte value;
    @Getter private final Class<? extends ProtocolMessage> clazz;

    MESSAGE_TYPE(byte value, Class<? extends ProtocolMessage> clazz) {
      this.value = value;
      this.clazz = clazz;
    }

    public static MESSAGE_TYPE fromValue(byte value) {
      for (MESSAGE_TYPE messageTYPE : MESSAGE_TYPE.values()) {
        if (messageTYPE.value == value) {
          return messageTYPE;
        }
      }
      throw new IllegalArgumentException("Invalid MESSAGE value: " + value);
    }
  }

  // without first 16 error detection bits
  private static short calculateCRC(byte[] data) {
    CRC32 crc = new CRC32();
    crc.update(data, 0, data.length);
    return (short) (crc.getValue() & 0xFFFF);
  }

  public static byte[] serialize(ProtocolMessage protocol) {
    int totalLength = FIXED_LENGTH + protocol.payload.length;
    ByteBuffer buffer = ByteBuffer.allocate(totalLength);

    buffer.put(protocol.header);
    buffer.putInt(protocol.seq);
    buffer.put(protocol.messageType.getValue());
    buffer.putInt(protocol.payload.length);
    buffer.put(protocol.payload);
    buffer.putShort(calculateCRC(protocol.payload));

    buffer.flip();
    byte[] res = buffer.array();
    buffer.clear();
    return res;
  }

  public static ProtocolMessage deserialize(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);

    byte[] version = new byte[2];
    buffer.get(version);
    String versionString = new String(version);
    switch (versionString) {
      case ("v1"):
        int seq = buffer.getInt();
        MESSAGE_TYPE messageType = MESSAGE_TYPE.fromValue(buffer.get());
        int dataLen = buffer.getInt();
        byte[] data = new byte[dataLen];
        buffer.get(data);
        short crc = buffer.getShort();
        // todo: add crc check
        return new ProtocolMessage(messageType, seq, data);
      default:
        throw new RuntimeException(
            String.format("not implemented for version: %s", new String(version)));
    }
  }
}
