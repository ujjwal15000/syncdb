package com.syncdb.core.serde.deserializer;

import com.syncdb.core.serde.Deserializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class StringDeserializer implements Deserializer<String>, Serializable {
  @Override
  public String deserialize(byte[] object) {
    return new String(object);
  }

  @Override
  public String deserialize(ByteBuffer buffer) {
    byte[] data = new byte[buffer.limit() - buffer.position()];
    buffer.get(buffer.position(), data);
    return new String(data);
  }
}
