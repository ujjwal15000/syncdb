package com.syncdb.server.cluster.factory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BucketConfig {
  private String name;
  private Integer ttl;

  public static BucketConfig create(String name, Integer ttl){
    return new BucketConfig(name, ttl);
  }

  public static byte[] serialize(BucketConfig config) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + config.name.length() + 4);
    buffer.putInt(config.name.length());
    buffer.put(config.name.getBytes());
    buffer.putInt(config.ttl);
    buffer.flip();
    return buffer.array();
  }

  public static BucketConfig deserialize(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    int len = buffer.getInt();
    byte[] name = new byte[len];
    buffer.get(name);
    int ttl = buffer.getInt();
    return new BucketConfig(new String(name), ttl);
  }
}
