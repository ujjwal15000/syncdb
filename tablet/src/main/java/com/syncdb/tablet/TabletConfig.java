
package com.syncdb.tablet;

import lombok.Data;

import java.nio.ByteBuffer;
import java.util.Objects;

@Data
public class TabletConfig {
    private final String namespace;
    private final Integer partitionId;

    TabletConfig(String namespace, Integer partitionId) {
      this.namespace = namespace;
      this.partitionId = partitionId;
    }

    public static TabletConfig create(String namespace, Integer partitionId) {
      return new TabletConfig(namespace, partitionId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      TabletConfig that = (TabletConfig) obj;
      return Objects.equals(partitionId, that.partitionId) && Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, partitionId);
    }

    public static byte[] serialize(TabletConfig config){
        ByteBuffer buffer = ByteBuffer.allocate(4 + config.getNamespace().length() + 4);
        buffer.putInt(config.getNamespace().length());
        buffer.put(config.getNamespace().getBytes());
        buffer.putInt(config.getPartitionId());
        buffer.flip();
        return buffer.array();
    }

    public static TabletConfig deserialize(byte[] data){
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int len = buffer.getInt();
        byte[] namespace = new byte[len];
        buffer.get(namespace);
        int partitionId = buffer.getInt();
        return new TabletConfig(new String(namespace), partitionId);
    }
  }