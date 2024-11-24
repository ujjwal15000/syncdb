
package com.syncdb.tablet;

import lombok.Data;

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
  }