package com.syncdb.tablet.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PartitionConfig {
  private String namespace;
  private Integer partitionId;
  private String rocksDbPath;
  private String rocksDbSecondaryPath;
}
