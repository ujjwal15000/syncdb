package com.syncdb.tablet.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PartitionConfig {
  // s3 configs
  private String s3Bucket;
  private String awsRegion;

  // syncdb configs
  private String namespace;
  private String partitionId;
  private String replicaId;

  public String getPath() {
    return namespace + "/" + partitionId;
  }

  public String getSecondaryPath() {
    return getPath() + "/" + replicaId;
  }
}
